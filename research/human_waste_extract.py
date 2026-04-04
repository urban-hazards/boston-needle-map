"""
Extract human waste ticket examples from Boston 311.

Two buckets:
1. CORRECTLY ROUTED: Tickets that reached INFO_HumanWaste queue
2. MISROUTED: Tickets with human waste keywords that went to PWDx/trash/street cleaning queues

Output: JSON files for later use with a small classifier model.
"""

import json
import urllib.parse
import urllib.request

CKAN_BASE = "https://data.boston.gov/api/3/action"
UA = "Boston311Research/1.0 (public-health-research)"

RESOURCE_IDS = {
    2024: "dff4d804-5031-443a-8409-8344efd0e5c8",
    2025: "9d7c2214-4709-478a-a2e8-fb2020a5bb94",
    2026: "1a0b420d-99f1-4887-9851-990b2a5a6e17",
}

FIELDS = """
    "case_enquiry_id", "type", "case_title", "reason", "queue",
    "department", "case_status", "closure_reason", "subject",
    "location_street_name", "location_zipcode", "neighborhood",
    "open_dt", "closed_dt", "source", "on_time",
    "latitude", "longitude"
""".strip()


def api_get(url: str) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  API error: {e}")
        return None


def sql_query(resource_id: str, sql: str) -> list[dict]:
    url = f"{CKAN_BASE}/datastore_search_sql?sql={urllib.parse.quote(sql)}"
    data = api_get(url)
    if data and data.get("success"):
        return data["result"]["records"]
    return []


def get_correctly_routed(resource_id: str, year: int) -> list[dict]:
    """All tickets routed to INFO_HumanWaste queue."""
    sql = f'''SELECT {FIELDS}
              FROM "{resource_id}"
              WHERE "queue" = 'INFO_HumanWaste'
              '''
    records = sql_query(resource_id, sql)
    for r in records:
        r["_year"] = year
        r["_label"] = "correctly_routed"
    print(f"  {year} INFO_HumanWaste: {len(records)} records")
    return records


def get_needle_team_human_waste(resource_id: str, year: int) -> list[dict]:
    """Human waste tickets that ended up at GEN_Needle_Pickup (same biohazard team)."""
    sql = f'''SELECT {FIELDS}
              FROM "{resource_id}"
              WHERE "queue" = 'GEN_Needle_Pickup'
              AND (LOWER("closure_reason") LIKE '%feces%'
                   OR LOWER("closure_reason") LIKE '%fecal%'
                   OR LOWER("closure_reason") LIKE '%human waste%'
                   OR LOWER("closure_reason") LIKE '%excrement%'
                   OR LOWER("closure_reason") LIKE '%poop%'
                   OR LOWER("closure_reason") LIKE '%urine%'
                   OR LOWER("closure_reason") LIKE '%biohazard%')
              '''
    records = sql_query(resource_id, sql)
    for r in records:
        r["_year"] = year
        r["_label"] = "correctly_routed_needle_team"
    print(f"  {year} GEN_Needle_Pickup (human waste mentions): {len(records)} records")
    return records


def get_misrouted_keyword_search(resource_id: str, year: int) -> list[dict]:
    """Tickets with human waste keywords NOT routed to INFO_HumanWaste or GEN_Needle_Pickup."""

    # Terms that specifically indicate human waste (not dog waste)
    waste_sql_terms = [
        "human waste", "human feces", "human fecal",
        "someone defecated", "person defecated", "people defecating",
        "man defecating", "woman defecating",
        "human excrement", "human poop",
        "human urine", "someone urinated", "person urinating",
        "bodily fluid", "bodily waste",
        "homeless.*feces", "homeless.*poop",
        "encampment.*feces", "encampment.*waste",
    ]

    # Broader terms that could be human or animal
    broad_terms = [
        "feces on sidewalk", "feces on street", "feces in front",
        "feces on the sidewalk", "feces on the street",
        "poop on sidewalk", "poop on street", "poop on the sidewalk",
        "feces all over", "smeared feces",
        "defecating on", "defecating in",
        "urinating on", "urinating in",
        "someone pooped", "someone pooping",
    ]

    all_records = []

    # Search closure_reason for specific human waste terms
    for term in waste_sql_terms + broad_terms:
        # Use LIKE for simple terms, skip regex patterns
        if ".*" in term:
            continue
        sql = f'''SELECT {FIELDS}
                  FROM "{resource_id}"
                  WHERE "queue" != 'INFO_HumanWaste'
                  AND "queue" != 'GEN_Needle_Pickup'
                  AND (LOWER("closure_reason") LIKE '%{term}%')
                  '''
        records = sql_query(resource_id, sql)
        for r in records:
            r["_year"] = year
            r["_label"] = "potential_misroute"
            r["_matched_term"] = term
        if records:
            print(f"  {year} closure '{term}': {len(records)} records")
        all_records.extend(records)

    # Also search case descriptions that may hint at human waste
    # The "type" field often doesn't reflect the actual content
    desc_terms = [
        "human waste", "human feces", "feces on sidewalk",
        "feces on the sidewalk", "feces on street",
        "poop on sidewalk", "poop on the sidewalk",
        "someone defecated", "defecating",
        "bodily fluid", "bodily waste",
        "biohazard", "bio hazard",
    ]

    for term in desc_terms:
        sql = f'''SELECT {FIELDS}
                  FROM "{resource_id}"
                  WHERE "queue" != 'INFO_HumanWaste'
                  AND "queue" != 'GEN_Needle_Pickup'
                  AND LOWER("closure_reason") LIKE '%{term}%'
                  '''
        records = sql_query(resource_id, sql)
        for r in records:
            r["_year"] = year
            r["_label"] = "potential_misroute"
            r["_matched_term"] = term
        if records:
            # Don't double-print if already covered
            pass
        all_records.extend(records)

    # Deduplicate by case_enquiry_id
    seen = set()
    unique = []
    for r in all_records:
        cid = r.get("case_enquiry_id")
        if cid not in seen:
            seen.add(cid)
            unique.append(r)

    print(f"  {year} potential misroutes (deduplicated): {len(unique)} records")
    return unique


def get_wrong_queue_feces(resource_id: str, year: int) -> list[dict]:
    """Broader search: any ticket mentioning feces/poop/waste in closure_reason
    that went to PWDx district queues, trash, or street cleaning — NOT biohazard."""

    non_bio_queues = [
        "PWDx_District%",
        "PWDx_Code Enforcement",
        "PWDx_Missed Trash%",
        "INFO01_%",
        "INFO_Reallocation%",
        "INFO_Homeless%",
        "INFO_Encampments",
        "INFO_Unsheltered%",
    ]

    queue_clause = " OR ".join(f'"queue" LIKE \'{q}\'' for q in non_bio_queues)

    sql = f'''SELECT {FIELDS}
              FROM "{resource_id}"
              WHERE ({queue_clause})
              AND (LOWER("closure_reason") LIKE '%feces%'
                   OR LOWER("closure_reason") LIKE '%fecal%'
                   OR LOWER("closure_reason") LIKE '%human waste%'
                   OR LOWER("closure_reason") LIKE '%poop%'
                   OR LOWER("closure_reason") LIKE '%defecate%'
                   OR LOWER("closure_reason") LIKE '%defecating%'
                   OR LOWER("closure_reason") LIKE '%urine%'
                   OR LOWER("closure_reason") LIKE '%excrement%'
                   OR LOWER("closure_reason") LIKE '%biohazard%'
                   OR LOWER("closure_reason") LIKE '%bodily%')
              '''
    records = sql_query(resource_id, sql)
    for r in records:
        r["_year"] = year
        r["_label"] = "misrouted_feces_in_closure"
    print(f"  {year} feces/poop in closure at non-bio queues: {len(records)} records")
    return records


def get_closure_says_not_our_job(resource_id: str, year: int) -> list[dict]:
    """Find tickets where closure says 'does not' or 'we don't' handle this — classic misroute signal."""
    sql = f'''SELECT {FIELDS}
              FROM "{resource_id}"
              WHERE (LOWER("closure_reason") LIKE '%does not servi%'
                     OR LOWER("closure_reason") LIKE '%we don''t%'
                     OR LOWER("closure_reason") LIKE '%not our%'
                     OR LOWER("closure_reason") LIKE '%bpw does not%'
                     OR LOWER("closure_reason") LIKE '%sorry. we don%')
              AND (LOWER("closure_reason") LIKE '%feces%'
                   OR LOWER("closure_reason") LIKE '%poop%'
                   OR LOWER("closure_reason") LIKE '%waste%'
                   OR LOWER("closure_reason") LIKE '%human%'
                   OR LOWER("closure_reason") LIKE '%biohazard%')
              '''
    records = sql_query(resource_id, sql)
    for r in records:
        r["_year"] = year
        r["_label"] = "rejected_not_our_job"
    print(f"  {year} 'not our job' + waste keywords: {len(records)} records")
    return records


def main():
    all_correct = []
    all_needle_hw = []
    all_misrouted = []
    all_wrong_queue = []
    all_rejected = []

    for year, rid in RESOURCE_IDS.items():
        print(f"\n=== {year} ===")
        all_correct.extend(get_correctly_routed(rid, year))
        all_needle_hw.extend(get_needle_team_human_waste(rid, year))
        all_misrouted.extend(get_misrouted_keyword_search(rid, year))
        all_wrong_queue.extend(get_wrong_queue_feces(rid, year))
        all_rejected.extend(get_closure_says_not_our_job(rid, year))

    # Combine all misrouted into one deduped set
    misrouted_combined = {}
    for r in all_misrouted + all_wrong_queue + all_rejected:
        cid = r.get("case_enquiry_id")
        if cid not in misrouted_combined:
            misrouted_combined[cid] = r
        else:
            # Keep the most specific label
            if r["_label"] == "rejected_not_our_job":
                misrouted_combined[cid] = r

    correctly_routed = all_correct + all_needle_hw

    print(f"\n\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Correctly routed (INFO_HumanWaste): {len(all_correct)}")
    print(f"Correctly routed (Needle team + hw mention): {len(all_needle_hw)}")
    print(f"Total correctly routed: {len(correctly_routed)}")
    print(f"Potential misroutes (combined, deduped): {len(misrouted_combined)}")

    # Show queue distribution for misrouted
    from collections import Counter
    q_counts = Counter(r.get("queue", "?") for r in misrouted_combined.values())
    print(f"\nMisrouted queue distribution:")
    for q, c in q_counts.most_common(20):
        print(f"  {q}: {c}")

    # Show type distribution for misrouted
    t_counts = Counter(r.get("type", "?") for r in misrouted_combined.values())
    print(f"\nMisrouted type distribution:")
    for t, c in t_counts.most_common(20):
        print(f"  {t}: {c}")

    # Show label distribution
    l_counts = Counter(r.get("_label", "?") for r in misrouted_combined.values())
    print(f"\nMisrouted label distribution:")
    for l, c in l_counts.most_common():
        print(f"  {l}: {c}")

    # Print some example closure reasons from misrouted
    print(f"\n--- Sample misrouted closure reasons ---")
    for r in list(misrouted_combined.values())[:20]:
        closure = r.get("closure_reason", "")[:150]
        print(f"  [{r.get('_label')}] type='{r.get('type')}' queue='{r.get('queue')}'")
        print(f"    closure: {closure}")
        print()

    # Print some example closure reasons from correctly routed
    print(f"\n--- Sample correctly routed closure reasons ---")
    for r in correctly_routed[:20]:
        closure = r.get("closure_reason", "")[:150]
        print(f"  [{r.get('_label')}] type='{r.get('type')}' queue='{r.get('queue')}'")
        print(f"    closure: {closure}")
        print()

    # Save to JSON
    output = {
        "correctly_routed": correctly_routed,
        "misrouted": list(misrouted_combined.values()),
        "metadata": {
            "years": list(RESOURCE_IDS.keys()),
            "correctly_routed_count": len(correctly_routed),
            "misrouted_count": len(misrouted_combined),
            "extraction_date": "2026-04-04",
            "notes": "INFO_HumanWaste is the correct queue for human waste. "
                     "GEN_Needle_Pickup (same biohazard team) also handles some. "
                     "Misrouted = went to PWDx trash/street/district queues instead.",
        }
    }

    outpath = "research/human_waste_examples.json"
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved to {outpath}")
    print(f"Total records: {len(correctly_routed) + len(misrouted_combined)}")


if __name__ == "__main__":
    main()
