"""
Deep dive: South End + Roxbury human waste tickets.

Goals:
1. What fields actually contain the caller's original description?
2. Search ALL text fields, not just closure_reason
3. Check source (app vs call center) — call center tickets may have operator notes
4. Look at ALL tickets in these neighborhoods for street cleaning/trash types
5. Find patterns in tickets coded wrong and just closed
"""

import json
import urllib.parse
import urllib.request
from collections import Counter

CKAN_BASE = "https://data.boston.gov/api/3/action"
UA = "Boston311Research/1.0 (public-health-research)"

RESOURCE_IDS = {
    2024: "dff4d804-5031-443a-8409-8344efd0e5c8",
    2025: "9d7c2214-4709-478a-a2e8-fb2020a5bb94",
}

ALL_FIELDS = '*'

# South End is in District 1C or neighborhood "South End"
# Roxbury is District 10A/10B or neighborhood "Roxbury"
HOODS = ["South End", "Roxbury"]


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


def step1_check_all_fields(resource_id: str, year: int):
    """Pull a few raw records to see every field and what contains caller text."""
    print(f"\n{'='*60}")
    print(f"STEP 1: Raw field inspection ({year}) - South End")
    print(f"{'='*60}")

    # Get a few INFO_HumanWaste tickets with all fields
    sql = f'''SELECT * FROM "{resource_id}"
              WHERE "queue" = 'INFO_HumanWaste'
              LIMIT 5'''
    records = sql_query(resource_id, sql)

    if records:
        print(f"\nAll fields in a record: {sorted(records[0].keys())}")
        for i, r in enumerate(records):
            print(f"\n  --- Record {i+1} ---")
            for k, v in sorted(r.items()):
                if v and str(v).strip() and k != '_full_text':
                    print(f"    {k}: {str(v)[:200]}")
            # Show _full_text separately (it's big)
            ft = r.get('_full_text', '')
            if ft:
                print(f"    _full_text (first 500): {str(ft)[:500]}")

    # Also grab a call-center ticket
    sql = f'''SELECT * FROM "{resource_id}"
              WHERE "source" = 'Constituent Call'
              AND "neighborhood" = 'South End'
              AND "type" = 'Requests for Street Cleaning'
              LIMIT 3'''
    records = sql_query(resource_id, sql)
    if records:
        print(f"\n\n  --- Call center ticket examples ---")
        for i, r in enumerate(records):
            print(f"\n  --- Call center record {i+1} ---")
            for k, v in sorted(r.items()):
                if v and str(v).strip() and k != '_full_text':
                    print(f"    {k}: {str(v)[:200]}")
            ft = r.get('_full_text', '')
            if ft:
                print(f"    _full_text (first 500): {str(ft)[:500]}")


def step2_source_distribution(resource_id: str, year: int):
    """How do tickets come in? App vs call center vs web."""
    print(f"\n{'='*60}")
    print(f"STEP 2: Source distribution ({year}) - South End + Roxbury")
    print(f"{'='*60}")

    for hood in HOODS:
        sql = f'''SELECT "source", COUNT(*) as cnt
                  FROM "{resource_id}"
                  WHERE "neighborhood" = '{hood}'
                  GROUP BY "source"
                  ORDER BY cnt DESC'''
        records = sql_query(resource_id, sql)
        print(f"\n  {hood} - all tickets by source:")
        for r in records:
            print(f"    {r.get('source', '?')}: {r.get('cnt')}")

        # Now just street cleaning
        sql = f'''SELECT "source", COUNT(*) as cnt
                  FROM "{resource_id}"
                  WHERE "neighborhood" = '{hood}'
                  AND "type" = 'Requests for Street Cleaning'
                  GROUP BY "source"
                  ORDER BY cnt DESC'''
        records = sql_query(resource_id, sql)
        print(f"\n  {hood} - Street Cleaning by source:")
        for r in records:
            print(f"    {r.get('source', '?')}: {r.get('cnt')}")

        # INFO_HumanWaste by source
        sql = f'''SELECT "source", COUNT(*) as cnt
                  FROM "{resource_id}"
                  WHERE "queue" = 'INFO_HumanWaste'
                  GROUP BY "source"
                  ORDER BY cnt DESC'''
        records = sql_query(resource_id, sql)
        if hood == HOODS[0]:  # only print once, queue is citywide
            print(f"\n  INFO_HumanWaste queue by source (citywide):")
            for r in records:
                print(f"    {r.get('source', '?')}: {r.get('cnt')}")


def step3_fulltext_search(resource_id: str, year: int):
    """Search _full_text for human waste terms in South End + Roxbury."""
    print(f"\n{'='*60}")
    print(f"STEP 3: Full-text search across ALL fields ({year})")
    print(f"{'='*60}")

    # The _full_text field is a tsvector - we can search it with LIKE on the raw text
    # or try to find if there's a description/notes field we're missing

    waste_terms = [
        "human waste", "feces", "fecal", "poop", "defecating",
        "excrement", "urine", "urinating", "biohazard",
        "bodily fluid", "someone pooped", "someone pooping",
        "shit", "shitting",  # people actually write this in complaints
        "doo doo", "doodoo", "doody",
        "number two", "#2",
        "soiled", "smeared",
        "homeless.*bathroom", "using.*bathroom",
        "bathroom.*sidewalk", "bathroom.*street",
        "relieving", "relieve themselves",
    ]

    for hood in HOODS:
        print(f"\n--- {hood} ---")
        all_found = {}

        for term in waste_terms:
            if ".*" in term:
                continue

            # Search across ALL text fields using _full_text
            sql = f'''SELECT "case_enquiry_id", "type", "case_title", "reason",
                             "queue", "department", "case_status", "closure_reason",
                             "source", "open_dt", "location_street_name",
                             "neighborhood", "on_time", "_full_text"
                      FROM "{resource_id}"
                      WHERE "neighborhood" = '{hood}'
                      AND (LOWER("_full_text"::text) LIKE '%{term}%'
                           OR LOWER("closure_reason") LIKE '%{term}%'
                           OR LOWER("case_title") LIKE '%{term}%')
                      LIMIT 100'''
            records = sql_query(resource_id, sql)

            if records:
                new = 0
                for r in records:
                    cid = r.get("case_enquiry_id")
                    if cid not in all_found:
                        all_found[cid] = r
                        all_found[cid]["_matched_terms"] = [term]
                        new += 1
                    else:
                        all_found[cid]["_matched_terms"].append(term)
                if new > 0:
                    print(f"  '{term}': {len(records)} hits, {new} new unique")

        print(f"\n  Total unique tickets with waste terms in {hood}: {len(all_found)}")

        # Analyze these tickets
        queue_counts = Counter(r.get("queue", "?") for r in all_found.values())
        type_counts = Counter(r.get("type", "?") for r in all_found.values())
        source_counts = Counter(r.get("source", "?") for r in all_found.values())
        status_counts = Counter(r.get("case_status", "?") for r in all_found.values())

        print(f"\n  Queue distribution:")
        for q, c in queue_counts.most_common(15):
            marker = " <<<< CORRECT" if q == "INFO_HumanWaste" else ""
            marker = " <<<< BIOHAZARD TEAM" if q == "GEN_Needle_Pickup" else marker
            print(f"    {q}: {c}{marker}")

        print(f"\n  Type distribution:")
        for t, c in type_counts.most_common(15):
            print(f"    {t}: {c}")

        print(f"\n  Source distribution:")
        for s, c in source_counts.most_common():
            print(f"    {s}: {c}")

        print(f"\n  Status distribution:")
        for s, c in status_counts.most_common():
            print(f"    {s}: {c}")

        # Show examples grouped by routing
        print(f"\n  --- Correctly routed examples ({hood}) ---")
        correct = [r for r in all_found.values()
                    if r.get("queue") in ("INFO_HumanWaste", "GEN_Needle_Pickup")]
        for r in correct[:5]:
            closure = (r.get("closure_reason") or "")[:150]
            ft_snippet = (r.get("_full_text") or "")[:300]
            print(f"    [{r.get('source')}] type='{r.get('type')}' queue='{r.get('queue')}'")
            print(f"      title: {r.get('case_title')}")
            print(f"      closure: {closure}")
            print(f"      fulltext snippet: {ft_snippet}")
            print()

        print(f"\n  --- Misrouted examples ({hood}) ---")
        misrouted = [r for r in all_found.values()
                     if r.get("queue") not in ("INFO_HumanWaste", "GEN_Needle_Pickup")]
        for r in misrouted[:10]:
            closure = (r.get("closure_reason") or "")[:150]
            ft_snippet = (r.get("_full_text") or "")[:300]
            print(f"    [{r.get('source')}] type='{r.get('type')}' queue='{r.get('queue')}' status={r.get('case_status')}")
            print(f"      title: {r.get('case_title')}")
            print(f"      terms: {r.get('_matched_terms')}")
            print(f"      closure: {closure}")
            print(f"      fulltext snippet: {ft_snippet}")
            print()


def step4_call_center_deep(resource_id: str, year: int):
    """Look specifically at Constituent Call tickets in these hoods for street cleaning.
    Call center operators type notes — these may have descriptions we're missing."""
    print(f"\n{'='*60}")
    print(f"STEP 4: Call center tickets - street cleaning ({year})")
    print(f"{'='*60}")

    for hood in HOODS:
        # Get call center street cleaning tickets and look at closure patterns
        sql = f'''SELECT "case_enquiry_id", "type", "case_title", "queue",
                         "closure_reason", "source", "case_status", "on_time",
                         "open_dt", "closed_dt", "location_street_name"
                  FROM "{resource_id}"
                  WHERE "neighborhood" = '{hood}'
                  AND "source" = 'Constituent Call'
                  AND "type" = 'Requests for Street Cleaning'
                  '''
        records = sql_query(resource_id, sql)
        print(f"\n  {hood}: {len(records)} call center street cleaning tickets")

        # Look for human waste signals in closure text
        hw_signals = []
        for r in records:
            closure = (r.get("closure_reason") or "").lower()
            if any(w in closure for w in [
                "feces", "fecal", "poop", "waste", "defec", "urine",
                "biohazard", "bodily", "excrement", "human",
                "does not servi", "not our", "we don't",
                "outside contractor", "sharps team",
                "doo doo", "soiled", "smear",
            ]):
                hw_signals.append(r)

        print(f"    With human waste signals in closure: {len(hw_signals)}")
        for r in hw_signals[:5]:
            print(f"      queue='{r.get('queue')}' closure: {(r.get('closure_reason') or '')[:150]}")

        # Now check what % get closed without explanation (possible silent mishandling)
        no_closure = [r for r in records if not (r.get("closure_reason") or "").strip()]
        closed_noted = [r for r in records
                        if "Noted" in (r.get("closure_reason") or "")
                        and len((r.get("closure_reason") or "")) < 80]
        print(f"    Closed with no explanation: {len(no_closure)}")
        print(f"    Closed with brief 'Noted': {len(closed_noted)}")

        # Queue distribution for call center tickets
        q_counts = Counter(r.get("queue", "?") for r in records)
        print(f"    Queue distribution:")
        for q, c in q_counts.most_common(10):
            print(f"      {q}: {c}")


def step5_all_closed_quickly(resource_id: str, year: int):
    """Find tickets closed same day or very quickly — could be auto-closed misroutes."""
    print(f"\n{'='*60}")
    print(f"STEP 5: Quick-closed street cleaning tickets ({year})")
    print(f"{'='*60}")

    for hood in HOODS:
        # Tickets closed same day with minimal closure reason
        sql = f'''SELECT "case_enquiry_id", "type", "case_title", "queue",
                         "closure_reason", "source", "case_status",
                         "open_dt", "closed_dt", "location_street_name"
                  FROM "{resource_id}"
                  WHERE "neighborhood" = '{hood}'
                  AND "type" = 'Requests for Street Cleaning'
                  AND "case_status" = 'Closed'
                  AND (LOWER("closure_reason") LIKE '%case noted%'
                       OR LOWER("closure_reason") LIKE '%case invalid%'
                       OR LOWER("closure_reason") LIKE '%case closed%'
                       OR "closure_reason" = ''
                       OR "closure_reason" IS NULL)
                  LIMIT 200'''
        records = sql_query(resource_id, sql)
        print(f"\n  {hood}: {len(records)} street cleaning closed with minimal/no explanation")

        q_counts = Counter(r.get("queue", "?") for r in records)
        print(f"    Queue distribution:")
        for q, c in q_counts.most_common(10):
            print(f"      {q}: {c}")

        # Show some examples
        for r in records[:5]:
            print(f"    [{r.get('source')}] queue='{r.get('queue')}' opened={r.get('open_dt')} closed={r.get('closed_dt')}")
            print(f"      closure: {(r.get('closure_reason') or '(empty)')[:150]}")


def step6_additional_slang(resource_id: str, year: int):
    """Try additional terms people might use in South End / Roxbury."""
    print(f"\n{'='*60}")
    print(f"STEP 6: Additional slang/terms search ({year})")
    print(f"{'='*60}")

    extra_terms = [
        "bathroom on", "bathroom in the", "using the street",
        "going to the bathroom", "went to the bathroom",
        "relieving", "relieve himself", "relieve herself",
        "number 2", "number two",
        "doo doo", "doodoo", "doody",
        "soiled", "smeared",
        "stench", "smells like",
        "disgusting", "unsanitary",
        "tent", "sleeping bag",  # encampment signals
        "homeless person", "homeless man", "homeless woman",
        "camp", "encampment",
    ]

    for hood in HOODS:
        print(f"\n--- {hood} ---")
        for term in extra_terms:
            sql = f'''SELECT COUNT(*) as cnt
                      FROM "{resource_id}"
                      WHERE "neighborhood" = '{hood}'
                      AND (LOWER("closure_reason") LIKE '%{term}%'
                           OR LOWER("_full_text"::text) LIKE '%{term}%')
                      '''
            records = sql_query(resource_id, sql)
            cnt = records[0].get("cnt", 0) if records else 0
            if int(cnt) > 0:
                print(f"  '{term}': {cnt} hits")

                # Show a few examples
                sql2 = f'''SELECT "case_enquiry_id", "type", "queue", "closure_reason",
                                  "source", "_full_text"
                           FROM "{resource_id}"
                           WHERE "neighborhood" = '{hood}'
                           AND (LOWER("closure_reason") LIKE '%{term}%'
                                OR LOWER("_full_text"::text) LIKE '%{term}%')
                           LIMIT 3'''
                examples = sql_query(resource_id, sql2)
                for ex in examples:
                    ft = (ex.get("_full_text") or "")[:200]
                    cl = (ex.get("closure_reason") or "")[:100]
                    print(f"    type='{ex.get('type')}' queue='{ex.get('queue')}' src={ex.get('source')}")
                    print(f"      closure: {cl}")
                    if term.lower() not in cl.lower():
                        print(f"      fulltext: {ft}")


def main():
    # Use 2025 as primary (most complete recent year)
    year = 2025
    rid = RESOURCE_IDS[year]

    step1_check_all_fields(rid, year)
    step2_source_distribution(rid, year)
    step3_fulltext_search(rid, year)
    step4_call_center_deep(rid, year)
    step5_all_closed_quickly(rid, year)
    step6_additional_slang(rid, year)

    # Also do fulltext search for 2024
    print(f"\n\n{'#'*60}")
    print("REPEAT STEP 3 FOR 2024")
    print(f"{'#'*60}")
    step3_fulltext_search(RESOURCE_IDS[2024], 2024)


if __name__ == "__main__":
    main()
