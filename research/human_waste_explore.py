"""
Research script: Explore Boston 311 data for human waste / biohazard tickets.

Strategy:
1. Find all distinct "type" and "queue" values that relate to biohazard/blood/vomit
2. Pull all tickets routed to that team
3. Identify common patterns in correctly-routed human waste tickets
4. Search for misrouted tickets using human waste keywords
"""

import json
import urllib.parse
import urllib.request
from collections import Counter

CKAN_BASE = "https://data.boston.gov/api/3/action"
UA = "Boston311Research/1.0 (public-health-research)"

# Use recent years with most data
RESOURCE_IDS = {
    2024: "dff4d804-5031-443a-8409-8344efd0e5c8",
    2025: "9d7c2214-4709-478a-a2e8-fb2020a5bb94",
    2026: "1a0b420d-99f1-4887-9851-990b2a5a6e17",
}


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


def search_types(resource_id: str, year: int):
    """Find all distinct type values that might relate to biohazard."""
    print(f"\n{'='*60}")
    print(f"YEAR {year}: Exploring distinct types and queues")
    print(f"{'='*60}")

    # Step 1: Get all distinct "type" values
    sql = f'SELECT DISTINCT "type" FROM "{resource_id}" ORDER BY "type"'
    records = sql_query(resource_id, sql)
    types = [r["type"] for r in records if r.get("type")]

    # Filter for potentially relevant types
    bio_keywords = ["bio", "hazard", "blood", "vomit", "waste", "needle", "syringe",
                     "feces", "poop", "human", "excrement", "sanit", "clean",
                     "trash", "illegal", "dump", "litter", "rubbish"]

    print(f"\nAll distinct types ({len(types)} total):")
    relevant = []
    for t in types:
        lower = t.lower()
        is_relevant = any(k in lower for k in bio_keywords)
        if is_relevant:
            relevant.append(t)
            print(f"  *** {t}")

    print(f"\nRelevant types: {relevant}")
    return types, relevant


def explore_queues_for_types(resource_id: str, year: int, type_values: list[str]):
    """For relevant types, see what queue/department/reason they map to."""
    print(f"\n--- Queues and departments for relevant types ({year}) ---")

    for t in type_values:
        sql = f'''SELECT DISTINCT "queue", "department", "reason", "subject"
                  FROM "{resource_id}"
                  WHERE "type" = '{t.replace("'", "''")}'
                  '''
        records = sql_query(resource_id, sql)
        if records:
            print(f"\n  Type: '{t}'")
            for r in records:
                print(f"    queue={r.get('queue')}, dept={r.get('department')}, reason={r.get('reason')}, subject={r.get('subject')}")


def search_biohazard_queue(resource_id: str, year: int):
    """Search for queues containing biohazard-related terms."""
    print(f"\n--- Searching queues for biohazard terms ({year}) ---")

    sql = f'''SELECT DISTINCT "queue" FROM "{resource_id}"
              WHERE LOWER("queue") LIKE '%bio%'
              OR LOWER("queue") LIKE '%hazard%'
              OR LOWER("queue") LIKE '%blood%'
              OR LOWER("queue") LIKE '%vomit%'
              OR LOWER("queue") LIKE '%waste%'
              OR LOWER("queue") LIKE '%needle%'
              OR LOWER("queue") LIKE '%sanit%'
              '''
    records = sql_query(resource_id, sql)
    queues = [r["queue"] for r in records if r.get("queue")]
    print(f"  Matching queues: {queues}")
    return queues


def search_by_keywords_in_description(resource_id: str, year: int):
    """Search case_title and closure_reason for human waste keywords."""
    print(f"\n--- Searching case titles for human waste keywords ({year}) ---")

    # Human waste and all its synonyms/slang
    waste_terms = [
        "human waste", "feces", "fecal", "poop", "defecation", "defecate",
        "excrement", "stool", "bowel", "diarrhea",
        "shit", "crap", "turd",
        "urine", "urinate", "pee", "piss",
        "biohazard", "bio hazard", "bio-hazard",
        "bodily fluid", "body fluid",
        "blood", "vomit", "vomiting",
    ]

    results_by_term = {}
    for term in waste_terms:
        sql = f'''SELECT "case_enquiry_id", "type", "case_title", "reason", "queue",
                         "department", "case_status", "closure_reason",
                         "location_street_name", "open_dt"
                  FROM "{resource_id}"
                  WHERE LOWER("case_title") LIKE '%{term}%'
                  OR LOWER("closure_reason") LIKE '%{term}%'
                  OR LOWER("type") LIKE '%{term}%'
                  LIMIT 50'''
        records = sql_query(resource_id, sql)
        if records:
            results_by_term[term] = records
            print(f"\n  '{term}': {len(records)} results")
            # Show routing info
            type_counter = Counter(r.get("type", "?") for r in records)
            queue_counter = Counter(r.get("queue", "?") for r in records)
            status_counter = Counter(r.get("case_status", "?") for r in records)
            print(f"    Types: {dict(type_counter)}")
            print(f"    Queues: {dict(queue_counter)}")
            print(f"    Status: {dict(status_counter)}")
            # Show a few examples
            for r in records[:3]:
                print(f"    Example: type='{r.get('type')}' queue='{r.get('queue')}' "
                      f"title='{r.get('case_title')}' closure='{r.get('closure_reason', '')[:80]}'")

    return results_by_term


def pull_biohazard_team_tickets(resource_id: str, year: int, queue_patterns: list[str]):
    """Pull all tickets for biohazard-related queues to see what types they handle."""
    print(f"\n--- All ticket types handled by biohazard queues ({year}) ---")

    for q in queue_patterns:
        sql = f'''SELECT "type", "case_title", "reason", "closure_reason", "case_status",
                         "queue", "open_dt", "case_enquiry_id", "location_street_name"
                  FROM "{resource_id}"
                  WHERE "queue" = '{q.replace("'", "''")}'
                  LIMIT 200'''
        records = sql_query(resource_id, sql)
        if records:
            print(f"\n  Queue '{q}': {len(records)} records (capped at 200)")
            type_counter = Counter(r.get("type", "?") for r in records)
            reason_counter = Counter(r.get("reason", "?") for r in records)
            title_counter = Counter(r.get("case_title", "?") for r in records)
            print(f"    Types: {dict(type_counter)}")
            print(f"    Reasons: {dict(reason_counter)}")
            print(f"    Titles: {dict(title_counter)}")

            # Show closure reasons for context
            closures = [r.get("closure_reason", "") for r in records if r.get("closure_reason")]
            if closures:
                print(f"    Sample closures:")
                for c in closures[:10]:
                    print(f"      - {c[:120]}")


def count_by_type_and_queue(resource_id: str, year: int, relevant_types: list[str]):
    """Get counts for relevant types by queue to see routing patterns."""
    print(f"\n--- Routing patterns: type -> queue counts ({year}) ---")

    for t in relevant_types:
        sql = f'''SELECT "queue", COUNT(*) as cnt
                  FROM "{resource_id}"
                  WHERE "type" = '{t.replace("'", "''")}'
                  GROUP BY "queue"
                  ORDER BY cnt DESC'''
        records = sql_query(resource_id, sql)
        if records:
            print(f"\n  Type '{t}':")
            for r in records:
                print(f"    {r.get('queue')}: {r.get('cnt')}")


def main():
    # Use 2025 as primary year (most complete recent year)
    year = 2025
    rid = RESOURCE_IDS[year]

    # Step 1: Find all types and filter relevant ones
    all_types, relevant_types = search_types(rid, year)

    # Step 2: Find biohazard-related queues
    bio_queues = search_biohazard_queue(rid, year)

    # Step 3: See what types map to relevant queues/departments
    if relevant_types:
        explore_queues_for_types(rid, year, relevant_types)

    # Step 4: Get routing counts for relevant types
    if relevant_types:
        count_by_type_and_queue(rid, year, relevant_types)

    # Step 5: Pull sample tickets from biohazard queues
    if bio_queues:
        pull_biohazard_team_tickets(rid, year, bio_queues)

    # Step 6: Search across all tickets for human waste keywords
    keyword_results = search_by_keywords_in_description(rid, year)

    # Step 7: Quick check on 2024 too for comparison
    print(f"\n\n{'#'*60}")
    print("CROSS-CHECK: 2024 data")
    print(f"{'#'*60}")
    rid_2024 = RESOURCE_IDS[2024]
    bio_queues_2024 = search_biohazard_queue(rid_2024, 2024)
    if bio_queues_2024:
        pull_biohazard_team_tickets(rid_2024, 2024, bio_queues_2024)
    search_by_keywords_in_description(rid_2024, 2024)


if __name__ == "__main__":
    main()
