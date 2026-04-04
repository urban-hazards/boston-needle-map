"""
Systematic audit of Boston 311 data feed quality issues.

Check for:
1. Missing/null fields
2. Inconsistent values
3. Geocoding issues
4. Timing anomalies
5. Closure reason quality
6. Source/routing gaps
7. Duplicate detection
8. SLA gaming
"""

import json
import urllib.parse
import urllib.request
from collections import Counter

CKAN_BASE = "https://data.boston.gov/api/3/action"
UA = "Boston311Research/1.0 (public-health-research)"

RID_2025 = "9d7c2214-4709-478a-a2e8-fb2020a5bb94"
RID_2024 = "dff4d804-5031-443a-8409-8344efd0e5c8"


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


def count(resource_id: str, where: str = "1=1") -> int:
    sql = f'SELECT COUNT(*) as cnt FROM "{resource_id}" WHERE {where}'
    records = sql_query(resource_id, sql)
    return int(records[0]["cnt"]) if records else 0


def audit(rid: str, year: int):
    print(f"\n{'#'*70}")
    print(f"DATA QUALITY AUDIT: {year}")
    print(f"{'#'*70}")

    total = count(rid)
    print(f"\nTotal records: {total}")

    # 1. NO DESCRIPTION FIELD
    print(f"\n{'='*60}")
    print("1. MISSING DESCRIPTION / COMPLAINT TEXT FIELD")
    print(f"{'='*60}")
    sql = f'SELECT * FROM "{rid}" LIMIT 1'
    records = sql_query(rid, sql)
    if records:
        cols = sorted(records[0].keys())
        print(f"  Available columns ({len(cols)}): {cols}")
        desc_cols = [c for c in cols if any(w in c.lower() for w in
                     ['desc', 'detail', 'comment', 'note', 'text', 'narr', 'body', 'message'])]
        print(f"  Description-like columns: {desc_cols if desc_cols else 'NONE'}")
        print(f"  _full_text is a PostgreSQL tsvector index, NOT a description field")

    # 2. NULL / EMPTY FIELDS
    print(f"\n{'='*60}")
    print("2. NULL / EMPTY FIELD RATES")
    print(f"{'='*60}")
    fields_to_check = [
        "closure_reason", "closed_dt", "location_street_name",
        "location_zipcode", "neighborhood", "latitude", "longitude",
        "submitted_photo", "closed_photo", "sla_target_dt",
        "on_time", "source"
    ]
    for f in fields_to_check:
        null_count = count(rid, f'"{f}" IS NULL OR "{f}" = \'\'')
        pct = (null_count / total * 100) if total else 0
        flag = " <<<" if pct > 30 else ""
        print(f"  {f}: {null_count} null/empty ({pct:.1f}%){flag}")

    # 3. CLOSURE REASON QUALITY
    print(f"\n{'='*60}")
    print("3. CLOSURE REASON QUALITY")
    print(f"{'='*60}")

    closed_total = count(rid, '"case_status" = \'Closed\'')
    no_closure = count(rid, '"case_status" = \'Closed\' AND ("closure_reason" IS NULL OR "closure_reason" = \'\')')
    minimal = count(rid, '"case_status" = \'Closed\' AND LENGTH("closure_reason") < 60 AND "closure_reason" IS NOT NULL AND "closure_reason" != \'\'')
    print(f"  Total closed: {closed_total}")
    print(f"  Closed with NO closure reason: {no_closure} ({no_closure/closed_total*100:.1f}%)")
    print(f"  Closed with <60 char reason: {minimal} ({minimal/closed_total*100:.1f}%)")

    # Check for template/boilerplate closures
    sql = f'''SELECT "closure_reason", COUNT(*) as cnt
              FROM "{rid}"
              WHERE "case_status" = 'Closed'
              AND "closure_reason" IS NOT NULL AND "closure_reason" != ''
              GROUP BY "closure_reason"
              ORDER BY cnt DESC
              LIMIT 20'''
    records = sql_query(rid, sql)
    print(f"\n  Top 20 most repeated closure reasons:")
    for r in records:
        print(f"    {r['cnt']}x: {(r['closure_reason'] or '')[:100]}")

    # 4. CASE STATUS DISTRIBUTION
    print(f"\n{'='*60}")
    print("4. CASE STATUS DISTRIBUTION")
    print(f"{'='*60}")
    sql = f'''SELECT "case_status", COUNT(*) as cnt
              FROM "{rid}"
              GROUP BY "case_status"
              ORDER BY cnt DESC'''
    records = sql_query(rid, sql)
    for r in records:
        print(f"  {r.get('case_status', '?')}: {r['cnt']}")

    # 5. OPEN CASES WITH NO ACTIVITY
    print(f"\n{'='*60}")
    print("5. STALE OPEN CASES (open > 30 days, still open)")
    print(f"{'='*60}")
    stale = count(rid, '''"case_status" = 'Open' AND "open_dt" < '2025-09-01' ''')
    overdue = count(rid, '''"on_time" = 'OVERDUE' ''')
    print(f"  Open cases opened before Sept 2025: {stale}")
    print(f"  Marked OVERDUE: {overdue}")

    # 6. GEOCODING ISSUES
    print(f"\n{'='*60}")
    print("6. GEOCODING / LOCATION ISSUES")
    print(f"{'='*60}")
    no_geo = count(rid, '"latitude" IS NULL OR "longitude" IS NULL OR "latitude" = \'\' OR "longitude" = \'\'')
    zero_geo = count(rid, '"latitude" = \'0\' OR "longitude" = \'0\'')
    # Out of Boston bounding box
    out_bbox = count(rid, '''
        "latitude" IS NOT NULL AND "latitude" != '' AND "longitude" IS NOT NULL AND "longitude" != ''
        AND (CAST("latitude" AS FLOAT) < 42.2279 OR CAST("latitude" AS FLOAT) > 42.3969
             OR CAST("longitude" AS FLOAT) < -71.1912 OR CAST("longitude" AS FLOAT) > -70.9235)
    ''')
    print(f"  No lat/lon: {no_geo}")
    print(f"  Zero lat or lon: {zero_geo}")
    print(f"  Outside Boston bounding box: {out_bbox}")

    # Neighborhood mismatches
    no_hood = count(rid, '"neighborhood" IS NULL OR "neighborhood" = \'\'')
    print(f"  No neighborhood: {no_hood}")

    # Check for suspicious default locations
    sql = f'''SELECT "location_street_name", COUNT(*) as cnt
              FROM "{rid}"
              WHERE "location_street_name" IS NOT NULL AND "location_street_name" != ''
              GROUP BY "location_street_name"
              ORDER BY cnt DESC
              LIMIT 10'''
    records = sql_query(rid, sql)
    print(f"\n  Top 10 most-used addresses (possible defaults/centroids):")
    for r in records:
        print(f"    {r['cnt']}x: {r['location_street_name']}")

    # 7. SOURCE FIELD ISSUES
    print(f"\n{'='*60}")
    print("7. SOURCE / INTAKE CHANNEL ANALYSIS")
    print(f"{'='*60}")
    sql = f'''SELECT "source", COUNT(*) as cnt
              FROM "{rid}"
              GROUP BY "source"
              ORDER BY cnt DESC'''
    records = sql_query(rid, sql)
    for r in records:
        print(f"  {r.get('source', '(null)')}: {r['cnt']}")

    # 8. TYPE/QUEUE CONSISTENCY
    print(f"\n{'='*60}")
    print("8. TYPE vs QUEUE ROUTING CONSISTENCY")
    print(f"{'='*60}")

    # Types that go to many different queues (routing chaos)
    sql = f'''SELECT "type", COUNT(DISTINCT "queue") as queue_count, COUNT(*) as total
              FROM "{rid}"
              GROUP BY "type"
              HAVING COUNT(DISTINCT "queue") > 10
              ORDER BY queue_count DESC
              LIMIT 15'''
    records = sql_query(rid, sql)
    print(f"\n  Types routed to 10+ different queues (routing chaos):")
    for r in records:
        print(f"    '{r['type']}': {r['queue_count']} queues, {r['total']} tickets")

    # 9. DUPLICATE DETECTION
    print(f"\n{'='*60}")
    print("9. DUPLICATE TICKETS")
    print(f"{'='*60}")
    dup_closure = count(rid, '''LOWER("closure_reason") LIKE '%duplicate%' OR LOWER("closure_reason") LIKE '%dup %' OR LOWER("closure_reason") LIKE '%dupe%' ''')
    print(f"  Closed as duplicate (in closure_reason): {dup_closure}")

    # Same location, same type, same day
    sql = f'''SELECT "location_street_name", "type", SUBSTRING("open_dt", 1, 10) as day, COUNT(*) as cnt
              FROM "{rid}"
              WHERE "location_street_name" IS NOT NULL AND "location_street_name" != ''
              GROUP BY "location_street_name", "type", SUBSTRING("open_dt", 1, 10)
              HAVING COUNT(*) > 3
              ORDER BY cnt DESC
              LIMIT 10'''
    records = sql_query(rid, sql)
    print(f"\n  Same address + type + day (>3 tickets):")
    for r in records:
        print(f"    {r['cnt']}x: {r['location_street_name']} | {r['type']} | {r['day']}")

    # 10. SLA / ON_TIME FIELD
    print(f"\n{'='*60}")
    print("10. SLA / ON_TIME ANALYSIS")
    print(f"{'='*60}")
    sql = f'''SELECT "on_time", COUNT(*) as cnt
              FROM "{rid}"
              GROUP BY "on_time"
              ORDER BY cnt DESC'''
    records = sql_query(rid, sql)
    for r in records:
        print(f"  {r.get('on_time', '(null)')}: {r['cnt']}")

    # Cases closed before SLA but with no actual resolution
    print(f"\n  'ONTIME' but no closure reason (auto-closed?):")
    auto_closed = count(rid, '''"on_time" = 'ONTIME' AND ("closure_reason" IS NULL OR "closure_reason" = '')''')
    print(f"    {auto_closed}")

    # 11. NEIGHBORHOOD INCONSISTENCY
    print(f"\n{'='*60}")
    print("11. NEIGHBORHOOD NAME CONSISTENCY")
    print(f"{'='*60}")
    sql = f'''SELECT DISTINCT "neighborhood" FROM "{rid}" ORDER BY "neighborhood"'''
    records = sql_query(rid, sql)
    hoods = [r.get("neighborhood", "") for r in records]
    print(f"  Distinct neighborhood values ({len(hoods)}):")
    for h in hoods:
        print(f"    '{h}'")

    # 12. DEPARTMENT / QUEUE FIELD FRAGMENTATION
    print(f"\n{'='*60}")
    print("12. QUEUE FRAGMENTATION")
    print(f"{'='*60}")
    distinct_queues = count(rid, '1=1')  # hack
    sql = f'''SELECT COUNT(DISTINCT "queue") as cnt FROM "{rid}"'''
    records = sql_query(rid, sql)
    q_count = records[0]["cnt"] if records else 0
    sql = f'''SELECT COUNT(DISTINCT "department") as cnt FROM "{rid}"'''
    records = sql_query(rid, sql)
    d_count = records[0]["cnt"] if records else 0
    sql = f'''SELECT COUNT(DISTINCT "type") as cnt FROM "{rid}"'''
    records = sql_query(rid, sql)
    t_count = records[0]["cnt"] if records else 0
    print(f"  Distinct queues: {q_count}")
    print(f"  Distinct departments: {d_count}")
    print(f"  Distinct types: {t_count}")

    # 13. PHOTO FIELD ANALYSIS
    print(f"\n{'='*60}")
    print("13. PHOTO EVIDENCE")
    print(f"{'='*60}")
    has_photo = count(rid, '"submitted_photo" IS NOT NULL AND "submitted_photo" != \'\'')
    has_close_photo = count(rid, '"closed_photo" IS NOT NULL AND "closed_photo" != \'\'')
    print(f"  Has submitted photo: {has_photo} ({has_photo/total*100:.1f}%)")
    print(f"  Has closed photo: {has_close_photo} ({has_close_photo/total*100:.1f}%)")

    # 14. Check if closed_dt is before open_dt (time travel)
    print(f"\n{'='*60}")
    print("14. TEMPORAL ANOMALIES")
    print(f"{'='*60}")
    time_travel = count(rid, '''"closed_dt" IS NOT NULL AND "closed_dt" != '' AND "closed_dt" < "open_dt"''')
    print(f"  Closed before opened: {time_travel}")

    # Instant closures (< 1 minute)
    sql = f'''SELECT COUNT(*) as cnt FROM "{rid}"
              WHERE "closed_dt" IS NOT NULL AND "closed_dt" != ''
              AND "open_dt" IS NOT NULL AND "open_dt" != ''
              AND "closed_dt"::timestamp - "open_dt"::timestamp < interval '1 minute'
              AND "closed_dt"::timestamp > "open_dt"::timestamp'''
    records = sql_query(rid, sql)
    instant = records[0]["cnt"] if records else "query failed"
    print(f"  Closed within 1 minute of opening: {instant}")


def main():
    audit(RID_2025, 2025)
    # Just key checks for 2024
    print(f"\n\n{'#'*70}")
    print("COMPARISON: 2024 KEY METRICS")
    print(f"{'#'*70}")
    total_2024 = count(RID_2024)
    no_closure_2024 = count(RID_2024, '"case_status" = \'Closed\' AND ("closure_reason" IS NULL OR "closure_reason" = \'\')')
    no_geo_2024 = count(RID_2024, '"latitude" IS NULL OR "longitude" IS NULL OR "latitude" = \'\' OR "longitude" = \'\'')
    print(f"  Total records: {total_2024}")
    print(f"  Closed with no closure reason: {no_closure_2024}")
    print(f"  No geocoding: {no_geo_2024}")


if __name__ == "__main__":
    main()
