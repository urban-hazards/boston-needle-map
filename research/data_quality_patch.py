"""Quick patch queries for items that 502'd in the main audit."""

import json
import urllib.parse
import urllib.request

CKAN_BASE = "https://data.boston.gov/api/3/action"
UA = "Boston311Research/1.0 (public-health-research)"
RID = "9d7c2214-4709-478a-a2e8-fb2020a5bb94"


def sql_query(resource_id: str, sql: str) -> list[dict]:
    url = f"{CKAN_BASE}/datastore_search_sql?sql={urllib.parse.quote(sql)}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data and data.get("success"):
                return data["result"]["records"]
    except Exception as e:
        print(f"  API error: {e}")
    return []


# ON_TIME distribution
print("ON_TIME distribution:")
records = sql_query(RID, f'''SELECT "on_time", COUNT(*) as cnt FROM "{RID}" GROUP BY "on_time" ORDER BY cnt DESC''')
for r in records:
    print(f"  {r.get('on_time', '(null)')}: {r['cnt']}")

# Neighborhoods
print("\nDistinct neighborhoods:")
records = sql_query(RID, f'''SELECT DISTINCT "neighborhood" FROM "{RID}" WHERE "neighborhood" IS NOT NULL AND "neighborhood" != '' ORDER BY "neighborhood"''')
for r in records:
    print(f"  '{r['neighborhood']}'")

# Photos
print("\nPhoto stats:")
records = sql_query(RID, f'''SELECT COUNT(*) as cnt FROM "{RID}" WHERE "submitted_photo" IS NOT NULL AND "submitted_photo" != '' ''')
print(f"  Has submitted photo: {records[0]['cnt'] if records else '?'}")
records = sql_query(RID, f'''SELECT COUNT(*) as cnt FROM "{RID}" WHERE "closed_photo" IS NOT NULL AND "closed_photo" != '' ''')
print(f"  Has closed photo: {records[0]['cnt'] if records else '?'}")

# Total for percentage
records = sql_query(RID, f'''SELECT COUNT(*) as cnt FROM "{RID}"''')
print(f"  Total: {records[0]['cnt'] if records else '?'}")

# Instant closures
print("\nTemporal anomalies:")
records = sql_query(RID, f'''SELECT COUNT(*) as cnt FROM "{RID}" WHERE "closed_dt" IS NOT NULL AND "closed_dt" != '' AND "closed_dt" < "open_dt"''')
print(f"  Closed before opened: {records[0]['cnt'] if records else '?'}")

# Distinct queues
print("\nFragmentation:")
records = sql_query(RID, f'''SELECT COUNT(DISTINCT "queue") as cnt FROM "{RID}"''')
print(f"  Distinct queues: {records[0]['cnt'] if records else '?'}")

# ONTIME with no closure
print("\nSLA gaming check:")
records = sql_query(RID, f'''SELECT COUNT(*) as cnt FROM "{RID}" WHERE "on_time" = 'ONTIME' AND "case_status" = 'Closed' AND ("closure_reason" LIKE 'Case Closed Case Invalid%' OR "closure_reason" LIKE 'Case Closed Case Noted%')''')
print(f"  ONTIME + closed as Invalid/Noted (no real resolution): {records[0]['cnt'] if records else '?'}")

records = sql_query(RID, f'''SELECT COUNT(*) as cnt FROM "{RID}" WHERE "on_time" = 'ONTIME' AND "case_status" = 'Closed'  ''')
print(f"  Total ONTIME + Closed: {records[0]['cnt'] if records else '?'}")

# Check for cases re-opened or re-assigned
print("\nCase Invalid stats:")
records = sql_query(RID, f'''SELECT COUNT(*) as cnt FROM "{RID}" WHERE "closure_reason" LIKE '%Case Invalid%' ''')
print(f"  Closed as Invalid: {records[0]['cnt'] if records else '?'}")
records = sql_query(RID, f'''SELECT COUNT(*) as cnt FROM "{RID}" WHERE "closure_reason" LIKE '%Referred to External%' ''')
print(f"  Referred to External Agency: {records[0]['cnt'] if records else '?'}")
records = sql_query(RID, f'''SELECT COUNT(*) as cnt FROM "{RID}" WHERE "closure_reason" LIKE '%not our%' OR "closure_reason" LIKE '%Not our%' OR "closure_reason" LIKE '%does not servi%' ''')
print(f"  'Not ours' / 'does not service': {records[0]['cnt'] if records else '?'}")
