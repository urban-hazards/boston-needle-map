"""Compare Open311 API records against CKAN bulk data for the same tickets.

Produces a rigorous, reproducible report showing exactly what fields CKAN
strips from the public data. Designed to withstand scrutiny — every claim
is backed by specific ticket IDs and percentages.

Usage:
    python compare.py                          # compare all types, 3 recent days
    python compare.py --days 7                 # compare 7 days
    python compare.py --type needles           # compare only needle tickets
    python compare.py --output report.json     # save structured report
"""

import argparse
import json
import logging
import time
import urllib.parse
import urllib.request
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# --- API endpoints ---

OPEN311_BASE = "https://boston2-production.spotmobile.net/open311/v2"
CKAN_BASE = "https://data.boston.gov/api/3/action"
UA = "BostonHazardResearch/1.0 (public-health-research)"
DELAY = 7.0  # 10 req/min limit on Open311

# CKAN resource IDs by year
CKAN_RESOURCES = {
    2024: "dff4d804-5031-443a-8409-8344efd0e5c8",
    2025: "9d7c2214-4709-478a-a2e8-fb2020a5bb94",
    2026: "1a0b420d-99f1-4887-9851-990b2a5a6e17",
}

# Types to compare: (slug, open311_service_code, ckan_type_name)
COMPARE_TYPES = [
    ("needles", "Mayor's 24 Hour Hotline:Needle Program:Needle Pickup", "Needle Pickup"),
    ("encampments", "Mayor's 24 Hour Hotline:Quality of Life:Encampments", "Encampments"),
    ("potholes", "Public Works Department:Highway Maintenance:Request for Pothole Repair", "Request for Pothole Repair"),
    ("dead-animals", "Public Works Department:Street Cleaning:Pick up Dead Animal", "Pick up Dead Animal"),
    ("sidewalks", "Public Works Department:Highway Maintenance:Sidewalk Repair (Make Safe)", "Sidewalk Repair (Make Safe)"),
]


def api_get(url: str, timeout: int = 30):
    """GET a URL, return parsed JSON."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def fetch_open311_day(day: date, service_code: str) -> list[dict]:
    """Fetch all Open311 tickets for a day and service type."""
    all_records = []
    page = 1
    while True:
        params = urllib.parse.urlencode({
            "start_date": f"{day}T00:00:00Z",
            "end_date": f"{day}T23:59:59Z",
            "service_code": service_code,
            "per_page": 100,
            "page": page,
        })
        url = f"{OPEN311_BASE}/requests.json?{params}"
        try:
            data = api_get(url)
        except Exception as e:
            log.warning("Open311 error for %s: %s", day, e)
            break
        if not data:
            break
        all_records.extend(data)
        if len(data) >= 100:
            page += 1
            time.sleep(DELAY)
        else:
            break
    return all_records


def fetch_ckan_day(day: date, ckan_type: str) -> list[dict]:
    """Fetch all CKAN records for a day and type."""
    resource_id = CKAN_RESOURCES.get(day.year)
    if not resource_id:
        return []

    next_day = day + timedelta(days=1)
    sql = (
        f'SELECT * FROM "{resource_id}" '
        f"WHERE \"type\" = '{ckan_type}' "
        f"AND \"open_dt\" >= '{day}' AND \"open_dt\" < '{next_day}'"
    )
    url = f"{CKAN_BASE}/datastore_search_sql?sql={urllib.parse.quote(sql)}"
    try:
        data = api_get(url, timeout=60)
        if data and data.get("success"):
            return data["result"]["records"]
    except Exception as e:
        log.warning("CKAN error for %s %s: %s", ckan_type, day, e)
    return []


def compare_records(open311_records: list[dict], ckan_records: list[dict]) -> dict:
    """Compare Open311 and CKAN records field-by-field.

    Returns structured comparison with specific examples and percentages.
    """
    # Try to match by service_request_id (Open311) to case_enquiry_id (CKAN)
    ckan_by_id = {}
    for r in ckan_records:
        cid = str(r.get("case_enquiry_id", "")).strip()
        if cid:
            ckan_by_id[cid] = r

    matched = []
    open311_only = []

    for rec in open311_records:
        srid = str(rec.get("service_request_id", "")).strip()
        ckan_rec = ckan_by_id.pop(srid, None)
        if ckan_rec:
            matched.append((rec, ckan_rec))
        else:
            open311_only.append(rec)

    ckan_only = list(ckan_by_id.values())

    # Analyze matched records for stripped fields
    desc_in_api = 0
    desc_in_ckan = 0
    photo_in_api = 0
    photo_in_ckan = 0
    notes_in_api = 0
    notes_in_ckan = 0
    examples = []

    for api_rec, ckan_rec in matched:
        # Description
        api_desc = (api_rec.get("description") or "").strip()
        ckan_desc = (ckan_rec.get("closure_reason") or "").strip()
        # CKAN has no "description" field — closest is closure_reason
        ckan_has_desc = bool(ckan_rec.get("description", "").strip()) if "description" in ckan_rec else False

        if api_desc:
            desc_in_api += 1
        if ckan_has_desc:
            desc_in_ckan += 1

        # Photo
        api_photo = (api_rec.get("media_url") or "").strip()
        ckan_photo = (ckan_rec.get("submitted_photo") or "").strip()
        if api_photo:
            photo_in_api += 1
        if ckan_photo:
            photo_in_ckan += 1

        # Status notes
        api_notes = (api_rec.get("status_notes") or "").strip()
        ckan_closure = (ckan_rec.get("closure_reason") or "").strip()
        if api_notes:
            notes_in_api += 1
        if ckan_closure:
            notes_in_ckan += 1

        # Collect examples (first 5 with descriptions)
        if api_desc and len(examples) < 5:
            examples.append({
                "ticket_id": api_rec.get("service_request_id"),
                "date": api_rec.get("requested_datetime", "")[:10],
                "api_description": api_desc[:500],
                "api_photo_url": api_photo or None,
                "api_status_notes": api_notes[:500] if api_notes else None,
                "ckan_has_description": ckan_has_desc,
                "ckan_submitted_photo": ckan_photo or None,
                "ckan_closure_reason": ckan_closure[:500] if ckan_closure else None,
                "address": api_rec.get("address", ""),
            })

    total_matched = len(matched)

    return {
        "counts": {
            "open311": len(open311_records),
            "ckan": len(ckan_records),
            "matched_by_id": total_matched,
            "open311_only": len(open311_only),
            "ckan_only": len(ckan_only),
        },
        "stripped_fields": {
            "description": {
                "in_api": desc_in_api,
                "in_ckan": desc_in_ckan,
                "pct_with_desc_api": round(desc_in_api / total_matched * 100, 1) if total_matched else 0,
                "pct_with_desc_ckan": round(desc_in_ckan / total_matched * 100, 1) if total_matched else 0,
            },
            "photos": {
                "in_api": photo_in_api,
                "in_ckan": photo_in_ckan,
                "pct_with_photo_api": round(photo_in_api / total_matched * 100, 1) if total_matched else 0,
                "pct_with_photo_ckan": round(photo_in_ckan / total_matched * 100, 1) if total_matched else 0,
            },
            "status_notes": {
                "in_api": notes_in_api,
                "in_ckan_closure_reason": notes_in_ckan,
                "pct_with_notes_api": round(notes_in_api / total_matched * 100, 1) if total_matched else 0,
                "pct_with_closure_ckan": round(notes_in_ckan / total_matched * 100, 1) if total_matched else 0,
            },
        },
        "examples": examples,
    }


def main():
    parser = argparse.ArgumentParser(description="Compare Open311 API vs CKAN bulk data")
    parser.add_argument("--days", type=int, default=3, help="Number of recent days to compare")
    parser.add_argument("--type", default=None, help="Compare only this type slug")
    parser.add_argument("--end", default=None, help="End date (default: 5 days ago, to avoid CKAN lag)")
    parser.add_argument("--output", default=None, help="Save JSON report to this file")
    args = parser.parse_args()

    if args.end:
        end = date.fromisoformat(args.end)
    else:
        # CKAN bulk export lags a few days — default to 5 days ago
        end = date.today() - timedelta(days=5)
    start = end - timedelta(days=args.days - 1)
    days = [start + timedelta(days=i) for i in range(args.days)]

    types_to_compare = COMPARE_TYPES
    if args.type:
        types_to_compare = [t for t in COMPARE_TYPES if t[0] == args.type]
        if not types_to_compare:
            log.error("Unknown type: %s", args.type)
            return

    report = {
        "generated": str(date.today()),
        "date_range": {"start": str(start), "end": str(end)},
        "methodology": (
            "For each ticket type and date, we fetched records from both the Open311 API "
            "and the CKAN bulk data API. Records were matched by service_request_id (Open311) "
            "to case_enquiry_id (CKAN). For each matched pair, we checked whether the citizen's "
            "description, submitted photo, and status notes survived into the CKAN export."
        ),
        "types": {},
    }

    for slug, service_code, ckan_type in types_to_compare:
        log.info("=== Comparing %s ===", slug)
        type_results = {"days": {}, "totals": None}

        all_api = []
        all_ckan = []

        for day in days:
            log.info("  Fetching %s from Open311...", day)
            api_records = fetch_open311_day(day, service_code)
            time.sleep(DELAY)

            log.info("  Fetching %s from CKAN...", day)
            ckan_records = fetch_ckan_day(day, ckan_type)

            log.info("  Open311: %d, CKAN: %d", len(api_records), len(ckan_records))

            comparison = compare_records(api_records, ckan_records)
            type_results["days"][str(day)] = comparison

            all_api.extend(api_records)
            all_ckan.extend(ckan_records)

            time.sleep(2)  # be nice to CKAN too

        # Compute totals across all days
        type_results["totals"] = compare_records(all_api, all_ckan)
        report["types"][slug] = type_results

        log.info("  TOTALS for %s:", slug)
        totals = type_results["totals"]
        sf = totals["stripped_fields"]
        log.info("    Matched: %d records", totals["counts"]["matched_by_id"])
        log.info("    Descriptions: %s%% in API, %s%% in CKAN",
                 sf["description"]["pct_with_desc_api"],
                 sf["description"]["pct_with_desc_ckan"])
        log.info("    Photos: %s%% in API, %s%% in CKAN",
                 sf["photos"]["pct_with_photo_api"],
                 sf["photos"]["pct_with_photo_ckan"])

    # Print summary
    print("\n" + "=" * 70)
    print("CKAN vs Open311 DATA COMPARISON REPORT")
    print("=" * 70)
    print(f"Date range: {start} to {end} ({args.days} days)")
    print()

    for slug, data in report["types"].items():
        totals = data["totals"]
        counts = totals["counts"]
        sf = totals["stripped_fields"]

        print(f"\n--- {slug.upper()} ---")
        print(f"  Records: {counts['open311']} (API) vs {counts['ckan']} (CKAN), {counts['matched_by_id']} matched")
        print(f"  Descriptions:  {sf['description']['pct_with_desc_api']}% in API → {sf['description']['pct_with_desc_ckan']}% in CKAN")
        print(f"  Photos:        {sf['photos']['pct_with_photo_api']}% in API → {sf['photos']['pct_with_photo_ckan']}% in CKAN")
        print(f"  Status notes:  {sf['status_notes']['pct_with_notes_api']}% in API → closure_reason in {sf['status_notes']['pct_with_closure_ckan']}% of CKAN")

        if totals["examples"]:
            print(f"\n  Example ticket #{totals['examples'][0]['ticket_id']}:")
            ex = totals["examples"][0]
            print(f"    Address: {ex['address']}")
            desc = ex["api_description"]
            print(f"    API description: \"{desc[:120]}{'...' if len(desc) > 120 else ''}\"")
            print(f"    CKAN has description: {ex['ckan_has_description']}")
            print(f"    API photo: {'Yes' if ex['api_photo_url'] else 'No'}")
            print(f"    CKAN photo: {'Yes' if ex['ckan_submitted_photo'] else 'No'}")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\nFull report saved to {args.output}")


if __name__ == "__main__":
    main()
