"""Fetch all Boston Open311 tickets and store raw JSON in S3.

Pulls ALL 16 service types exposed by the BOS:311 app. Each type gets its
own S3 prefix (open311/{slug}/YYYY-MM-DD.json) so data is organized and
independently resumable.

Why we pull from the API when CKAN bulk data exists:
  1. "Other" (General Request) tickets are MISSING from CKAN entirely — 142k+ invisible tickets
  2. CKAN strips citizen descriptions from ALL types — the free-text field is gone
  3. CKAN strips ALL photos — the submitted_photo column is universally empty
  4. CKAN strips staff status notes — only a truncated closure_reason survives

The API is the only source of the actual human-written reports and photo evidence.

API constraints (from https://boston2-production.spotmobile.net/open311/docs):
  - 10 requests per minute (unauthenticated)
  - 100 results per page max
  - 429 response includes Retry-After header
  - 90-day max date range (we use single days, so N/A)

Usage:
    python fetch.py                        # fetch all types, all days
    python fetch.py --type other           # fetch only "Other" tickets
    python fetch.py --type needles         # fetch only needle tickets
    python fetch.py --start 2025-01-01     # fetch from a specific date
    python fetch.py --dry-run              # show plan without fetching
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta

import boto3

# --- Config from env (same env vars as the main pipeline) ---

BUCKET = os.environ.get("BUCKET", "")
S3_ACCESS_KEY = os.environ.get("ACCESS_KEY_ID", "")
S3_SECRET_KEY = os.environ.get("SECRET_ACCESS_KEY", "")
S3_ENDPOINT = os.environ.get("ENDPOINT", "")
S3_REGION = os.environ.get("REGION", "us-east-1")

# --- Open311 API ---

OPEN311_BASE = "https://boston2-production.spotmobile.net/open311/v2"
START_DATE = "2023-01-01"
UA = "BostonHazardResearch/1.0 (public-health-research)"

# All 16 service types exposed by the BOS:311 app.
# slug -> (service_code, human_name)
SERVICE_TYPES: dict[str, tuple[str, str]] = {
    "other": (
        "Mayor's 24 Hour Hotline:General Request:General Request",
        "Other (General Request)",
    ),
    "needles": (
        "Mayor's 24 Hour Hotline:Needle Program:Needle Pickup",
        "Needle Cleanup",
    ),
    "encampments": (
        "Mayor's 24 Hour Hotline:Quality of Life:Encampments",
        "Encampments",
    ),
    "potholes": (
        "Public Works Department:Highway Maintenance:Request for Pothole Repair",
        "Pothole Repair",
    ),
    "sidewalks": (
        "Public Works Department:Highway Maintenance:Sidewalk Repair (Make Safe)",
        "Broken Sidewalk",
    ),
    "dead-animals": (
        "Public Works Department:Street Cleaning:Pick up Dead Animal",
        "Dead Animal Pickup",
    ),
    "graffiti": (
        "input.Illegal Graffiti",
        "Illegal Graffiti",
    ),
    "litter": (
        "input.Litter",
        "Litter",
    ),
    "rodents": (
        "input.Rodent Sighting",
        "Rodent Sighting",
    ),
    "trash-cans": (
        "input.Overflowing Trash Can",
        "Overflowing Trash Can",
    ),
    "abandoned-vehicles": (
        "Transportation - Traffic Division:Enforcement & Abandoned Vehicles:Abandoned Vehicles",
        "Abandoned Vehicle",
    ),
    "parking": (
        "Transportation - Traffic Division:Enforcement & Abandoned Vehicles:Parking Enforcement",
        "Illegal Parking",
    ),
    "traffic-signals": (
        "Transportation - Traffic Division:Signs & Signals:Traffic Signal Inspection",
        "Traffic Signal",
    ),
    "signs": (
        "Transportation - Traffic Division:Signs & Signals:Sign Repair",
        "Damaged Sign",
    ),
    "abandoned-bikes": (
        "Mayor's 24 Hour Hotline:Abandoned Bicycle:Abandoned Bicycle",
        "Abandoned Bicycle",
    ),
    "illegal-trash": (
        "Public Works Department:Code Enforcement:Improper Storage of Trash (Barrels)",
        "Residential Trash out Illegally",
    ),
}

# --- Rate limiting (API allows 10 req/min = 1 every 6s) ---

DELAY = 7.0
MAX_DELAY = 120
MAX_RETRIES = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def get_s3_client():
    kwargs = {
        "service_name": "s3",
        "region_name": S3_REGION,
    }
    if S3_ACCESS_KEY:
        kwargs["aws_access_key_id"] = S3_ACCESS_KEY
    if S3_SECRET_KEY:
        kwargs["aws_secret_access_key"] = S3_SECRET_KEY
    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT
    return boto3.client(**kwargs)


def list_existing_days(s3, prefix: str) -> set[str]:
    """List day files already in S3 under a given prefix."""
    existing = set()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                day_str = key.removeprefix(prefix).removesuffix(".json")
                if len(day_str) == 10:
                    existing.add(day_str)
    except Exception as e:
        log.warning("Could not list S3 keys at %s: %s", prefix, e)
    return existing


def save_day(s3, prefix: str, day: date, records: list[dict]) -> None:
    """Write a day's records to S3 with record count in metadata."""
    key = f"{prefix}{day}.json"
    body = json.dumps(records, separators=(",", ":"))
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={"record-count": str(len(records))},
    )


def verify_day(s3, prefix: str, day: date, expected_count: int) -> bool:
    """Verify a saved day file has the right record count."""
    key = f"{prefix}{day}.json"
    try:
        resp = s3.head_object(Bucket=BUCKET, Key=key)
        stored_count = resp.get("Metadata", {}).get("record-count")
        if stored_count and int(stored_count) != expected_count:
            log.warning("  MISMATCH %s: expected %d, got %s", day, expected_count, stored_count)
            return False
        return True
    except Exception:
        return False


def _do_request(url: str) -> tuple[list[dict] | None, int | None]:
    """Make a single HTTP request. Returns (data, retry_after_seconds)."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read()), None
    except urllib.error.HTTPError as e:
        if e.code == 429:
            retry_after = e.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else 60
            return None, wait
        raise


def fetch_day(day: date, service_code: str, delay: float) -> tuple[list[dict], float]:
    """Fetch all tickets for a single day and service type with pagination."""
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

        data = None
        for attempt in range(MAX_RETRIES):
            try:
                data, retry_after = _do_request(url)
            except Exception as e:
                log.error("  ERROR %s page %d: %s", day, page, e)
                return all_records, delay

            if data is not None:
                break

            wait = min(retry_after or 60, MAX_DELAY)
            log.info("  RATE LIMITED on %s page %d (attempt %d/%d), Retry-After: %ds",
                     day, page, attempt + 1, MAX_RETRIES, wait)
            time.sleep(wait)
        else:
            log.warning("  GIVING UP on %s after %d retries (will retry next run)", day, MAX_RETRIES)
            return [], delay

        if not data:
            break

        all_records.extend(data)

        if len(data) >= 100:
            page += 1
            time.sleep(delay)
        else:
            break

    return all_records, delay


def fetch_type(
    s3, slug: str, service_code: str, name: str,
    start: date, end: date, delay: float, dry_run: bool,
) -> dict:
    """Fetch all days for a single service type. Returns run stats."""
    prefix = f"open311/{slug}/"

    existing = list_existing_days(s3, prefix)
    days_needed = []
    current = start
    while current <= end:
        if str(current) not in existing:
            days_needed.append(current)
        current += timedelta(days=1)
    days_needed.reverse()

    total_days = (end - start).days + 1
    est_minutes = len(days_needed) * delay / 60

    log.info("[%s] %s — %d/%d days needed (est. %.0f min)",
             slug, name, len(days_needed), total_days, est_minutes)

    if dry_run or not days_needed:
        return {"slug": slug, "name": name, "fetched": 0, "skipped": 0, "existing": len(existing)}

    total_records = 0
    skipped = 0

    for i, day in enumerate(days_needed):
        records, delay = fetch_day(day, service_code, delay)

        if records:
            save_day(s3, prefix, day, records)
            if not verify_day(s3, prefix, day, len(records)):
                s3.delete_object(Bucket=BUCKET, Key=f"{prefix}{day}.json")
                skipped += 1
            else:
                total_records += len(records)
                if len(records) > 0:
                    log.info("  [%s] %s: %d tickets (total: %d, %d/%d)",
                             slug, day, len(records), total_records, i + 1, len(days_needed))
        else:
            skipped += 1

        if i > 0 and i % 100 == 0:
            log.info("  [%s] PROGRESS: %d/%d days, %d records, %d skipped",
                     slug, i, len(days_needed), total_records, skipped)

        time.sleep(delay)

    return {
        "slug": slug,
        "name": name,
        "fetched": total_records,
        "days_attempted": len(days_needed),
        "skipped": skipped,
        "existing": len(existing),
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch Boston Open311 tickets to S3")
    parser.add_argument("--start", default=START_DATE, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (default: yesterday)")
    parser.add_argument("--type", default=None, help="Slug of a single type to fetch (e.g. 'needles', 'other')")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without fetching")
    parser.add_argument("--delay", type=float, default=DELAY, help="Delay between requests in seconds")
    args = parser.parse_args()

    if not BUCKET:
        log.error("BUCKET env var not set. Need S3/Tigris credentials.")
        sys.exit(1)

    s3 = get_s3_client()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)

    # Determine which types to fetch
    if args.type:
        if args.type not in SERVICE_TYPES:
            log.error("Unknown type: %s (available: %s)", args.type, ", ".join(SERVICE_TYPES.keys()))
            sys.exit(1)
        types_to_fetch = {args.type: SERVICE_TYPES[args.type]}
    else:
        types_to_fetch = SERVICE_TYPES

    log.info("Date range: %s to %s", start, end)
    log.info("Types to fetch: %s", ", ".join(types_to_fetch.keys()))

    all_stats = []
    for slug, (service_code, name) in types_to_fetch.items():
        stats = fetch_type(s3, slug, service_code, name, start, end, args.delay, args.dry_run)
        all_stats.append(stats)

    # Write manifest
    manifest = {
        "last_run": datetime.utcnow().isoformat() + "Z",
        "date_range": {"start": str(start), "end": str(end)},
        "types": all_stats,
    }
    key = "open311/manifest.json"
    body = json.dumps(manifest, indent=2, default=str)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"), ContentType="application/json")

    total_fetched = sum(s["fetched"] for s in all_stats)
    total_skipped = sum(s.get("skipped", 0) for s in all_stats)
    log.info("Done. %d records fetched across %d types, %d days skipped.",
             total_fetched, len(all_stats), total_skipped)


if __name__ == "__main__":
    main()
