"""Fetch all 'Other' (General Request) tickets from Boston Open311 API.

Day-by-day batching to work around the 100-result-per-page cap.
Stores each day as a JSON object in S3 so you can stop and resume.

On first run, backfills from START_DATE to today.
On subsequent runs, only fetches days not already in S3.

API constraints (from https://boston2-production.spotmobile.net/open311/docs):
  - 10 requests per minute (unauthenticated)
  - 100 results per page max
  - 429 response includes Retry-After header
  - 90-day max date range (we use single days, so N/A)

Usage (local testing with env vars):
    BUCKET=... ACCESS_KEY_ID=... SECRET_ACCESS_KEY=... ENDPOINT=... python fetch.py

    python fetch.py --start 2025-01-01   # fetch from a specific date
    python fetch.py --dry-run            # show what would be fetched
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

S3_PREFIX = "open311/raw/"  # all raw day files go under this prefix

# --- Open311 API ---

OPEN311_BASE = "https://boston2-production.spotmobile.net/open311/v2"
SERVICE_CODE = "Mayor's 24 Hour Hotline:General Request:General Request"
START_DATE = "2023-01-01"
UA = "BostonHazardResearch/1.0 (public-health-research)"

# --- Rate limiting (API allows 10 req/min = 1 every 6s) ---

DELAY = 7.0          # stay safely under 10 req/min
MAX_DELAY = 120       # max backoff on repeated 429s
MAX_RETRIES = 5       # retries per request before skipping a day

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


def list_existing_days(s3) -> set[str]:
    """List day files already in S3 to know what to skip."""
    existing = set()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=S3_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                day_str = key.removeprefix(S3_PREFIX).removesuffix(".json")
                if len(day_str) == 10:  # YYYY-MM-DD
                    existing.add(day_str)
    except Exception as e:
        log.warning("Could not list existing S3 keys: %s", e)
    return existing


def save_day(s3, day: date, records: list[dict]) -> None:
    """Write a day's records to S3 with record count in metadata for verification."""
    key = f"{S3_PREFIX}{day}.json"
    body = json.dumps(records, separators=(",", ":"))
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={"record-count": str(len(records))},
    )


def update_manifest(s3, stats: dict) -> None:
    """Write a summary manifest so the pipeline knows what's available."""
    key = "open311/manifest.json"
    body = json.dumps(stats, indent=2, default=str)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )


def _do_request(url: str) -> tuple[list[dict] | None, int | None]:
    """Make a single HTTP request. Returns (data, retry_after_seconds).

    On success: (data, None)
    On 429: (None, retry_seconds from Retry-After header or default 60)
    On other error: raises
    """
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


def fetch_day(day: date, delay: float) -> tuple[list[dict], float]:
    """Fetch all 'Other' tickets for a single day with pagination and rate limit handling.

    Returns (records, current_delay). If a day is skipped due to rate limits,
    returns empty list — the day won't be saved to S3 so it'll be retried next run.
    """
    all_records = []
    page = 1

    while True:
        params = urllib.parse.urlencode({
            "start_date": f"{day}T00:00:00Z",
            "end_date": f"{day}T23:59:59Z",
            "service_code": SERVICE_CODE,
            "per_page": 100,  # API max is 100
            "page": page,
        })
        url = f"{OPEN311_BASE}/requests.json?{params}"

        # Try the request with retries on 429
        data = None
        for attempt in range(MAX_RETRIES):
            try:
                data, retry_after = _do_request(url)
            except Exception as e:
                log.error("  ERROR %s page %d: %s", day, page, e)
                return all_records, delay

            if data is not None:
                break  # success

            # Rate limited — use Retry-After header
            wait = min(retry_after or 60, MAX_DELAY)
            log.info("  RATE LIMITED on %s page %d (attempt %d/%d), Retry-After: %ds",
                     day, page, attempt + 1, MAX_RETRIES, wait)
            time.sleep(wait)
        else:
            # All retries exhausted
            log.warning("  GIVING UP on %s after %d retries (will retry next run)", day, MAX_RETRIES)
            return [], delay  # empty = won't be saved to S3

        if not data:
            break

        all_records.extend(data)

        # Paginate if we got a full page (exactly 100 = there may be more)
        if len(data) >= 100:
            page += 1
            time.sleep(delay)
        else:
            break

    return all_records, delay


def verify_day(s3, day: date, expected_count: int) -> bool:
    """Verify a saved day file has the right record count."""
    key = f"{S3_PREFIX}{day}.json"
    try:
        resp = s3.head_object(Bucket=BUCKET, Key=key)
        stored_count = resp.get("Metadata", {}).get("record-count")
        if stored_count and int(stored_count) != expected_count:
            log.warning("  MISMATCH %s: expected %d, S3 metadata says %s", day, expected_count, stored_count)
            return False
        return True
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="Fetch Open311 'Other' tickets to S3")
    parser.add_argument("--start", default=START_DATE, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (default: yesterday)")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without fetching")
    parser.add_argument("--delay", type=float, default=DELAY, help="Delay between requests in seconds")
    args = parser.parse_args()

    if not BUCKET:
        log.error("BUCKET env var not set. Need S3/Tigris credentials.")
        sys.exit(1)

    s3 = get_s3_client()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)

    # Check what's already in S3
    log.info("Checking S3 for existing data...")
    existing = list_existing_days(s3)
    log.info("Found %d days already in S3", len(existing))

    # Build list of days to fetch (newest first — recent data is most valuable)
    days_needed = []
    current = start
    while current <= end:
        if str(current) not in existing:
            days_needed.append(current)
        current += timedelta(days=1)
    days_needed.reverse()

    total_days = (end - start).days + 1
    est_minutes = len(days_needed) * args.delay / 60
    log.info("Date range: %s to %s (%d days)", start, end, total_days)
    log.info("Already in S3: %d days", len(existing))
    log.info("Need to fetch: %d days (est. %.0f min at %.0fs/req)", len(days_needed), est_minutes, args.delay)

    if args.dry_run or not days_needed:
        if not days_needed:
            log.info("Nothing to fetch — all caught up!")
        return

    total_records = 0
    skipped = 0
    delay = args.delay

    for i, day in enumerate(days_needed):
        records, delay = fetch_day(day, delay)

        if records:
            save_day(s3, day, records)
            if not verify_day(s3, day, len(records)):
                log.warning("  Verification failed for %s, will retry next run", day)
                # Delete the bad file so next run retries
                s3.delete_object(Bucket=BUCKET, Key=f"{S3_PREFIX}{day}.json")
                skipped += 1
            else:
                total_records += len(records)
                log.info("  %s: %d tickets (total: %d, %d/%d done)",
                         day, len(records), total_records, i + 1, len(days_needed))
        else:
            skipped += 1

        # Progress every 100 days
        if i > 0 and i % 100 == 0:
            log.info("  PROGRESS: %d/%d days done, %d records, %d skipped",
                     i, len(days_needed), total_records, skipped)

        time.sleep(delay)

    # Write manifest
    all_days_in_s3 = list_existing_days(s3)
    manifest = {
        "last_run": datetime.utcnow().isoformat() + "Z",
        "total_days_in_s3": len(all_days_in_s3),
        "date_range": {"start": str(start), "end": str(end)},
        "this_run": {
            "records_fetched": total_records,
            "days_attempted": len(days_needed),
            "days_skipped": skipped,
        },
    }
    update_manifest(s3, manifest)

    log.info("Done. %d records fetched, %d days skipped (will retry next run).", total_records, skipped)
    if skipped:
        log.info("Skipped days will be retried on the next run automatically.")


if __name__ == "__main__":
    main()
