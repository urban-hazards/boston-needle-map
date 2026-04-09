"""Fetch all 'Other' (General Request) tickets from Boston Open311 API.

Day-by-day batching to work around the 100-result-per-query cap.
Caches each day as a JSON file so you can stop and resume.

Usage:
    python fetch.py                    # fetch all days 2023-01-01 to today
    python fetch.py --start 2025-01-01 # fetch from a specific date
    python fetch.py --dry-run          # show what would be fetched
"""

import argparse
import json
import time
import urllib.request
import urllib.parse
from datetime import date, datetime, timedelta
from pathlib import Path

OPEN311_BASE = "https://boston2-production.spotmobile.net/open311/v2"
SERVICE_CODE = "Mayor's 24 Hour Hotline:General Request:General Request"
RAW_DIR = Path(__file__).parent / "raw"
UA = "BostonHazardResearch/1.0 (public-health-research)"
DELAY = 0.5  # seconds between requests, be polite
MIN_DELAY = 0.3
MAX_DELAY = 60
BACKOFF_FACTOR = 2.0
COOLDOWN_FACTOR = 0.9  # slowly speed back up after success


def fetch_day(day: date, current_delay: float) -> tuple[list[dict], float]:
    """Fetch all 'Other' tickets for a single day. Paginates if needed.

    Returns (records, updated_delay) — delay adapts to rate limiting.
    """
    all_records = []
    page = 1
    delay = current_delay

    while True:
        start = f"{day}T00:00:00Z"
        end = f"{day}T23:59:59Z"
        params = urllib.parse.urlencode({
            "start_date": start,
            "end_date": end,
            "service_code": SERVICE_CODE,
            "page_size": 500,
            "page": page,
        })
        url = f"{OPEN311_BASE}/requests.json?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": UA})

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # Rate limited — exponential backoff with retries
                for attempt in range(3):
                    delay = min(delay * BACKOFF_FACTOR, MAX_DELAY)
                    wait = delay * (attempt + 1)
                    print(f"  RATE LIMITED on {day} (attempt {attempt+1}), "
                          f"waiting {wait:.0f}s, new delay={delay:.1f}s...")
                    time.sleep(wait)
                    try:
                        with urllib.request.urlopen(req, timeout=30) as resp:
                            data = json.loads(resp.read())
                        break  # success
                    except urllib.error.HTTPError as retry_e:
                        if retry_e.code != 429:
                            print(f"  ERROR {day} page {page}: {retry_e}")
                            return all_records or [], delay
                    except Exception:
                        pass
                else:
                    print(f"  STILL LIMITED on {day} after 3 retries, skipping")
                    return [], delay  # empty so we don't cache
            else:
                print(f"  ERROR {day} page {page}: {e}")
                break
        except Exception as e:
            print(f"  ERROR {day} page {page}: {e}")
            break

        if not data:
            break

        # Success — gradually cool down delay
        delay = max(delay * COOLDOWN_FACTOR, MIN_DELAY)

        all_records.extend(data)

        # If we got exactly 100, there might be more (API cap)
        if len(data) >= 100:
            page += 1
            time.sleep(delay)
        else:
            break

    return all_records, delay


def main():
    parser = argparse.ArgumentParser(description="Fetch Open311 'Other' tickets")
    parser.add_argument("--start", default="2023-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without fetching")
    parser.add_argument("--delay", type=float, default=DELAY, help="Delay between requests")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today()
    delay = args.delay

    # Figure out which days we still need (newest first)
    days_needed = []
    current = start
    while current <= end:
        cache_file = RAW_DIR / f"{current}.json"
        if not cache_file.exists():
            days_needed.append(current)
        current += timedelta(days=1)

    days_needed.reverse()  # newest first — recent data is most valuable

    total_days = (end - start).days + 1
    cached = total_days - len(days_needed)

    print(f"Date range: {start} to {end} ({total_days} days)")
    print(f"Already cached: {cached} days")
    print(f"Need to fetch: {len(days_needed)} days (newest first)")
    print(f"Estimated time: {len(days_needed) * (delay + 0.3):.0f}s ({len(days_needed) * (delay + 0.3) / 60:.1f}min)")
    print(f"Delay: {delay:.1f}s (adaptive: backs off on 429, speeds up on success)")
    print()

    if args.dry_run:
        return

    total_records = 0
    current_delay = delay
    for i, day in enumerate(days_needed):
        records, current_delay = fetch_day(day, current_delay)
        if records:  # only cache non-empty results
            cache_file = RAW_DIR / f"{day}.json"
            with open(cache_file, "w") as f:
                json.dump(records, f)

        total_records += len(records)
        if records:
            print(f"  {day}: {len(records)} tickets "
                  f"(total: {total_records}, delay: {current_delay:.1f}s)")

        # Progress every 50 days even if empty
        if i > 0 and i % 50 == 0 and not records:
            print(f"  ... {day} ({i}/{len(days_needed)} days done, "
                  f"delay: {current_delay:.1f}s)")

        time.sleep(current_delay)

    print(f"\nDone. {total_records} tickets fetched across {len(days_needed)} days.")
    print(f"Raw files in: {RAW_DIR}")


if __name__ == "__main__":
    main()
