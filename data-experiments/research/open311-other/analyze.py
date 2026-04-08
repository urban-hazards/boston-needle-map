"""Analyze fetched Open311 'Other' tickets.

Reads the cached JSON files from raw/ and produces summary stats.
Run fetch.py first to populate raw/.

Usage:
    python analyze.py
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

RAW_DIR = Path(__file__).parent / "raw"


def load_all() -> list[dict]:
    """Load all cached daily JSON files."""
    records = []
    for f in sorted(RAW_DIR.glob("*.json")):
        with open(f) as fh:
            day_records = json.load(fh)
            records.extend(day_records)
    return records


def extract_bracket_tag(description: str) -> str | None:
    """Extract the [Type] bracket tag from a reclassified description."""
    match = re.search(r'Type:\s*\[([^\]]+)\]', description)
    return match.group(1) if match else None


def extract_referred_to(description: str) -> str | None:
    """Extract the Referred To: [X] tag."""
    match = re.search(r'Referred To:\s*\[([^\]]+)\]', description)
    return match.group(1) if match else None


def main():
    records = load_all()
    if not records:
        print("No records found. Run fetch.py first.")
        return

    print(f"Total 'Other' tickets: {len(records)}\n")

    # Status breakdown
    statuses = Counter(r.get("status", "?") for r in records)
    print(f"Status: {dict(statuses)}\n")

    # Reclassification tags
    tags = Counter()
    referred = Counter()
    no_tag = []
    has_tag = []

    for r in records:
        desc = r.get("description", "")
        tag = extract_bracket_tag(desc)
        ref = extract_referred_to(desc)
        if tag:
            tags[tag] += 1
            has_tag.append(r)
        else:
            no_tag.append(r)
        if ref:
            referred[ref] += 1

    print(f"Reclassified (has [Type] tag): {len(has_tag)}")
    print(f"Not reclassified: {len(no_tag)}\n")

    print("=== Reclassification tags ===\n")
    for tag, count in tags.most_common(30):
        print(f"  {count:>5}  [{tag}]")

    print(f"\n=== Referred To tags ===\n")
    for ref, count in referred.most_common(20):
        print(f"  {count:>5}  [{ref}]")

    # Monthly volume
    print(f"\n=== Monthly volume ===\n")
    monthly = Counter()
    for r in records:
        dt = r.get("requested_datetime", "")[:7]
        if dt:
            monthly[dt] += 1
    for month in sorted(monthly.keys()):
        bar = "#" * (monthly[month] // 5)
        print(f"  {month}: {monthly[month]:>5} {bar}")

    # Human waste specifically
    waste_records = [r for r in records if "Human Waste" in (r.get("description") or "")
                     or "HUMAN WASTE" in (r.get("description") or "")]
    print(f"\n=== HUMAN WASTE tickets in 'Other' ===\n")
    print(f"Total: {len(waste_records)}\n")

    waste_monthly = Counter()
    for r in waste_records:
        dt = r.get("requested_datetime", "")[:7]
        if dt:
            waste_monthly[dt] += 1

    for month in sorted(waste_monthly.keys()):
        print(f"  {month}: {waste_monthly[month]}")

    print(f"\n=== Sample human waste descriptions ===\n")
    for r in waste_records[:20]:
        desc = (r.get("description") or "")[:150]
        status = r.get("status", "?")
        case_id = r.get("service_request_id", "?")
        addr = r.get("address", "?")
        print(f"  [{status}] #{case_id}")
        print(f"    {addr}")
        print(f"    {desc}")
        print()

    # Encampment/homeless in Other
    camp_records = [r for r in records
                    if any(kw in (r.get("description") or "").lower()
                           for kw in ["homeless", "encampment", "tent", "sleeping", "camp"])]
    print(f"=== ENCAMPMENT/HOMELESS tickets in 'Other' ===\n")
    print(f"Total: {len(camp_records)}\n")
    for r in camp_records[:10]:
        desc = (r.get("description") or "")[:150]
        print(f"  #{r.get('service_request_id','?')} [{r.get('status','?')}]")
        print(f"    {desc}")
        print()

    # Has photo?
    with_photo = sum(1 for r in records if r.get("media_url"))
    print(f"=== Photos ===")
    print(f"  Tickets with photos: {with_photo} ({100*with_photo/len(records):.0f}%)")

    # Save summary
    summary = {
        "total": len(records),
        "statuses": dict(statuses),
        "reclassified": len(has_tag),
        "not_reclassified": len(no_tag),
        "human_waste_total": len(waste_records),
        "encampment_total": len(camp_records),
        "with_photo": with_photo,
        "tags": dict(tags.most_common()),
        "monthly": dict(sorted(monthly.items())),
        "waste_monthly": dict(sorted(waste_monthly.items())),
    }
    out = Path(__file__).parent / "summary.json"
    with open(out, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary saved to {out}")


if __name__ == "__main__":
    main()
