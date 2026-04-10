"""Stats computation for the dashboard."""

import re
from collections import Counter, defaultdict
from datetime import datetime

from pipeline.models import (
    CleanedRecord,
    DashboardStats,
    MarkerData,
    NeighborhoodStat,
    ZipStat,
)


def slugify(name: str) -> str:
    """Create a URL-safe slug from a neighborhood name.

    Must match the backend slugify logic exactly.
    """
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _bin_records(recs: list[CleanedRecord], bin_size: float = 0.0008) -> list[list[float]]:
    """Bin records into spatial grid cells for heatmap display."""
    grid: dict[tuple[float, float], int] = defaultdict(int)
    for r in recs:
        key = (round(r.lat / bin_size) * bin_size, round(r.lng / bin_size) * bin_size)
        grid[key] += 1
    return [[round(la, 6), round(lo, 6), float(c)] for (la, lo), c in grid.items()]


def compute_stats(records: list[CleanedRecord]) -> DashboardStats:
    """Compute all the stats the dashboard needs."""
    years = sorted({r.year for r in records})

    # Pre-bin heat for every filter combo
    heat_keys: dict[str, list[list[float]]] = {"all": _bin_records(records)}
    for y in years:
        yr_recs = [r for r in records if r.year == y]
        heat_keys[str(y)] = _bin_records(yr_recs)
        for m in range(1, 13):
            mo_recs = [r for r in yr_recs if r.month == m]
            if mo_recs:
                heat_keys[f"{y}-{m:02d}"] = _bin_records(mo_recs)
    for m in range(1, 13):
        mo_recs = [r for r in records if r.month == m]
        if mo_recs:
            heat_keys[f"all-{m:02d}"] = _bin_records(mo_recs)

    # Build district index maps for compact encoding (0 = unknown, 1+ = index)
    council_values = sorted({r.council_district for r in records if r.council_district})
    police_values = sorted({r.police_district for r in records if r.police_district})
    rep_values = sorted({r.state_rep_district for r in records if r.state_rep_district})
    senate_values = sorted({r.state_senate_district for r in records if r.state_senate_district})
    council_idx = {v: i + 1 for i, v in enumerate(council_values)}
    police_idx = {v: i + 1 for i, v in enumerate(police_values)}
    rep_idx = {v: i + 1 for i, v in enumerate(rep_values)}
    senate_idx = {v: i + 1 for i, v in enumerate(senate_values)}

    # Compact point array: [lat, lng, year, month, source_flag,
    #   council_idx, police_idx, rep_idx, senate_idx]
    points: list[list[float | int]] = [
        [
            r.lat,
            r.lng,
            r.year,
            r.month,
            1 if r.source == "detected" else 0,
            council_idx.get(r.council_district, 0),
            police_idx.get(r.police_district, 0),
            rep_idx.get(r.state_rep_district, 0),
            senate_idx.get(r.state_senate_district, 0),
        ]
        for r in records
    ]

    # Neighborhood breakdown
    by_hood: dict[str, list[CleanedRecord]] = defaultdict(list)
    for r in records:
        by_hood[r.hood or "Unknown"].append(r)

    hood_stats: list[NeighborhoodStat] = []
    for name, recs in sorted(by_hood.items(), key=lambda x: -len(x[1])):
        streets = Counter(r.street for r in recs if r.street)
        resp = [r.resp_hrs for r in recs if r.resp_hrs is not None]
        hood_stats.append(
            NeighborhoodStat(
                name=name,
                slug=slugify(name),
                count=len(recs),
                pct=round(len(recs) / len(records) * 100, 1),
                top_street=streets.most_common(1)[0][0] if streets else "\u2014",
                avg_resp=round(sum(resp) / max(len(resp), 1), 1),
            )
        )

    # Hourly distribution
    hourly_counter = Counter(r.hour for r in records)
    hourly_data = [hourly_counter.get(h, 0) for h in range(24)]

    # Hourly counts by year
    year_hourly: dict[str, list[int]] = {}
    for y in years:
        yr_hourly = Counter(r.hour for r in records if r.year == y)
        year_hourly[str(y)] = [yr_hourly.get(h, 0) for h in range(24)]

    # Monthly counts by year
    year_monthly = {
        str(y): [sum(1 for r in records if r.year == y and r.month == m) for m in range(1, 13)] for y in years
    }

    # Top zip codes
    zip_counts = Counter(r.zipcode for r in records if r.zipcode)
    zip_stats = [ZipStat(zip=z, count=c) for z, c in zip_counts.most_common(10)]

    # Individual markers (cap at 3000 most recent)
    recent = sorted(records, key=lambda r: r.dt, reverse=True)[:3000]
    markers = [
        MarkerData(
            lat=r.lat,
            lng=r.lng,
            dt=r.dt[:10],
            hood=r.hood,
            street=r.street,
            zip=r.zipcode,
            council_district=r.council_district,
            police_district=r.police_district,
            state_rep_district=r.state_rep_district,
            state_senate_district=r.state_senate_district,
            source=r.source,
        )
        for r in recent
    ]

    dow = Counter(r.dow for r in records)

    return DashboardStats(
        total=len(records),
        years=years,
        heat_keys=heat_keys,
        points=points,
        hoods=hood_stats[:15],
        hourly=hourly_data,
        year_hourly=year_hourly,
        year_monthly=year_monthly,
        zip_stats=zip_stats,
        markers=markers,
        council_districts=council_values,
        police_districts=police_values,
        state_rep_districts=rep_values,
        state_senate_districts=senate_values,
        generated=datetime.now().strftime("%B %d, %Y at %I:%M %p"),
        peak_hood=hood_stats[0].name if hood_stats else "\u2014",
        peak_hour=max(range(24), key=lambda h: hourly_counter.get(h, 0)),
        peak_dow=dow.most_common(1)[0][0] if dow else "\u2014",
        avg_monthly=round(len(records) / max(len({f"{r.year}-{r.month}" for r in records}), 1), 1),
    )
