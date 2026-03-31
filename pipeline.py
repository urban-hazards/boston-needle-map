#!/usr/bin/env python3
"""
Boston 311 Needle Hotspot Pipeline
====================================
Fetches needle-related 311 requests from Boston's open data portal,
processes them, and generates a self-contained static HTML dashboard
with a Leaflet.js heatmap.

Output: docs/index.html (served by GitHub Pages)

Run manually:   python pipeline.py
Run multi-year: python pipeline.py 2023 2024 2025 2026
Automated:      GitHub Actions cron (see .github/workflows/update.yml)
"""

import csv
import io
import json
import math
import os
import sys
import urllib.parse
import urllib.request
import urllib.error
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from string import Template

# ── Config ──────────────────────────────────────────────────────────────────

CKAN_BASE = "https://data.boston.gov/api/3/action"

# Resource IDs for each year's 311 dataset on data.boston.gov
RESOURCE_IDS = {
    2015: "c9509ab4-6f6d-4b97-979a-0cf2a10c922b",
    2016: "b7ea6b1b-3ca4-4c5b-9713-6dc1db52379a",
    2017: "30022137-709d-465e-baae-ca155b51927d",
    2018: "2be28d90-3a90-4af1-a3f6-f28c1e25880a",
    2019: "ea2e4696-4a2d-429c-9807-d02eb92e0222",
    2020: "6ff6a6fd-3141-4440-a880-6f60a37fe789",
    2021: "f53ebccd-bc61-49f9-83db-625f209c95f5",
    2022: "81a7b022-f8fc-4da5-80e4-b160058ca207",
    2023: "e6013a93-1321-4f2a-bf91-8d8a02f1e62f",
    2024: "dff4d804-5031-443a-8409-8344efd0e5c8",
    2025: "9d7c2214-4709-478a-a2e8-fb2020a5bb94",
    2026: "1a0b420d-99f1-4887-9851-990b2a5a6e17",
}

NEEDLE_TYPES = {"Needle Pickup", "Needle Clean-up", "Needle Cleanup"}

BOSTON_BBOX = {
    "lat_min": 42.2279, "lat_max": 42.3969,
    "lon_min": -71.1912, "lon_max": -70.9235,
}

OUTPUT_DIR = Path("docs")
UA = "Boston311NeedlePipeline/2.0 (github-actions; public-health-research)"

# ── Data fetching ───────────────────────────────────────────────────────────

def _api_get(url: str) -> dict | None:
    """GET a CKAN API endpoint, return parsed JSON or None."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as e:
        print(f"  ✗ API error: {e}")
        return None


def fetch_needle_records_sql(resource_id: str) -> list[dict]:
    """Use CKAN datastore_search_sql to pull only needle rows (fast)."""
    type_clauses = " OR ".join(f"\"type\" = '{t}'" for t in NEEDLE_TYPES)
    sql = (
        f'SELECT * FROM "{resource_id}" '
        f'WHERE ({type_clauses}) '
        f'OR LOWER("type") LIKE \'%needle%\''
    )
    url = f"{CKAN_BASE}/datastore_search_sql?sql={urllib.parse.quote(sql)}"
    data = _api_get(url)
    if data and data.get("success"):
        return data["result"]["records"]
    return []


def fetch_needle_records_paged(resource_id: str) -> list[dict]:
    """Fallback: page through datastore_search with a TYPE filter."""
    all_records = []
    for needle_type in NEEDLE_TYPES:
        offset = 0
        limit = 5000
        while True:
            filters = json.dumps({"type": needle_type})
            url = (
                f"{CKAN_BASE}/datastore_search"
                f"?resource_id={resource_id}"
                f"&filters={urllib.parse.quote(filters)}"
                f"&limit={limit}&offset={offset}"
            )
            data = _api_get(url)
            if not data or not data.get("success"):
                break
            records = data["result"]["records"]
            all_records.extend(records)
            if len(records) < limit:
                break
            offset += limit
    return all_records


def fetch_year(year: int) -> list[dict]:
    """Fetch needle records for a given year."""
    rid = RESOURCE_IDS.get(year)
    if not rid:
        print(f"  ⚠ No resource ID for {year}, skipping")
        return []

    print(f"  → {year}: trying SQL API...", end=" ", flush=True)
    records = fetch_needle_records_sql(rid)
    if records:
        print(f"got {len(records)} records")
        return records

    print(f"retrying with paged search...", end=" ", flush=True)
    records = fetch_needle_records_paged(rid)
    print(f"got {len(records)} records")
    return records


# ── Cleaning ────────────────────────────────────────────────────────────────

def clean(row: dict) -> dict | None:
    """Normalize a raw API record. Returns None if invalid."""
    try:
        lat = float(row.get("latitude") or row.get("LATITUDE") or 0)
        lon = float(row.get("longitude") or row.get("LONGITUDE") or 0)
    except (ValueError, TypeError):
        return None

    if not (BOSTON_BBOX["lat_min"] <= lat <= BOSTON_BBOX["lat_max"]):
        return None
    if not (BOSTON_BBOX["lon_min"] <= lon <= BOSTON_BBOX["lon_max"]):
        return None

    dt_str = row.get("open_dt") or row.get("OPEN_DT") or ""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(dt_str[:19], fmt)
            break
        except ValueError:
            continue
    else:
        return None

    closed_str = row.get("closed_dt") or row.get("CLOSED_DT") or ""
    closed = None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            closed = datetime.strptime(closed_str[:19], fmt)
            break
        except ValueError:
            continue

    hood = (
        row.get("neighborhood")
        or row.get("NEIGHBORHOOD")
        or row.get("neighborhood_services_district")
        or ""
    ).strip()

    street = (
        row.get("location_street_name")
        or row.get("LOCATION_STREET_NAME")
        or ""
    ).strip()

    return {
        "lat": lat,
        "lng": lon,
        "dt": dt.isoformat(),
        "year": dt.year,
        "month": dt.month,
        "hour": dt.hour,
        "dow": dt.strftime("%A"),
        "hood": hood,
        "street": street,
        "resp_hrs": (
            round((closed - dt).total_seconds() / 3600, 1) if closed else None
        ),
    }


# ── Analytics ───────────────────────────────────────────────────────────────

def compute_stats(records: list[dict]) -> dict:
    """Compute all the stats the HTML template needs."""

    # Monthly trend
    monthly = Counter(f"{r['year']}-{r['month']:02d}" for r in records)
    monthly_sorted = sorted(monthly.items())

    # Neighborhood breakdown
    by_hood = defaultdict(list)
    for r in records:
        by_hood[r["hood"] or "Unknown"].append(r)

    hood_stats = []
    for name, recs in sorted(by_hood.items(), key=lambda x: -len(x[1])):
        streets = Counter(r["street"] for r in recs if r["street"])
        resp = [r["resp_hrs"] for r in recs if r["resp_hrs"] is not None]
        hood_stats.append({
            "name": name,
            "count": len(recs),
            "pct": round(len(recs) / len(records) * 100, 1),
            "top_street": streets.most_common(1)[0][0] if streets else "—",
            "avg_resp": round(sum(resp) / max(len(resp), 1), 1),
        })

    # Hourly distribution
    hourly = Counter(r["hour"] for r in records)
    hourly_data = [hourly.get(h, 0) for h in range(24)]

    # Day of week
    dow = Counter(r["dow"] for r in records)

    def bin_records(recs):
        """Cluster lat/lng points into ~90m grid cells for heatmap."""
        grid = defaultdict(int)
        bin_size = 0.0008
        for r in recs:
            key = (round(r["lat"] / bin_size) * bin_size,
                   round(r["lng"] / bin_size) * bin_size)
            grid[key] += 1
        return [[lat, lng, count] for (lat, lng), count in grid.items()]

    heat_points = bin_records(records)

    # Per-year heat data for the year filter
    years = sorted(set(r["year"] for r in records))
    heat_by_year = {
        str(y): bin_records([r for r in records if r["year"] == y])
        for y in years
    }

    # Individual points for the marker layer (cap at 3000 most recent)
    recent = sorted(records, key=lambda r: r["dt"], reverse=True)[:3000]
    markers = [
        {"lat": r["lat"], "lng": r["lng"], "dt": r["dt"][:10],
         "hood": r["hood"], "street": r["street"]}
        for r in recent
    ]

    return {
        "total": len(records),
        "years": years,
        "monthly": monthly_sorted,
        "hoods": hood_stats[:15],
        "hourly": hourly_data,
        "dow": dict(dow.most_common()),
        "heat_points": heat_points,
        "heat_by_year": heat_by_year,
        "markers": markers,
        "generated": datetime.now().strftime("%B %d, %Y at %I:%M %p"),
        "peak_hood": hood_stats[0]["name"] if hood_stats else "—",
        "peak_hour": max(range(24), key=lambda h: hourly.get(h, 0)),
        "peak_dow": dow.most_common(1)[0][0] if dow else "—",
        "avg_monthly": round(len(records) / max(len(monthly), 1), 1),
    }


# ── HTML Generation ─────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Boston 311 — Needle Hotspot Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #06060f; --bg2: #0c0c1a; --bg3: #121220;
    --border: #1a1a30; --border2: #242440;
    --t1: #f0f0f8; --t2: #9090b8; --t3: #505070;
    --red: #ef4444; --orange: #f97316; --amber: #f59e0b;
    --green: #22c55e; --sidebar: 270px;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  html, body { height:100%; overflow:hidden; }
  body { background:var(--bg); color:var(--t1);
    font-family:'DM Sans',system-ui,sans-serif; display:flex; }

  /* ── Sidebar ── */
  #sidebar {
    width:var(--sidebar); flex-shrink:0;
    background:rgba(6,6,15,0.96);
    border-right:1px solid var(--border);
    display:flex; flex-direction:column;
    overflow-y:auto; z-index:500;
    backdrop-filter:blur(4px);
  }
  .sb-title {
    padding:18px 16px 12px;
    border-bottom:1px solid var(--border);
  }
  .sb-title-top { display:flex; align-items:center; gap:8px; margin-bottom:4px; }
  .dot { width:8px;height:8px;border-radius:50%;background:var(--red);
    box-shadow:0 0 8px 3px rgba(239,68,68,.5); animation:pulse 2s infinite; flex-shrink:0; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
  .sb-h1 { font-family:'DM Mono',monospace; font-size:13px; font-weight:500;
    letter-spacing:.06em; text-transform:uppercase;
    background:linear-gradient(90deg,var(--orange),var(--amber));
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
  .sb-sub { font-family:'DM Mono',monospace; font-size:9px; color:var(--t3);
    letter-spacing:.03em; line-height:1.5; }

  /* KPIs */
  .kpis { display:grid; grid-template-columns:1fr 1fr; gap:1px;
    background:var(--border); border-bottom:1px solid var(--border); }
  .kpi { padding:10px 12px; background:var(--bg2); }
  .kpi-label { font-family:'DM Mono',monospace; font-size:8px; color:var(--t3);
    letter-spacing:.1em; text-transform:uppercase; margin-bottom:3px; }
  .kpi-val { font-family:'DM Mono',monospace; font-size:18px; font-weight:500; }
  .kpi-val.red{color:var(--red);} .kpi-val.orange{color:var(--orange);}
  .kpi-val.amber{color:var(--amber);} .kpi-val.green{color:var(--green);}

  /* Section headers */
  .sb-section { padding:10px 16px 6px;
    font-family:'DM Mono',monospace; font-size:9px; color:var(--t3);
    letter-spacing:.1em; text-transform:uppercase;
    border-bottom:1px solid var(--border); }

  /* Year filter */
  .year-btns { display:flex; flex-wrap:wrap; gap:4px; padding:10px 12px;
    border-bottom:1px solid var(--border); }
  .yr-btn { font-family:'DM Mono',monospace; font-size:11px;
    padding:5px 10px; border-radius:4px; cursor:pointer;
    border:1px solid var(--border2); background:transparent;
    color:var(--t3); transition:all .15s; }
  .yr-btn:hover { border-color:var(--orange); color:var(--t2); }
  .yr-btn.active { background:rgba(249,115,22,.15);
    border-color:var(--orange); color:var(--orange); }

  /* Neighborhood list */
  .hood-list { flex:1; overflow-y:auto; }
  .hood-row { display:flex; align-items:center; gap:8px;
    padding:7px 12px; border-bottom:1px solid var(--bg3);
    cursor:pointer; transition:background .1s; }
  .hood-row:hover { background:rgba(249,115,22,.04); }
  .hood-name { flex:1; font-size:12px; white-space:nowrap;
    overflow:hidden; text-overflow:ellipsis; }
  .hood-count { font-family:'DM Mono',monospace; font-size:11px;
    color:var(--orange); flex-shrink:0; }
  .hood-bar-wrap { width:50px; flex-shrink:0; }
  .hood-bar { height:4px; border-radius:2px; background:var(--orange); }

  /* Legend */
  .legend { padding:12px; border-top:1px solid var(--border); flex-shrink:0; }
  .legend-title { font-family:'DM Mono',monospace; font-size:9px; color:var(--t3);
    letter-spacing:.1em; text-transform:uppercase; margin-bottom:8px; }
  .legend-gradient {
    height:10px; border-radius:5px; margin-bottom:5px;
    background:linear-gradient(90deg,
      rgba(255,255,178,0.6) 0%,
      #fecc5c 30%, #fd8d3c 55%, #f03b20 78%, #bd0026 100%);
  }
  .legend-labels { display:flex; justify-content:space-between;
    font-family:'DM Mono',monospace; font-size:9px; color:var(--t3); }

  /* Attribution */
  .sb-footer { padding:8px 12px; font-family:'DM Mono',monospace;
    font-size:8px; color:var(--t3); border-top:1px solid var(--border);
    line-height:1.6; flex-shrink:0; }
  .sb-footer a { color:var(--t3); text-decoration:underline; }

  /* ── Map ── */
  #map-wrap { flex:1; position:relative; }
  #map { position:absolute; inset:0; }

  /* Leaflet overrides */
  .leaflet-container { background:var(--bg) !important; }
  .leaflet-control-attribution {
    background:rgba(6,6,15,0.75) !important;
    color:var(--t3) !important; font-size:9px !important;
  }
  .leaflet-control-attribution a { color:var(--t3) !important; }
  .info-popup { font-family:'DM Mono',monospace; font-size:11px;
    line-height:1.6; color:#e0e0f0; }
  .info-popup b { color:var(--orange); display:block; margin-bottom:2px; }
  .leaflet-popup-content-wrapper {
    background:rgba(12,12,26,0.95) !important;
    border:1px solid var(--border2) !important;
    border-radius:6px !important; box-shadow:0 4px 20px rgba(0,0,0,.5) !important;
  }
  .leaflet-popup-tip { background:rgba(12,12,26,0.95) !important; }

  /* Mobile */
  @media(max-width:640px) {
    body { flex-direction:column; overflow:auto; }
    #sidebar { width:100%; height:auto; overflow:visible; border-right:none;
      border-bottom:1px solid var(--border); }
    #map-wrap { flex:none; height:65vh; }
    #map { position:relative; height:100%; }
    .hood-list { max-height:200px; }
  }
</style>
</head>
<body>

<!-- ── Sidebar ── -->
<div id="sidebar">
  <div class="sb-title">
    <div class="sb-title-top">
      <div class="dot"></div>
      <div class="sb-h1">Boston 311 · Needles</div>
    </div>
    <div class="sb-sub">
      Needle Pickup &amp; Clean-up requests<br>
      Source: <a href="https://data.boston.gov/dataset/311-service-requests"
        target="_blank" style="color:var(--t3);text-decoration:underline">data.boston.gov</a>
      &nbsp;·&nbsp; Updated: $GENERATED
    </div>
  </div>

  <div class="kpis">
    <div class="kpi">
      <div class="kpi-label">Total</div>
      <div class="kpi-val red">$TOTAL</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Top area</div>
      <div class="kpi-val orange" style="font-size:13px;line-height:1.3">$PEAK_HOOD</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Peak hour</div>
      <div class="kpi-val amber">${PEAK_HOUR}:00</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Avg/month</div>
      <div class="kpi-val green">$AVG_MONTHLY</div>
    </div>
  </div>

  <div class="sb-section">Year</div>
  <div class="year-btns" id="year-btns">
    <button class="yr-btn active" data-year="all" onclick="setYear(this,'all')">All</button>
    <!-- year buttons injected by JS -->
  </div>

  <div class="sb-section">Neighborhoods</div>
  <div class="hood-list" id="hood-list"></div>

  <div class="legend">
    <div class="legend-title">Request density</div>
    <div class="legend-gradient"></div>
    <div class="legend-labels"><span>Low</span><span>High</span></div>
  </div>

  <div class="sb-footer">
    Zoom in past level&nbsp;15 for individual report markers.
    Auto-updates monthly via GitHub&nbsp;Actions.
    Years: $YEARS &nbsp;·&nbsp;
    <a href="https://github.com/coffeethencode/boston-needle-map" target="_blank">Source</a>
  </div>
</div>

<!-- ── Map ── -->
<div id="map-wrap">
  <div id="map"></div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
<script>
// ── Embedded data ─────────────────────────────────────────────────────
const HEAT_ALL   = $HEAT_JSON;
const HEAT_YEARS = $HEAT_BY_YEAR_JSON;
const MARKERS    = $MARKERS_JSON;
const MONTHLY    = $MONTHLY_JSON;
const HOODS      = $HOODS_JSON;
const HOURLY     = $HOURLY_JSON;

// ── Map ───────────────────────────────────────────────────────────────
const map = L.map('map', {
  center: [42.332, -71.078],
  zoom: 13,
  zoomControl: true,
  attributionControl: true,
});

L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  attribution: '&copy; <a href="https://carto.com/">CARTO</a> &middot; <a href="https://www.openstreetmap.org/copyright">OSM</a>',
  subdomains: 'abcd',
  maxZoom: 19,
}).addTo(map);

// Heat gradient — YlOrRd (matches Tableau sequential heat palette)
const GRADIENT = {
  0.00: 'rgba(0,0,0,0)',
  0.15: 'rgba(255,255,178,0.55)',
  0.35: '#fecc5c',
  0.55: '#fd8d3c',
  0.75: '#f03b20',
  1.00: '#bd0026',
};

function makeHeat(pts) {
  const counts = pts.map(p => p[2]);
  // Use 95th-percentile as max so one outlier doesn't wash out the palette
  const sorted = [...counts].sort((a,b) => a-b);
  const p95 = sorted[Math.floor(sorted.length * 0.95)] || 1;
  return L.heatLayer(pts, {
    radius: 30,
    blur: 22,
    maxZoom: 15,
    max: p95,
    minOpacity: 0.35,
    gradient: GRADIENT,
  });
}

let heatLayer = makeHeat(HEAT_ALL);
heatLayer.addTo(map);

// ── Year filter ───────────────────────────────────────────────────────
const yearBtns = document.getElementById('year-btns');
Object.keys(HEAT_YEARS).sort().forEach(yr => {
  const btn = document.createElement('button');
  btn.className = 'yr-btn';
  btn.dataset.year = yr;
  btn.textContent = yr;
  btn.onclick = () => setYear(btn, yr);
  yearBtns.appendChild(btn);
});

function setYear(btn, yr) {
  document.querySelectorAll('.yr-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  map.removeLayer(heatLayer);
  const pts = yr === 'all' ? HEAT_ALL : (HEAT_YEARS[yr] || []);
  heatLayer = makeHeat(pts);
  heatLayer.addTo(map);
}

// ── Marker layer (zoom 15+) ───────────────────────────────────────────
const markerGroup = L.layerGroup();
MARKERS.forEach(m => {
  L.circleMarker([m.lat, m.lng], {
    radius: 5, fillColor: '#f97316', fillOpacity: 0.8,
    color: '#fff', weight: 0.5, opacity: 0.5,
  }).bindPopup(
    `<div class="info-popup"><b>${m.hood || 'Unknown area'}</b>${m.street ? m.street + '<br>' : ''}${m.dt}</div>`
  ).addTo(markerGroup);
});

map.on('zoomend', () => {
  if (map.getZoom() >= 15) map.addLayer(markerGroup);
  else map.removeLayer(markerGroup);
});

// ── Neighborhood list ─────────────────────────────────────────────────
(function() {
  const el = document.getElementById('hood-list');
  const maxCount = HOODS.length ? HOODS[0].count : 1;
  HOODS.forEach(h => {
    const w = Math.max(3, Math.round((h.count / maxCount) * 50));
    const div = document.createElement('div');
    div.className = 'hood-row';
    div.title = `${h.top_street} · avg response ${h.avg_resp}h`;
    div.innerHTML = `
      <div class="hood-name">${h.name}</div>
      <div class="hood-bar-wrap"><div class="hood-bar" style="width:${w}px"></div></div>
      <div class="hood-count">${h.count.toLocaleString()}</div>
    `;
    el.appendChild(div);
  });
})();
</script>
</body>
</html>"""


def generate_html(stats: dict) -> str:
    """Inject computed stats into the HTML template."""
    html = HTML_TEMPLATE
    html = html.replace("$GENERATED", stats["generated"])
    html = html.replace("$TOTAL", f"{stats['total']:,}")
    html = html.replace("$PEAK_HOOD", stats["peak_hood"])
    html = html.replace("${PEAK_HOUR}", str(stats["peak_hour"]))
    html = html.replace("$PEAK_DOW", stats["peak_dow"])
    html = html.replace("$AVG_MONTHLY", str(stats["avg_monthly"]))
    html = html.replace("$YEARS", ", ".join(str(y) for y in stats["years"]))
    html = html.replace("$HEAT_JSON", json.dumps(stats["heat_points"]))
    html = html.replace("$HEAT_BY_YEAR_JSON", json.dumps(stats["heat_by_year"]))
    html = html.replace("$MARKERS_JSON", json.dumps(stats["markers"]))
    html = html.replace("$MONTHLY_JSON", json.dumps(stats["monthly"]))
    html = html.replace("$HOODS_JSON", json.dumps(stats["hoods"]))
    html = html.replace("$HOURLY_JSON", json.dumps(stats["hourly"]))
    return html


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) > 1:
        years = sorted(int(y) for y in sys.argv[1:])
    else:
        # Default: last 3 years + current
        now = datetime.now().year
        years = [y for y in range(now - 2, now + 1) if y in RESOURCE_IDS]

    print(f"╔══════════════════════════════════════════════╗")
    print(f"║  Boston 311 Needle Hotspot Pipeline          ║")
    print(f"║  Years: {', '.join(str(y) for y in years):<37s} ║")
    print(f"╚══════════════════════════════════════════════╝")

    all_records = []
    for year in years:
        raw = fetch_year(year)
        cleaned = [r for r in (clean(row) for row in raw) if r is not None]
        print(f"  ✓ {year}: {len(raw)} raw → {len(cleaned)} valid")
        all_records.extend(cleaned)

    if not all_records:
        print("\n⚠ No records retrieved. Writing placeholder page.")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        (OUTPUT_DIR / "index.html").write_text(
            "<html><body><h1>No data available</h1>"
            "<p>The pipeline could not retrieve data from data.boston.gov. "
            "Check the CKAN API or resource IDs.</p></body></html>"
        )
        return

    print(f"\n  Total valid records: {len(all_records):,}")
    print(f"  Computing stats...")

    stats = compute_stats(all_records)
    html = generate_html(stats)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "index.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"  ✓ Wrote {out_path} ({len(html):,} bytes)")

    # Also dump raw data as JSON for anyone who wants it
    data_path = OUTPUT_DIR / "needle_data.json"
    data_path.write_text(json.dumps({
        "generated": stats["generated"],
        "total": stats["total"],
        "years": stats["years"],
        "records": all_records,
    }), encoding="utf-8")
    print(f"  ✓ Wrote {data_path}")

    print(f"\n  Done. Serve with: cd docs && python -m http.server 8000")


if __name__ == "__main__":
    main()
