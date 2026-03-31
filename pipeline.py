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

    # Heatmap points: [lat, lng, intensity]
    # Cluster nearby points for performance
    grid = defaultdict(int)
    bin_size = 0.0008  # ~90m
    for r in records:
        key = (round(r["lat"] / bin_size) * bin_size,
               round(r["lng"] / bin_size) * bin_size)
        grid[key] += 1

    heat_points = [[lat, lng, count] for (lat, lng), count in grid.items()]

    # Individual points for the marker layer (cap at 3000 most recent)
    recent = sorted(records, key=lambda r: r["dt"], reverse=True)[:3000]
    markers = [
        {"lat": r["lat"], "lng": r["lng"], "dt": r["dt"][:10],
         "hood": r["hood"], "street": r["street"]}
        for r in recent
    ]

    return {
        "total": len(records),
        "years": sorted(set(r["year"] for r in records)),
        "monthly": monthly_sorted,
        "hoods": hood_stats[:15],
        "hourly": hourly_data,
        "dow": dict(dow.most_common()),
        "heat_points": heat_points,
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
<title>Boston 311 Needle Hotspot Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #080810; --bg2: #0e0e1a; --bg3: #151528;
    --border: #1c1c36; --border2: #26264a;
    --t1: #eeeef5; --t2: #a0a0c0; --t3: #5c5c80;
    --red: #ef4444; --orange: #f97316; --amber: #eab308;
    --green: #22c55e;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:var(--bg); color:var(--t1); font-family:'DM Sans',system-ui,sans-serif; }
  a { color:var(--orange); text-decoration:none; }
  a:hover { text-decoration:underline; }

  .hdr {
    padding:16px 20px; border-bottom:1px solid var(--border);
    background:linear-gradient(180deg,#0c0c18,var(--bg));
    display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:10px;
  }
  .hdr-left { display:flex; align-items:center; gap:10px; }
  .dot { width:9px;height:9px;border-radius:50%;background:var(--red);
    box-shadow:0 0 10px 3px rgba(239,68,68,.5); animation:pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }
  .hdr h1 { font-family:'DM Mono',monospace; font-size:15px; font-weight:500;
    letter-spacing:.07em; text-transform:uppercase;
    background:linear-gradient(90deg,var(--orange),var(--amber));
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
  .hdr-sub { font-family:'DM Mono',monospace; font-size:10px; color:var(--t3); letter-spacing:.04em; margin-top:3px; }
  .hdr-right { font-family:'DM Mono',monospace; font-size:10px; color:var(--t3); text-align:right; }

  .kpi-bar {
    display:flex; gap:1px; border-bottom:1px solid var(--border); background:var(--border);
  }
  .kpi { flex:1; padding:12px 16px; background:var(--bg2); min-width:120px; }
  .kpi-label { font-family:'DM Mono',monospace; font-size:9px; color:var(--t3);
    letter-spacing:.1em; text-transform:uppercase; margin-bottom:3px; }
  .kpi-val { font-family:'DM Mono',monospace; font-size:20px; font-weight:500; }
  .kpi-val.red { color:var(--red); } .kpi-val.orange { color:var(--orange); }
  .kpi-val.amber { color:var(--amber); } .kpi-val.green { color:var(--green); }

  .tabs {
    display:flex; gap:1px; border-bottom:1px solid var(--border); background:var(--border);
  }
  .tab {
    padding:10px 18px; background:var(--bg2); cursor:pointer; border:none;
    font-family:'DM Mono',monospace; font-size:11px; letter-spacing:.06em;
    text-transform:uppercase; color:var(--t3); transition:all .15s;
  }
  .tab:hover { color:var(--t2); }
  .tab.active { color:var(--orange); background:var(--bg);
    box-shadow:inset 0 -2px 0 var(--orange); }

  .panel { display:none; } .panel.active { display:block; }

  #map { width:100%; height:calc(100vh - 210px); min-height:450px; background:var(--bg); }

  .chart-wrap { padding:20px; }
  .chart-title { font-family:'DM Mono',monospace; font-size:12px; color:var(--t2);
    letter-spacing:.06em; text-transform:uppercase; margin-bottom:14px; }

  .bar-chart { display:flex; align-items:flex-end; gap:3px; height:180px; }
  .bar-col { flex:1; display:flex; flex-direction:column; align-items:center; gap:3px; }
  .bar { width:100%; max-width:32px; border-radius:3px 3px 0 0; transition:height .4s ease;
    min-width:8px; cursor:pointer; position:relative; }
  .bar:hover { filter:brightness(1.3); }
  .bar-label { font-family:'DM Mono',monospace; font-size:8px; color:var(--t3);
    transform:rotate(-50deg); white-space:nowrap; }
  .bar-val { font-family:'DM Mono',monospace; font-size:8px; color:var(--t2); }

  .hood-table { width:100%; border-collapse:collapse; }
  .hood-table th { font-family:'DM Mono',monospace; font-size:9px; color:var(--t3);
    letter-spacing:.08em; text-transform:uppercase; text-align:left; padding:8px 12px;
    border-bottom:1px solid var(--border); }
  .hood-table td { padding:8px 12px; font-size:13px; border-bottom:1px solid var(--bg3); }
  .hood-table tr:hover td { background:rgba(249,115,22,.03); }
  .hood-bar { height:6px; border-radius:3px; background:var(--orange); min-width:2px; }

  .hour-chart { display:flex; align-items:flex-end; gap:2px; height:120px; }
  .hour-bar { flex:1; border-radius:2px 2px 0 0; min-width:4px; transition:height .3s; }

  .ftr { padding:12px 20px; border-top:1px solid var(--border);
    font-family:'DM Mono',monospace; font-size:9px; color:var(--t3); letter-spacing:.04em; }

  .leaflet-container { background:var(--bg) !important; }
  .info-popup { font-family:'DM Mono',monospace; font-size:11px; line-height:1.5; }
  .info-popup b { color:var(--orange); }

  @media(max-width:700px) {
    .kpi-bar { flex-wrap:wrap; }
    .kpi { min-width:calc(50% - 1px); }
    #map { height:60vh; min-height:300px; }
  }
</style>
</head>
<body>

<div class="hdr">
  <div>
    <div class="hdr-left"><div class="dot"></div><h1>Boston 311 Needle Hotspots</h1></div>
    <div class="hdr-sub">Source: data.boston.gov/dataset/311-service-requests · Filter: Needle Pickup / Needle Clean-up</div>
  </div>
  <div class="hdr-right">Updated: $GENERATED<br>
    <a href="https://data.boston.gov/dataset/311-service-requests" target="_blank">Open Data Portal ↗</a>
  </div>
</div>

<div class="kpi-bar">
  <div class="kpi"><div class="kpi-label">Total requests</div><div class="kpi-val red">$TOTAL</div></div>
  <div class="kpi"><div class="kpi-label">Top neighborhood</div><div class="kpi-val orange">$PEAK_HOOD</div></div>
  <div class="kpi"><div class="kpi-label">Peak hour</div><div class="kpi-val amber">${PEAK_HOUR}:00</div></div>
  <div class="kpi"><div class="kpi-label">Peak day</div><div class="kpi-val green">$PEAK_DOW</div></div>
  <div class="kpi"><div class="kpi-label">Avg / month</div><div class="kpi-val orange">$AVG_MONTHLY</div></div>
</div>

<div class="tabs">
  <button class="tab active" onclick="showTab('map')">Heat Map</button>
  <button class="tab" onclick="showTab('trend')">Monthly Trend</button>
  <button class="tab" onclick="showTab('hoods')">Neighborhoods</button>
  <button class="tab" onclick="showTab('hours')">By Hour</button>
</div>

<div id="p-map" class="panel active"><div id="map"></div></div>

<div id="p-trend" class="panel">
  <div class="chart-wrap">
    <div class="chart-title">Monthly needle pickup requests</div>
    <div class="bar-chart" id="trend-chart"></div>
  </div>
</div>

<div id="p-hoods" class="panel">
  <div class="chart-wrap">
    <div class="chart-title">Neighborhood breakdown</div>
    <table class="hood-table">
      <thead><tr><th>Neighborhood</th><th>Requests</th><th>% of total</th><th></th><th>Top street</th><th>Avg response</th></tr></thead>
      <tbody id="hood-tbody"></tbody>
    </table>
  </div>
</div>

<div id="p-hours" class="panel">
  <div class="chart-wrap">
    <div class="chart-title">Requests by hour of day</div>
    <div class="hour-chart" id="hour-chart"></div>
    <div style="display:flex;justify-content:space-between;margin-top:6px;">
      <span style="font-family:'DM Mono',monospace;font-size:9px;color:var(--t3)">12 AM</span>
      <span style="font-family:'DM Mono',monospace;font-size:9px;color:var(--t3)">6 AM</span>
      <span style="font-family:'DM Mono',monospace;font-size:9px;color:var(--t3)">12 PM</span>
      <span style="font-family:'DM Mono',monospace;font-size:9px;color:var(--t3)">6 PM</span>
      <span style="font-family:'DM Mono',monospace;font-size:9px;color:var(--t3)">11 PM</span>
    </div>
  </div>
</div>

<div class="ftr">
  Pipeline: GitHub Actions (monthly cron) → CKAN Datastore SQL API → Python → Static HTML + Leaflet.js → GitHub Pages
  · Data: City of Boston Open Data (PDDL License)
  · Years: $YEARS
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
<script>
// ── Embedded data (generated by pipeline.py) ───────────────────────────
const HEAT = $HEAT_JSON;
const MARKERS = $MARKERS_JSON;
const MONTHLY = $MONTHLY_JSON;
const HOODS = $HOODS_JSON;
const HOURLY = $HOURLY_JSON;

// ── Tabs ───────────────────────────────────────────────────────────────
function showTab(id) {
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('p-' + id).classList.add('active');
  event.target.classList.add('active');
  if (id === 'map') map.invalidateSize();
}

// ── Map ────────────────────────────────────────────────────────────────
const map = L.map('map', {
  center: [42.335, -71.075],
  zoom: 13,
  zoomControl: true,
  attributionControl: true,
});

// Dark tile layer
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  attribution: '&copy; <a href="https://carto.com/">CARTO</a> · <a href="https://www.openstreetmap.org/copyright">OSM</a>',
  subdomains: 'abcd',
  maxZoom: 19,
}).addTo(map);

// Heatmap layer
const heat = L.heatLayer(HEAT, {
  radius: 22,
  blur: 18,
  maxZoom: 15,
  max: Math.max(...HEAT.map(p => p[2])),
  gradient: {
    0.0: '#0d0d2b',
    0.2: '#1a1a5e',
    0.4: '#e17055',
    0.6: '#f97316',
    0.8: '#ef4444',
    1.0: '#fbbf24',
  },
}).addTo(map);

// Marker cluster (show on zoom)
const markerGroup = L.layerGroup();
MARKERS.forEach(m => {
  const circle = L.circleMarker([m.lat, m.lng], {
    radius: 4, fillColor: '#f97316', fillOpacity: 0.7,
    color: '#f97316', weight: 1, opacity: 0.4,
  });
  circle.bindPopup(
    `<div class="info-popup"><b>${m.hood || 'Unknown'}</b><br>${m.street || ''}<br>${m.dt}</div>`
  );
  markerGroup.addLayer(circle);
});

map.on('zoomend', function() {
  if (map.getZoom() >= 15) { map.addLayer(markerGroup); }
  else { map.removeLayer(markerGroup); }
});

// ── Monthly trend ──────────────────────────────────────────────────────
(function() {
  const el = document.getElementById('trend-chart');
  const maxVal = Math.max(...MONTHLY.map(m => m[1]));
  MONTHLY.forEach(([label, count]) => {
    const h = Math.max(4, (count / maxVal) * 170);
    const intensity = count / maxVal;
    const color = intensity > 0.7 ? '#ef4444' : intensity > 0.4 ? '#f97316' : '#eab308';
    const col = document.createElement('div');
    col.className = 'bar-col';
    col.innerHTML = `
      <div class="bar-val">${count}</div>
      <div class="bar" style="height:${h}px;background:linear-gradient(180deg,${color},${color}66)"></div>
      <div class="bar-label">${label.slice(2)}</div>
    `;
    el.appendChild(col);
  });
})();

// ── Neighborhood table ─────────────────────────────────────────────────
(function() {
  const el = document.getElementById('hood-tbody');
  const maxCount = HOODS.length ? HOODS[0].count : 1;
  HOODS.forEach(h => {
    const barW = Math.max(2, (h.count / maxCount) * 100);
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td style="font-weight:600">${h.name}</td>
      <td style="font-family:'DM Mono',monospace;color:var(--orange)">${h.count.toLocaleString()}</td>
      <td style="font-family:'DM Mono',monospace;color:var(--t3)">${h.pct}%</td>
      <td style="width:120px"><div class="hood-bar" style="width:${barW}%"></div></td>
      <td style="color:var(--t2)">${h.top_street}</td>
      <td style="font-family:'DM Mono',monospace;color:var(--t3)">${h.avg_resp}h</td>
    `;
    el.appendChild(tr);
  });
})();

// ── Hourly chart ───────────────────────────────────────────────────────
(function() {
  const el = document.getElementById('hour-chart');
  const maxVal = Math.max(...HOURLY);
  HOURLY.forEach((count, hour) => {
    const h = Math.max(2, (count / maxVal) * 110);
    const intensity = count / maxVal;
    const color = intensity > 0.7 ? '#ef4444' : intensity > 0.4 ? '#f97316' : intensity > 0.2 ? '#eab308' : '#3b3b6e';
    const bar = document.createElement('div');
    bar.className = 'hour-bar';
    bar.style.height = h + 'px';
    bar.style.background = `linear-gradient(180deg,${color},${color}44)`;
    bar.title = `${hour}:00 — ${count} requests`;
    el.appendChild(bar);
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
