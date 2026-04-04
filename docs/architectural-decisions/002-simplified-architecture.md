# ADR-002: Simplified Architecture — Daily Cron + Static JSON + Astro SSR

**Status:** Accepted
**Date:** 2026-04-04
**Supersedes:** ADR-001 (storage decisions still apply, but pipeline/serving architecture simplified)

## Context

The current architecture runs a FastAPI backend 24/7 to serve data that changes once a day. This is overengineered. We're adding a third dataset (human waste) and want to simplify, not add complexity.

## Decision

Replace the FastAPI backend and Redis with:
- A **Python cron job** that runs daily, precomputes everything, writes JSON to Tigris
- **Astro SSR** reads directly from Tigris, serves pages — no backend dependency

### What we're killing
- FastAPI backend service (Railway)
- Redis service (Railway)
- Backend hourly refresh loop
- Backend in-memory state management

### What we're keeping
- Astro SSR frontend (Railway) — becomes the only running service
- Python data pipeline code — becomes a daily cron job

### What we're adding
- Tigris bucket (Railway-native S3) — stores precomputed JSON
- Railway cron service — boots Python, runs pipeline, writes to Tigris, shuts down

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Python Cron (Railway, runs daily, ~60s then shuts down) │
│                                                         │
│  For each dataset (needles, encampments, waste):        │
│    1. Fetch current year from CKAN (overwrites prior)   │
│    2. For waste: classify with spaCy + enrich Open311   │
│    3. Compute stats (neighborhoods, hourly, zips, etc)  │
│    4. Write JSON files to Tigris                        │
│                                                         │
│  Previous years: fetched once, never re-fetched         │
│  Current year: re-fetched daily, overwrites             │
└───────────────────────┬─────────────────────────────────┘
                        │ writes
                        ▼
              ┌──────────────────┐
              │  Tigris Bucket   │
              │                  │
              │  needles.json    │
              │  encampments.json│
              │  waste.json      │
              │  stats.json      │
              │  markers.json    │
              └────────┬─────────┘
                       │ reads
                       ▼
┌─────────────────────────────────────────────────────────┐
│  Astro SSR (Railway, always running)                    │
│                                                         │
│  On page request:                                       │
│    - Read precomputed JSON from Tigris                  │
│    - Render SSR page with stats, tables, charts         │
│    - Ship heatmap + marker data to client               │
│                                                         │
│  Client-side (React islands):                           │
│    - HeatMap: filter points by year/month in browser    │
│    - Charts: render from shipped data                   │
│    - Markers: filter by bounding box in browser         │
└─────────────────────────────────────────────────────────┘
```

---

## Data Pipeline — What the Cron Does

### Fetch strategy

- **Current year:** Re-fetch ALL records from CKAN every run. Overwrite the file in Tigris. No delta logic — simpler and correct. A full year is ~20K records, takes 3-5 seconds to fetch.
- **Previous years:** Fetch once (on first run or if missing from Tigris). Never re-fetched — historical data doesn't change.
- **Waste classification:** Run spaCy classifier on all fetched records. Enrich new matches via Open311 (descriptions cached in Tigris, so only new case IDs hit the API).

### What gets computed

For each dataset (needles, encampments, waste), the pipeline produces:

| File | Contents | Size (gzipped) |
|------|----------|----------------|
| `{dataset}/points.json` | `[[lat, lng, year, month, weight], ...]` for heatmap | ~100 KB |
| `{dataset}/markers.json` | `[{lat, lng, dt, hood, street, zip}, ...]` for map pins | ~340 KB |
| `{dataset}/stats.json` | Neighborhood table, hourly distribution, zip stats, peak stats, totals | ~20 KB |
| `{dataset}/monthly.json` | Year-monthly breakdown for trend charts | ~5 KB |

Total per dataset: ~465 KB gzipped. All three datasets: ~1.4 MB total in the bucket.

### Pipeline timing

| Step | Time |
|------|------|
| Fetch current year from CKAN (3 datasets) | ~10s |
| Classify waste with spaCy | <1s |
| Enrich new waste matches via Open311 | ~5-10s |
| Compute stats for all datasets | ~2s |
| Write JSON files to Tigris | ~2s |
| **Total** | **~20-30s** |

Railway cron boots the container, runs the script, shuts down. Pay for ~60 seconds of compute per day.

---

## Tigris Bucket Structure

```
urban-hazard-maps/
  needles/
    points.json          # Heatmap points: [[lat, lng, year, month, weight], ...]
    markers.json         # Map markers: [{lat, lng, dt, hood, street, zip}, ...]
    stats.json           # Computed stats for SSR page
    monthly.json         # Year-monthly breakdown for charts
  encampments/
    points.json
    markers.json
    stats.json
    monthly.json
  waste/
    points.json
    markers.json
    stats.json
    monthly.json
    classified.json      # Full classification results with scores, terms, tiers
  raw/
    needles_2024.json    # Raw CKAN responses (permanent archive)
    needles_2025.json
    needles_2026.json
    encampments_2025.json
    encampments_2026.json
    street_cleaning_2024.json
    street_cleaning_2025.json
    street_cleaning_2026.json
  enriched/
    descriptions.json    # Open311 description cache (keyed by case_id)
  geo/
    boston_zipcodes.geojson
  metadata/
    last_run.json        # Timestamp, record counts, pipeline version
```

---

## Astro Frontend Changes

### SSR page rendering

Currently `index.astro` calls `fetchPageStats()` which hits the FastAPI backend. Replace with a Tigris read:

```typescript
// Before (calls FastAPI backend)
const stats = await fetchPageStats()

// After (reads from Tigris)
const stats = await readFromTigris("needles/stats.json")
```

Astro can read from S3-compatible storage using the AWS SDK or a simple fetch to the Tigris public URL (if bucket is public) or signed URL.

### Client-side data

Ship all points to the client. The HeatMap React component receives the full points array and filters by year/month in JavaScript:

```typescript
// ~100 KB gzipped for 20K+ points
const filtered = allPoints.filter(([lat, lng, year, month]) =>
  (selectedYear === "all" || year === selectedYear) &&
  (selectedMonth === 0 || month === selectedMonth)
)
```

Filter switching is instant — no network request, no server round-trip.

Marker filtering by bounding box is similarly trivial:

```typescript
const visible = markers.filter(m =>
  m.lat >= bounds.south && m.lat <= bounds.north &&
  m.lng >= bounds.west && m.lng <= bounds.east
)
```

20K array filter in JavaScript: <1ms.

### Caching

Astro can cache the Tigris reads in-memory (data only changes daily). On first request after deploy, fetch from Tigris. Hold in a module-level variable. Subsequent requests use cached data. No Redis needed.

Optionally, set `Cache-Control` headers on Tigris objects so Astro's fetch gets HTTP-level caching too.

---

## Railway Services (after migration)

| Service | Type | Always running? | Cost |
|---------|------|----------------|------|
| Astro SSR | Web service | Yes | ~$5/mo |
| Python pipeline | Cron job | No — runs ~60s/day | ~$0.01/mo |
| Tigris | Object storage | N/A (storage) | Free tier (5GB) |
| ~~FastAPI backend~~ | ~~Web service~~ | ~~Removed~~ | ~~Saved~~ |
| ~~Redis~~ | ~~Database~~ | ~~Removed~~ | ~~Saved~~ |

---

## NLP Classifier (waste pipeline)

Documented in ADR-001. Runs as part of the daily cron. Key points:

- spaCy `en_core_web_sm` for tokenization + lemmatization
- Tiered keyword matching (high signal, medium signal, context boosters)
- Animal waste downgrade: dog/pet/animal context reduces score 70%, overridden by explicit "human" phrases
- 98% recall, 0% false positives on confirmed cases
- Open311 description enrichment finds ~350 additional "dark matter" reports per year
- Three-tier classification: confirmed (routed to INFO_HumanWaste), misrouted (waste found but wrong queue), enriched_only (only visible in caller description)

---

## Migration Path

1. Build the Python pipeline cron (reuse code from `data-experiments/` and `backend/`)
2. Set up Tigris bucket and populate it with initial data
3. Modify Astro frontend to read from Tigris instead of FastAPI
4. Deploy and verify
5. Remove FastAPI backend service from Railway
6. Remove Redis service from Railway
