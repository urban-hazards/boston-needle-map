# ADR-001: Human Waste Data Pipeline — Storage and Processing Architecture

**Status:** Proposed
**Date:** 2026-04-04
**Context:** We need a daily pipeline to detect, classify, and serve human waste reports from Boston 311 data.

## Problem

Human waste reports in Boston 311 data are not tagged with their own type. They're scattered across "Street Cleaning", "Needle Pickup", "Encampments", and other categories. Signal lives in:

1. **closure_reason** text (e.g., "Poop removed", "BPW does not service human waste")
2. **Open311 description** — the caller's original complaint, stripped from the CKAN data feed but available via a separate API
3. **queue** field — records routed to `INFO_HumanWaste` are confirmed

We built a spaCy NLP classifier that gets 98% recall / 0% false positives on confirmed cases. Now we need to productionize it.

### Scale

- ~57 new street cleaning records per day
- ~420 waste reports per year (estimated across all sources)
- Classification: <1 second for a day's batch
- Open311 enrichment: ~0.5s per record (rate-limited API)
- 12 years of historical data available (2015-2026)

## Decision Drivers

- Historical data is crunched once, then served repeatedly — reads >> writes
- Open311 enrichment is slow (rate-limited) and should not block API requests
- Redis is already in the stack (Railway, used for CKAN cache)
- We want classified results to survive Redis evictions and redeploys
- Daily incremental updates, not full reprocessing

---

## Options

### Option A: Object Storage (R2/S3) + Redis Cache

```
CKAN API ──> Pipeline ──> Classify ──> Enrich ──> Object Store (R2/S3)
                                                        │
                                                        ▼
                                                  Redis (hot cache)
                                                        │
                                                        ▼
                                                   FastAPI ──> Frontend
```

**How it works:**
- Daily cron fetches new CKAN records, classifies them, enriches via Open311
- Results written to an object store bucket as JSON (one file per dataset, e.g., `waste/classified_2025.json`)
- On write, also push to Redis for immediate serving
- On cold start (redeploy), backend reads from bucket into Redis
- Historical backfill is a one-time batch job, results go to the bucket

**Object store options:**
| Service | Free Tier | S3-Compatible | Notes |
|---------|-----------|---------------|-------|
| Cloudflare R2 | 10GB storage, 10M reads/mo | Yes | No egress fees, generous |
| AWS S3 | 5GB (12 months) | Yes | Industry standard, egress costs |
| Railway Volume | Persistent disk | No (filesystem) | Simplest, but tied to Railway |
| Tigris (on Railway) | 5GB | Yes | Railway-native, S3-compatible |

**Pros:**
- Classified data survives Redis evictions, redeploys, and infra changes
- Can share bucket between services (pipeline worker, backend API)
- Standard pattern — boto3/s3 client is well-understood
- Bucket is inspectable (download and look at the data directly)
- Cost: effectively free at our scale

**Cons:**
- Extra service to configure (bucket credentials, CORS)
- Two write targets (bucket + Redis) to keep in sync
- Slightly more complex deploy

### Option B: Redis Only (extend current pattern)

```
CKAN API ──> Pipeline ──> Classify ──> Enrich ──> Redis
                                                    │
                                                    ▼
                                               FastAPI ──> Frontend
```

**How it works:**
- Same pipeline, but results go directly to Redis with a long TTL (7 days)
- On cold start, if Redis is empty, pipeline re-runs to repopulate
- Historical data re-fetched and re-classified on demand

**Pros:**
- Simplest — no new services
- Matches existing needle/encampment data pattern exactly
- Redis is already configured on Railway

**Cons:**
- Data lost on Redis restart or eviction (must re-fetch from CKAN + re-enrich from Open311)
- Re-enrichment takes hours for historical data (Open311 rate limiting)
- No persistent artifact to inspect or share
- Open311 descriptions are NOT reproducible — if the API changes or goes down, we lose that data

### Option C: Railway Volume (persistent filesystem)

```
CKAN API ──> Pipeline ──> Classify ──> Enrich ──> Volume (JSON files)
                                                        │
                                                        ▼
                                                  Redis (hot cache)
                                                        │
                                                        ▼
                                                   FastAPI ──> Frontend
```

**How it works:**
- Pipeline writes classified results as JSON files to a Railway persistent volume
- Backend reads from volume on cold start, pushes to Redis
- Daily updates append to existing files

**Pros:**
- No external service (stays within Railway)
- Persistent across redeploys
- Filesystem is simple to work with

**Cons:**
- Tied to Railway — harder to migrate
- Volume is attached to one service — can't share between pipeline worker and backend easily
- Not inspectable from outside (can't download files without SSH)
- No versioning or lifecycle management

---

## Recommendation: Option A — Object Storage (R2/S3) + Redis Cache

**Use Cloudflare R2 or Tigris (Railway-native S3).** Reasons:

1. **Open311 descriptions are the critical asset.** The city doesn't include them in the data feed. If we lose our enriched data, it costs hours to re-fetch. A bucket preserves this permanently.

2. **Cost is zero** at our scale. R2 free tier is 10GB storage, 10M reads/month. We'll use maybe 50MB total.

3. **Inspectability.** Brian (or anyone) can download the classified JSON from the bucket and work with it offline. Can't do that with Redis.

4. **Decouples processing from serving.** Pipeline writes to bucket on its schedule. Backend reads from bucket on cold start. Redis is purely a hot cache that can be rebuilt.

### Recommended bucket structure

```
waste-pipeline/
  classified/
    2025.json              # All classified records for 2025
    2024.json
    ...
  enriched/
    descriptions.json      # Open311 descriptions cache (keyed by case_id)
  geo/
    boston_zipcodes.geojson  # ZIP code boundaries for polygon lookup
  metadata/
    last_run.json           # Timestamp + stats of last pipeline run
    classifier_config.json  # Keyword lists, version info
```

Each `classified/{year}.json` contains an array of objects:

```json
{
  "case_id": "101006073445",
  "score": 1.0,
  "confidence": "high",
  "tier": "misrouted",
  "matched_terms": ["fece"],
  "matched_phrases": ["human waste"],
  "lat": 42.345,
  "lng": -71.073,
  "neighborhood": "South End",
  "zipcode": "02118",
  "address": "180 W Canton St",
  "open_dt": "2025-05-19",
  "queue": "PWDx_District 1C: Downtown",
  "type": "Requests for Street Cleaning",
  "closure_reason": "BPW does not service human waste...",
  "open311_description": "Toilet paper (used), bloody washcloth",
  "classified_at": "2026-04-04T19:00:00Z"
}
```

### Three-tier classification

| Tier | Meaning | How detected |
|------|---------|-------------|
| `confirmed` | Properly routed to biohazard contractor | queue = `INFO_HumanWaste` |
| `misrouted` | Waste report sent to wrong team | Classifier match in closure_reason or description, queue != INFO_HumanWaste |
| `enriched_only` | Only detectable from Open311 description | No signal in closure_reason, matched only after enrichment |

---

## Pipeline Architecture

### Daily incremental pipeline

```
┌─────────────────────────────────────────────────┐
│  Daily Cron (Railway)                           │
│                                                 │
│  1. Fetch today's new records from CKAN         │  ~3s
│     - SQL query with date filter                │
│     - All types (street cleaning, needle, etc)  │
│                                                 │
│  2. Classify with spaCy                         │  <1s
│     - Tokenize + lemmatize                      │
│     - Match against keyword tiers               │
│     - Score and assign confidence               │
│                                                 │
│  3. Enrich matches via Open311                  │  ~5-10s
│     - Only records with signal OR               │
│       queue=INFO_HumanWaste                     │
│     - Cache descriptions in bucket              │
│                                                 │
│  4. Fill missing ZIP codes                      │  <1s
│     - Point-in-polygon with GeoJSON             │
│     - Only for records missing zipcode          │
│                                                 │
│  5. Write to bucket                             │  ~1s
│     - Append to classified/{year}.json          │
│     - Update descriptions cache                 │
│     - Update last_run.json                      │
│                                                 │
│  6. Push to Redis                               │  <1s
│     - Update hot cache for API serving          │
│                                                 │
│  Total: ~15-20 seconds                          │
└─────────────────────────────────────────────────┘
```

### Historical backfill (one-time)

```
┌─────────────────────────────────────────────────┐
│  Backfill Job                                   │
│                                                 │
│  For each year 2015-2026:                       │
│    1. Fetch all street cleaning records          │
│    2. Classify closure_reason (fast)            │
│    3. Enrich ALL records via Open311            │  ~3h per year
│       (rate-limited, cached incrementally)       │
│    4. Fill ZIP codes                            │
│    5. Write to bucket                           │
│                                                 │
│  Total: ~24-36 hours (run once, overnight)      │
│  Can be interrupted and resumed (cached)        │
└─────────────────────────────────────────────────┘
```

### Backend serving

```
┌─────────────────────────────────────────────────┐
│  FastAPI Backend                                │
│                                                 │
│  On startup:                                    │
│    1. Check Redis for waste data                │
│    2. If miss, read from bucket                 │
│    3. Compute stats (same as needle/encampment) │
│    4. Serve via /api/waste/* endpoints          │
│                                                 │
│  Endpoints:                                     │
│    GET /api/waste/stats/page                    │
│    GET /api/waste/heatmap                       │
│    GET /api/waste/markers                       │
│    GET /api/waste/neighborhoods                 │
└─────────────────────────────────────────────────┘
```

---

## ZIP Code Enrichment

Two approaches, used together:

### 1. Point-in-polygon lookup (primary)
- Download Boston ZIP code GeoJSON (from Census Bureau TIGER/Line or data.boston.gov)
- Store in bucket at `geo/boston_zipcodes.geojson`
- Use Shapely for point-in-polygon: given (lat, lng), find containing ZIP polygon
- Fast (<1ms per point), no API calls, works offline

### 2. Census Bureau Geocoder (fallback)
- Free, no API key needed
- Bulk endpoint: up to 10,000 addresses per batch
- URL: `https://geocoding.geo.census.gov/geocoder/geographies/coordinates`
- Use only for records where polygon lookup fails (edge cases at boundaries)

---

## Open Questions

1. **Tigris vs R2?** Tigris is Railway-native (simpler config), R2 has a larger free tier and is provider-independent. Both are S3-compatible.

2. **Should the pipeline run inside the existing backend or as a separate service?** Running inside the backend is simpler (shared Redis connection, no extra deploy). Separate service is cleaner but adds a Railway service cost.

3. **How much historical enrichment is worth doing?** Full backfill of all years via Open311 takes ~24-36 hours. Could start with just 2024-2026 (~3-6 hours) and backfill older years later.

4. **Should we store raw CKAN records in the bucket too?** Pro: full reproducibility. Con: ~100MB per year, and the data is always re-fetchable from CKAN.
