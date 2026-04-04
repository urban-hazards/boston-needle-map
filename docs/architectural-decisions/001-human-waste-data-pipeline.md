# ADR-001: Human Waste Data Pipeline — Storage and Processing Architecture

**Status:** Accepted
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

---

## Decisions

- **Object storage:** Tigris (Railway-native, S3-compatible)
- **Pipeline deployment:** Separate Railway service (daily cron), shares Docker base with backend
- **Hot cache:** Not needed — backend reads from Tigris on startup into memory (same pattern as needle/encampment data). Tigris reads are ~100-200ms for small JSON files, only happens on cold start.
- **Raw data:** Store in bucket permanently (reproducibility, Open311 descriptions are not re-fetchable if the API changes)
- **Initial backfill:** 2024-2026, run locally (~3-6 hours). Older years backfilled later.

---

## NLP Classifier Architecture

### Overview

The classifier uses spaCy's English language model (`en_core_web_sm`) for tokenization and lemmatization, then applies tiered keyword matching rules. This is NOT regex — spaCy breaks text into real linguistic tokens and reduces them to base forms (lemmas), which solves the false positive problem that regex can't handle.

### Why spaCy tokenization matters

Regex matching for waste keywords produces false positives because target words appear as substrings of unrelated words:

| Word | Contains | But means |
|------|----------|-----------|
| Sa**turd**ay | "turd" | Day of the week |
| s**pee**d | "pee" | Velocity |
| s**crap**ed | "crap" | To scrape |

spaCy tokenizes and lemmatizes each word independently, so:
- "Saturday" → token `saturday`, lemma `saturday` (never matches "turd")
- "speed" → token `speed`, lemma `speed` (never matches "pee")
- "scraped" → token `scraped`, lemma `scrap` (never matches "crap")

### Animal Waste Downgrade (False Positive Prevention)

A critical part of the classifier is distinguishing **human** waste from **animal** waste. Many 311 records mention dog poop, which is not what we're looking for. The classifier handles this through a three-layer system:

#### Layer 1: False Positive Context Detection

When classifying text, the classifier checks for animal-related context words alongside waste terms:

```
FALSE_POSITIVE_CONTEXTS = {
    "dog", "canine", "pet", "animal",
    "rat", "rodent",
    "restaurant", "establishment", "food", "inspection"
}
```

If any of these words appear in the same record as a waste keyword, the record's score is multiplied by **0.3x** (70% reduction), dropping it from medium/high confidence to low.

**Examples:**
- "Dog poop removed" → `poop` matched + `dog` context → score reduced 70% → **low confidence** (correctly excluded)
- "Poop removed" → `poop` matched, no animal context → **medium confidence** (correctly included)
- "BPW do not pick up dog feces" → `fece` matched + `dog` context → score reduced → **low confidence**

#### Layer 2: Phrase Override

When an explicit **human-qualifying phrase** is present (e.g., "human poop", "human feces", "human waste"), the false positive downgrade is **skipped entirely** — the phrase already disambiguates:

- "Human poop and clothing on curb near tree" → phrase `human poop` matched → `tree` does NOT trigger downgrade → **medium confidence**

Without this override, the word "tree" (which appears as a location marker) would incorrectly downgrade the score.

#### Layer 3: BPW Rejection Override

The standard city rejection phrase "BPW does not service human waste on public streets" is detected as a special pattern. When present, the false positive downgrade is also skipped — this phrase is unambiguous confirmation of a human waste report.

#### Scoring Flow

```
Input text
    │
    ▼
┌──────────────────────────┐
│  spaCy tokenize +        │
│  lemmatize               │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Check high-signal       │  fece, poop, shit, crap, turd,
│  lemmas                  │  diarrhea, excrement, biohazard, ...
│                          │  → +0.3 per match
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Check high-signal       │  "human waste", "bodily fluid",
│  phrases                 │  "human feces", "fecal matter", ...
│                          │  → +0.4 per match
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Check BPW rejection     │  "bpw does not service human waste"
│  pattern                 │  → +0.8
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Check medium-signal     │  urine, vomit, sewage, pee, ...
│  lemmas                  │  → +0.1 to +0.2 (depends on context)
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Check context boosters  │  encampment, homeless, needle,
│  (only if signal exists) │  sidewalk, contractor, ...
│                          │  → +0.05 per match
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────────────────────┐
│  FALSE POSITIVE CHECK                    │
│                                          │
│  If animal context words found           │
│  AND no human-qualifying phrases         │
│  AND no BPW rejection:                   │
│       → score × 0.3 (70% reduction)     │
│                                          │
│  If human phrase OR BPW rejection:       │
│       → no reduction (already confirmed) │
└──────────┬───────────────────────────────┘
           │
           ▼
┌──────────────────────────┐
│  Assign confidence       │
│  ≥ 0.6 → high            │
│  ≥ 0.3 → medium          │
│  > 0.0 → low             │
│  = 0.0 → none            │
└──────────────────────────┘
```

### Keyword Configuration

**High-signal lemmas** (single term = likely waste):
`feces, fecal, fece, feece, poop, poope, excrement, biohazard, defecate, defacate, defacating, defecation, shit, crap, turd, diarrhea`

Note: includes common misspellings (`feece` for "feeces", `defacate`/`defacating` for "defecate") and spaCy lemma variants (`poope` is spaCy's lemma for "pooped", `fece` for "feces").

**High-signal phrases** (multi-word = very likely waste):
`human waste, bodily fluid, human fece, human feece, bio hazard, bio waste, human feces, human excrement, human poop, human dump, human diarrhea, fecal matter, outside contractor, biohazard contractor, not service human`

**Medium-signal lemmas** (need context to distinguish human vs animal):
`urine, urinate, urination, pee, peed, stool, vomit, sewage, soiled, bathroom`

**Context boosters** (upgrade medium signal when co-occurring):
`encampment, homeless, needle, syringe, sidewalk, street, public, tent, camp, contractor`

**False positive contexts** (downgrade when co-occurring with waste terms):
`dog, canine, pet, animal, rat, rodent, restaurant, establishment, food, inspection`

### Classifier Performance

Tested against 50 confirmed human waste reports from the `INFO_HumanWaste` queue (enriched with Open311 descriptions):

| Metric | Value |
|--------|-------|
| Recall | 98% (49/50) |
| Precision | 100% (0 false positives on 32K records) |
| Only miss | "Human faces and socks" (typo for "feces") |

---

## Storage Architecture

### Tigris Bucket Structure

```
waste-pipeline/
  raw/
    ckan/
      street_cleaning_2024.json    # Raw CKAN records
      street_cleaning_2025.json
      street_cleaning_2026.json
      all_types_2025.json          # Multi-type fetch
  classified/
    2024.json                      # Classified + enriched records
    2025.json
    2026.json
  enriched/
    descriptions.json              # Open311 descriptions cache (keyed by case_id)
  geo/
    boston_zipcodes.geojson         # ZIP code boundaries for polygon lookup
  metadata/
    last_run.json                  # Timestamp + stats of last pipeline run
    classifier_config.json         # Keyword lists, version info
```

### Data Flow

```
                                        Tigris (permanent storage)
                                       ┌─────────────────────────┐
CKAN API ──┐                           │  raw/ckan/*.json        │
           │                           │  classified/*.json      │
           ▼                           │  enriched/desc.json     │
┌─────────────────────┐                │  geo/zipcodes.geojson   │
│  Pipeline Service   │  write ──────> │  metadata/last_run.json │
│  (daily cron)       │                └────────────┬────────────┘
│                     │                             │
│  1. Fetch CKAN      │                             │ read on startup
│  2. Classify (spaCy)│                             │
│  3. Enrich (Open311)│                             ▼
│  4. Fill ZIP codes  │                ┌─────────────────────────┐
│  5. Write to Tigris │                │  FastAPI Backend        │
└─────────────────────┘                │  app.state.waste_stats  │
                                       │  (in-memory, like       │
                                       │   needle/encampment)    │
                                       │                         │
                                       │  /api/waste/stats/page  │
                                       │  /api/waste/heatmap     │
                                       │  /api/waste/markers     │
                                       └─────────────────────────┘
```

No Redis layer for waste data. The backend reads classified JSON from Tigris on startup (~100-200ms) and holds it in memory. This matches the existing pattern — needle and encampment data is also held in `app.state` after being fetched/computed, not read from Redis per-request.

### Classified Record Schema

Each `classified/{year}.json` contains an array:

```json
{
  "case_id": "101006073445",
  "score": 1.0,
  "confidence": "high",
  "tier": "misrouted",
  "matched_terms": ["fece"],
  "matched_phrases": ["human waste"],
  "bpw_rejection": true,
  "false_positive_flags": [],
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

### Three-Tier Classification

| Tier | Meaning | How detected |
|------|---------|-------------|
| `confirmed` | Properly routed to biohazard contractor | queue = `INFO_HumanWaste` |
| `misrouted` | Waste report sent to wrong team | Classifier match in closure_reason or description, queue != `INFO_HumanWaste` |
| `enriched_only` | Only detectable from Open311 description | No signal in closure_reason, matched only after Open311 enrichment |

---

## Pipeline Service

### Deployment

Separate Railway service running as a daily cron. Shares the same Python/uv base as the backend but runs independently. Can reuse the same Docker image with a different entrypoint command (e.g., `python -m waste_pipeline.run` vs `uvicorn boston_needle_map.api:app`).

### Daily Incremental Pipeline

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
│     - Animal waste downgrade                    │
│     - Score and assign confidence               │
│                                                 │
│  3. Enrich matches via Open311                  │  ~5-10s
│     - Only records with signal OR               │
│       queue=INFO_HumanWaste                     │
│     - Check description cache first             │
│                                                 │
│  4. Fill missing ZIP codes                      │  <1s
│     - Point-in-polygon with GeoJSON             │
│     - Only for records missing zipcode          │
│                                                 │
│  5. Write to Tigris                             │  ~1s
│     - Append to classified/{year}.json          │
│     - Store raw records to raw/ckan/            │
│     - Update descriptions cache                 │
│     - Update last_run.json                      │
│                                                 │
│  Total: ~15-20 seconds                          │
└─────────────────────────────────────────────────┘
```

### Historical Backfill

Run locally (not on Railway) due to Open311 rate limiting. Interruptible and resumable — descriptions are cached incrementally.

| Scope | Estimated Time | Records |
|-------|---------------|---------|
| 2024-2026 | 3-6 hours | ~60K street cleaning |
| 2015-2026 | 24-36 hours | ~200K+ street cleaning |

Starting with 2024-2026. Older years backfilled later.

---

## ZIP Code Enrichment

Two approaches, used together:

### 1. Point-in-polygon lookup (primary)
- Boston ZIP code GeoJSON from Census Bureau TIGER/Line or data.boston.gov
- Store in Tigris at `geo/boston_zipcodes.geojson`, load at pipeline startup
- Use Shapely for point-in-polygon: given (lat, lng), find containing ZIP polygon
- Fast (<1ms per point), no API calls, works offline
- Covers 100% of records with lat/lng (99.98% of all records)

### 2. Census Bureau Geocoder (fallback)
- Free, no API key needed
- Bulk endpoint: up to 10,000 addresses per batch
- URL: `https://geocoding.geo.census.gov/geocoder/geographies/coordinates`
- Use only for edge cases where polygon lookup fails
