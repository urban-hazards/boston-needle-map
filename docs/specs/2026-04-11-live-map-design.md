# Live Map: Real-Time Public Health Ticket Dashboard

> Status: Planned (blocked on scraper backfill completion)
> Created: 2026-04-11

## Problem

Boston's 311 system generates thousands of public health/safety reports daily
but provides no real-time operational view. Decision-makers allocating SHARPS
teams, cleanup crews, and outreach workers rely on stale data or anecdotal
reports. There's no way to see "where should we send people right now" or detect
displacement when enforcement pushes problems block to block.

## Goal

Enable same-day resource allocation by showing where public health/safety
tickets are clustering across types in real-time. A supervisor opens this at any
point during the day and sees active hotspots that need coordinated response.

## Audience

Primary: resource allocators — people deciding where to send crews today.
Secondary: council members, journalists, residents who keep it open and share
patterns. The site is public, no auth.

The funding argument: if we can show patterns, hotspots, and movement,
resources can be directed same day.

## Ticket Types (Phase 1: Public Health/Safety)

- Needles (Needle Pickup)
- Encampments
- Human waste (NLP-classified from Street Cleaning)
- Illegal dumping
- Street cleaning (catch-all, some misrouted waste/encampment reports)

Infrastructure types (potholes, sidewalks, signals, signs, abandoned vehicles,
parking) are a separate live map page in the future.

**NLP note:** As the waste classifier and future classifiers improve over time,
the live map data quality improves automatically. The system must support
reprocessing/reclassification without architectural changes. New classified
types feed into the live view on the next pipeline run.

## Architecture: Poller + Static JSON (Approach A)

### Why This Approach

- Zero new infrastructure — S3 + Railway cron, same as existing pipeline
- Frontend stays static Astro SSR, no WebSocket complexity
- Cheap — poller uses ~2 API calls per 5-min cycle, S3 reads are pennies
- If poller dies, last good state persists in S3
- 1-6 minute lag doesn't matter — nobody dispatches faster than that
- SSE or WebSockets can be bolted on later without rewriting

### Why Not Direct API Proxy

10 req/min rate limit shared across all users. Two concurrent visitors would
exhaust the limit. The site breaking when it gets popular is the opposite of
what we want.

## Component 1: The Poller

New mode in the existing scraper (`services/open311-scraper/fetch.py`), not a
separate service. Runs on a 5-minute Railway cron after backfill completes.

### Each Cycle (3 steps)

**1. Incremental pull**
`?status=open&updated_after={last_poll}` across all 5 types. Typically 1-5 API
calls total per cycle.

**2. Hourly reconciliation**
Once per hour, pull all open tickets per type to catch closures. ~10 API calls.
Tickets that disappear from `status=open` results get marked closed with a
timestamp.

**3. Write outputs**
- `live/snapshots/YYYY-MM-DD/HH-MM.json` — raw API response, immutable
- `live/state.json` — computed current view (all known open tickets)
- `live/clusters.json` — precomputed block-level clusters

### Append-Only Audit Trail

Snapshots are never deleted or overwritten. Every poll cycle writes a timestamped
record of what the API returned. If the city edits a ticket description, changes
a status, or backdates a closure, we have the before and after.

Storage estimate: ~500 tickets x 288 cycles/day x ~500 bytes = ~70MB/day
uncompressed. ~2GB/month on Tigris. Flag if this grows significantly but well
within current plan limits.

### State Per Ticket

```
id                    — service_request_id
type                  — classified type (not just raw 311 type)
slug                  — scraper slug (needles, encampments, etc.)
lat, lng              — coordinates
address               — street address
description           — citizen's free-text report (from Open311 API)
photo_url             — Cloudinary URL if present
opened_at             — requested_datetime
last_updated          — updated_datetime from API
first_seen_by_poller  — when our poller first picked it up
closed_at             — null while open, timestamp when it stops appearing
```

### Rate Limit Budget

Per 5-min cycle:
- Incremental pull: 1-5 calls (usually 1 per type with <100 new tickets)
- Hourly reconciliation: ~10 calls (once per hour only)
- Typical cycle: 1-5 calls = well under 10 req/min limit

## Component 2: Block-Level Clustering

Computed server-side in the poller so the frontend gets pre-grouped data.

### Algorithm

Grid-based clustering using geohash-6 precision (~600m x ~600m, roughly
2 city blocks). Deterministic — same ticket always lands in the same cell.
Clusters don't jump around between refreshes.

### Cluster Record

```
cell_id               — geohash
center_lat, center_lng
total_count
counts_by_type        — {needles: 3, encampments: 1, waste: 2}
ticket_ids            — [...]
newest_ticket_at
oldest_open_since
is_hotspot            — true if 3+ types present
```

### Cross-Type Hotspot Detection

A cluster with 3+ distinct types is flagged as a hotspot. This is the core
insight: "this block has needles AND encampments AND trash — send a coordinated
team, not three separate ones."

### Displacement Detection

Compare current clusters against the previous cycle. If a hotspot disappears
from one cell and a new one appears in an adjacent cell within the same hour,
flag as possible displacement. Stored as a `movements` array in clusters.json.

Context: crowds and activity move one block over when police arrive. Block-level
clustering that recalculates each cycle shows this migration in near-real-time.

## Component 3: Frontend — `/live` Page

New Astro page with a React island. Public URL, no auth.

### Map Layer

- Individual pins at zoom 15+ (street level)
- Cluster circles at lower zoom with count badge
- Pins colored by type (same palette as existing heatmap layers)
- Multi-type clusters get segmented ring showing type breakdown
- Hotspots (3+ types) get pulsing highlight animation

### Sidebar Panel

- **Active hotspots** — sorted by severity (most types, most tickets). Click
  to fly map to that location.
- **Ticket feed** — chronological stream of new tickets. Type icon, address,
  time ago, description preview.
- **Type filter toggles** — same pattern as existing NeighborhoodTable but for
  the 5 public health types.

### Auto-Refresh

Frontend polls `live/state.json` and `live/clusters.json` from S3 every 90
seconds. Subtle pulse animation on refresh indicator. No full page reload —
swap data and re-render map.

### Daily Briefing Mode

Toggle or `/live?view=daily` shows last 24 hours instead of just currently-open.
Includes recently-closed tickets (grayed out) so supervisors see what got
handled overnight. The "8 AM briefing" view.

## Dependencies & Sequencing

### Must exist before this ships

1. **Scraper backfill complete** — poller shares the scraper codebase and
   Railway service. Backfill finishes first, then service switches to poll mode.

2. **Waste NLP classifier in pipeline** — poller tags street-cleaning tickets as
   "unclassified" and the daily pipeline reclassifies on its next run. Keeps
   the poller lightweight (no spaCy dependency). Waste detection lags up to 24h
   on the live map; needles and encampments show up instantly.

3. **Field normalization layer** — Open311 API uses different field names than
   CKAN (`requested_datetime` vs `open_dt`, `service_request_id` vs
   `case_enquiry_id`). The poller needs a thin mapping so downstream code
   handles both sources.

### No changes required

- Existing daily pipeline keeps running against CKAN for historical data
- Existing heatmap pages untouched
- No new Railway services — poller replaces scraper on same cron
- S3 path `live/` prefix is new, no conflicts with existing `raw/`, `needles/`,
  `waste/`, etc.

### Future enhancements (not in scope)

- Infrastructure live map (potholes, sidewalks, signals) — separate page
- SSE/WebSocket push for sub-minute updates
- Photo archival to S3 (Cloudinary URLs are volatile — separate decision)
- API history depth investigation (why only 2023+, storage/retention implications)
- Per-ticket change log (diffing snapshots to detect city edits)
- Displacement animation/replay from snapshot history
