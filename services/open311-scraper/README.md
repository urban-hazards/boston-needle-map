# Open311 Scraper

Standalone service that fetches **all 16 service types** from Boston's Open311 API and stores raw daily JSON files in S3/Tigris.

## Why not just use CKAN?

The city publishes 311 data in bulk on data.boston.gov, but that export **strips critical fields**:

| Field | Open311 API | CKAN Bulk |
|-------|-------------|-----------|
| Citizen description (free text) | Present (94% of tickets) | **Removed** |
| Photos (Cloudinary URLs) | Present (85% of needle tickets) | **Always empty** |
| Staff status notes | Present | Truncated to `closure_reason` only |
| "Other" tickets (General Request) | Present (142k+) | **Missing entirely** |

This scraper preserves the complete records that CKAN strips down.

## Service types

All 16 types exposed by the BOS:311 app are scraped:

- `other` — General Request (invisible in CKAN)
- `needles` — Needle Cleanup
- `encampments` — Encampments
- `potholes` — Pothole Repair
- `sidewalks` — Broken Sidewalk
- `dead-animals` — Dead Animal Pickup
- `graffiti` — Illegal Graffiti
- `litter` — Litter
- `rodents` — Rodent Sighting
- `trash-cans` — Overflowing Trash Can
- `abandoned-vehicles` — Abandoned Vehicle
- `parking` — Illegal Parking
- `traffic-signals` — Traffic Signal
- `signs` — Damaged Sign
- `abandoned-bikes` — Abandoned Bicycle
- `illegal-trash` — Residential Trash out Illegally

## S3 layout

```
open311/
  other/2023-01-01.json
  needles/2023-01-01.json
  potholes/2023-01-01.json
  ...
  manifest.json
```

## Railway deployment

Deploy as a **cron service** in the same Railway project. Shares the same Tigris bucket.

Env vars: `BUCKET`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `ENDPOINT`, `REGION`

Root directory: `services/open311-scraper`

Cron schedule: `0 6 * * *` (daily at 6 AM UTC)

## First run

Backfills from 2023-01-01 across all 16 types. Full backfill takes ~36 hours at the API rate limit. Each type is independently resumable — if the service restarts, it skips days already in S3.

## Usage

```bash
python fetch.py                    # fetch all types, backfill from 2023
python fetch.py --type other       # fetch only "Other" tickets
python fetch.py --type needles     # fetch only needle tickets
python fetch.py --start 2025-01-01 # start from a specific date
python fetch.py --dry-run          # show plan without fetching
```
