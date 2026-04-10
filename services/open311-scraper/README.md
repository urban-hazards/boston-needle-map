# Open311 Scraper

Standalone service that fetches "Other" (General Request) tickets from Boston's Open311 API and stores raw daily JSON files in S3/Tigris.

## What it does

- Fetches tickets day-by-day from the Open311 API (back to 2023-01-01)
- Stores each day as `open311/raw/YYYY-MM-DD.json` in S3
- Skips days already fetched (resumable)
- Handles rate limiting with exponential backoff
- Writes `open311/manifest.json` with run metadata

## Railway deployment

Deploy as a **cron service** in the same Railway project as the main pipeline. It shares the same Tigris bucket via env vars:

- `BUCKET` — Tigris bucket name
- `ACCESS_KEY_ID` — Tigris access key
- `SECRET_ACCESS_KEY` — Tigris secret key
- `ENDPOINT` — Tigris endpoint URL
- `REGION` — defaults to `us-east-1`

Set the cron schedule to `0 6 * * *` (daily at 6 AM UTC) or similar.

Set the Railway **root directory** to `services/open311-scraper`.

## First run

The first run backfills all days from 2023-01-01 to yesterday. This takes 30-60 minutes depending on rate limiting. Subsequent runs only fetch new days (seconds).

## Data format

Each day file contains an array of Open311 service request objects. The pipeline can read these from S3 at `open311/raw/*` to incorporate into analysis.
