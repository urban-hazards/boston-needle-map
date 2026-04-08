# "Other" Tickets: The Invisible 311 Data

## What we found

**142,946 tickets** filed through the BOS:311 app's "Other" button (aka "General Request") between Jan 2023 and Apr 2026. These tickets have **zero records in CKAN** (data.boston.gov) — they are completely invisible in the city's public bulk data.

We scraped them day-by-day from the Open311 API.

## What happened to them

| Metric | Count | % |
|---|---|---|
| Total tickets | 142,946 | |
| Closed | 95,379 | 67% |
| **Still open** | **47,567** | **33%** |
| Reclassified by staff | 54,127 | 38% |
| Never reclassified | 88,819 | 62% |
| **Open AND never reclassified** | **13,499** | **Black hole** |

## How long open tickets sit

| Age | Count |
|---|---|
| < 1 week | 368 |
| 1-4 weeks | 895 |
| 1-3 months | 1,930 |
| 3-12 months | 9,898 |
| 1-2 years | 16,424 |
| **2+ years** | **18,052** |

**34,476 tickets have been open over a year.**

## Time to close (for tickets that did close)

- Median: **428 days** (over a year)
- Same day: 9,772
- Within 1 week: 13,094
- Over 6 months: 66,496
- Over 1 year: 53,097

## Human waste tickets in "Other"

- **2,433 total** human waste reports
- **60% still open** (1,456 tickets)
- Median age of open waste tickets: **624 days** (almost 2 years)
- Open over 1 year: 1,174
- Oldest open: **3 years, 52 days** (150 Southampton St, Roxbury)
- 98% were reclassified (staff tagged them as Human Waste), but many were never closed

### Seasonality

Peaks every summer (Jul-Sep), drops in winter. Consistent pattern 2023-2025.

### Repeat locations (people kept filing, nothing happened)

| Address | Waste tickets | Still open | Total 311 tickets there |
|---|---|---|---|
| 960 Dorchester Ave | 11 | 7 | 66 |
| 39-41 Belden St, Dorchester | 16 | 4 | 115 |
| Mass Ave & Melnea Cass Blvd | 9 | 5 | 155 |
| 1143-1149 Washington St, Roxbury | 10 | 5 | 147 |
| Williams St & Forest Hills, JP | 3 | 3 | 68 |
| 230 Shawmut Ave, Roxbury | 2 | 2 | 69 |

## Encampment/homeless tickets in "Other"

- **6,533 total** (keyword: encampment, unsheltered, homeless issue)
- 26% still open (1,688)
- 98% reclassified

## What the 13,499 black hole tickets are about

Open, never reclassified, no one looked at them:

| Category | Count |
|---|---|
| Tree/branch | 3,276 |
| Parking | 1,469 |
| Rats/rodents | 700 |
| Graffiti | 487 |
| Encampment/homeless | 479 |
| Noise | 409 |
| Human waste | 199 |
| Pothole/road | 147 |
| Illegal dumping | 52 |
| Needles/syringes | 35 |

## Top reclassification tags (staff-assigned categories)

| Tag | Count |
|---|---|
| Miscellaneous | 7,143 |
| BPD: Quality of Life | 5,739 |
| Wires: Downed/Low Hanging | 4,062 |
| BPD: Traffic/Parking | 3,366 |
| Unsheltered Persons | 3,106 |
| Animal Care & Control | 2,304 |
| Human Waste | 2,277 |
| USPS: Graffiti/Mailbox Issues | 2,144 |
| Mass DCR | 2,119 |
| MBTA | 1,967 |
| Encampments | 1,635 |

## Why this matters for the pipeline

1. **Waste classifier is blind to 2,400+ reports** — these never made it into CKAN
2. **Encampment dataset is missing ~6,500 pre-button tickets** from "Other"
3. **Open311 has user descriptions and photos** that CKAN lacks
4. The "Other" button is the default for anything that doesn't fit a category — it's where confused/urgent reports go

## Data sources

- **Open311 API**: `https://boston2-production.spotmobile.net/open311/v2/requests.json`
- **Service code**: `Mayor's 24 Hour Hotline:General Request:General Request`
- **Scraped**: Jan 1, 2023 through Apr 8, 2026 (1,192 days)
- **Raw data**: `raw/YYYY-MM-DD.json` (one file per day)

## Reproducing the data

The raw data (117MB, 1,192 JSON files) is gitignored. To regenerate:

```bash
cd data-experiments/research/open311-other
python3 fetch.py          # fetches all days, caches to raw/, newest-first
python3 analyze.py        # reads raw/, outputs summary.json + analysis_output.txt
```

`fetch.py` caches each day as `raw/YYYY-MM-DD.json` so you can stop/resume. It has adaptive backoff for rate limiting. No API key needed.

Or ask Brian — he has the full dataset on his machine.

## Open questions

- Are the "still open" tickets truly unresolved, or is there a separate system (text-based CRT/NEST workflow) handling them without closing the 311 ticket?
- Do repeat filings at the same address represent different incidents or frustrated residents re-reporting?
- Can we cross-reference Open311 ticket IDs with CKAN case_enquiry_ids to find any overlap?
