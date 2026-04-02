# Boston 311 Needle Hotspot Map

Automated pipeline that pulls needle cleanup requests from Boston's 311 open data portal and publishes an interactive heatmap. Includes a Streamlit dashboard for local data exploration and a static HTML dashboard for GitHub Pages.

**Live site:** https://urban-hazards.github.io/boston-needle-map/

![Pipeline](https://img.shields.io/badge/schedule-monthly-orange)
![Data](https://img.shields.io/badge/source-data.boston.gov-blue)
![License](https://img.shields.io/badge/license-MIT-green)

![Streamlit Dashboard](docs/images/streamlit-screenshot.png)

---

## How it works

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  data.boston.gov     │────>│  boston-needle-map    │────>│  docs/index.html│
│  CKAN Datastore API │     │  Python 3.12 + uv    │     │  Leaflet.js map │
│  311 Service Reqs   │     │  Pydantic + Typer    │     │  GitHub Pages   │
└─────────────────────┘     └──────────────────────┘     └─────────────────┘
                                     │
                            ┌────────┴────────┐
                            │  Streamlit App  │
                            │  Local explore  │
                            └─────────────────┘
```

**Key details:**
- **Data source:** [Analyze Boston](https://data.boston.gov/dataset/311-service-requests) — 311 Service Requests dataset
- **Filter:** `TYPE = "Needle Pickup"` or `"Needle Clean-up"`
- **API:** Uses CKAN Datastore SQL API (fetches only needle rows, not the full 200MB+ CSV)
- **Output:** Self-contained HTML with embedded data, Leaflet.js heatmap, and CARTO tiles
- **Schedule:** GitHub Actions cron runs at 2 AM EST on the 1st of each month
- **Caching:** Fetched data is cached in `tmp/` to avoid re-fetching during development

---

## Setup

### Prerequisites
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- [lefthook](https://github.com/evilmartians/lefthook) (git hooks)

### Install

```bash
# Clone the repo
git clone https://github.com/<you>/boston-needle-map.git
cd boston-needle-map

# Install dependencies
uv sync

# Install git hooks
lefthook install
```

---

## Usage

### Run the pipeline (generates static HTML for GitHub Pages)

```bash
# Fetch last 3 years + current, generate docs/index.html
uv run boston-needle-map run

# Specific years
uv run boston-needle-map run 2022 2023 2024 2025

# Skip cache (always fetch fresh data)
uv run boston-needle-map run --no-cache
```

### Explore data with Streamlit

```bash
uv run boston-needle-map explore
```

This launches an interactive dashboard at `http://localhost:8501` with:
- Folium heatmap with year/month filters
- Plotly trend charts and hourly distribution
- Neighborhood and zip code rankings

### Other commands

```bash
# Preview the static HTML dashboard
uv run boston-needle-map serve

# Clear cached data
uv run boston-needle-map cache-clear
```

---

## Development

### Linting & Type Checking

```bash
# Lint
uv run ruff check src/ tests/

# Auto-format
uv run ruff format src/ tests/

# Type check
uv run mypy src/

# Run tests
uv run pytest
```

Git hooks (via lefthook) run ruff and mypy automatically on commit.

---

## Project Structure

```
boston-needle-map/
├── src/boston_needle_map/       # Main package
│   ├── cli.py                  # Typer CLI (run, explore, serve, cache-clear)
│   ├── config.py               # Constants (CKAN URLs, resource IDs)
│   ├── models.py               # Pydantic models (CleanedRecord, DashboardStats)
│   ├── fetcher.py              # CKAN API data fetching
│   ├── cleaner.py              # Record normalization & validation
│   ├── analytics.py            # Stats computation
│   ├── renderer.py             # Static HTML generation
│   ├── cache.py                # tmp/ caching layer
│   └── app.py                  # Streamlit interactive dashboard
├── templates/
│   └── dashboard.html          # HTML template for static site
├── tests/                      # Test suite
├── docs/                       # Generated output (GitHub Pages)
├── tmp/                        # Cached API data (gitignored)
├── pyproject.toml              # Project config (deps, ruff, mypy)
├── lefthook.yml                # Git hook config
└── CLAUDE.md                   # AI assistant instructions
```

---

## Configuration

Edit `src/boston_needle_map/config.py` to adjust:

| Variable | What it does |
|---|---|
| `RESOURCE_IDS` | Map of year to CKAN resource ID. Add new years as Boston publishes them. |
| `NEEDLE_TYPES` | Set of TYPE values to filter on. |
| `BOSTON_BBOX` | Bounding box for coordinate validation. |

To find a new year's resource ID:
1. Go to https://data.boston.gov/dataset/311-service-requests
2. Click the year's CSV resource
3. The resource ID is in the URL: `/resource/<THIS-PART>/`

---

## Data source

All data comes from the City of Boston's [Analyze Boston](https://data.boston.gov/) open data portal under the [Open Data Commons PDDL license](http://www.opendefinition.org/licenses/odc-pddl).

The 311 dataset contains all service requests. This pipeline filters for needle-related request types, which represent reports to the city's Mobile Sharps Collection Team for picking up discarded needles found in public spaces.
