"""FastAPI application for the Boston Needle Map backend."""

import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

from boston_needle_map.analytics import compute_stats
from boston_needle_map.cache import load_cached, save_cache
from boston_needle_map.cleaner import clean
from boston_needle_map.config import RESOURCE_IDS
from boston_needle_map.fetcher import fetch_year
from boston_needle_map.models import CleanedRecord, DashboardStats

logger = logging.getLogger(__name__)

REFRESH_INTERVAL_SECONDS = 3600  # 1 hour


class HealthResponse(BaseModel):
    status: str
    version: str
    generated: str | None = None


class SummaryResponse(BaseModel):
    total: int
    years: list[int]
    peak_hood: str
    peak_hour: int
    peak_dow: str
    avg_monthly: float
    generated: str
    neighborhood_count: int


class NeighborhoodDetailResponse(BaseModel):
    name: str
    count: int
    pct: float
    top_street: str
    avg_resp: float
    slug: str


def _slugify(name: str) -> str:
    """Convert neighborhood name to URL slug."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _load_data(years: list[int] | None = None) -> DashboardStats:
    """Run the pipeline: fetch, clean, compute."""
    if years is None:
        now = datetime.now().year
        years = [y for y in range(now - 2, now + 1) if y in RESOURCE_IDS]

    all_records: list[CleanedRecord] = []
    for year in years:
        raw: list[dict[str, Any]]
        cached = load_cached(year)
        if cached is not None:
            raw = cached
        else:
            raw = fetch_year(year)
            if raw:
                save_cache(year, raw)

        cleaned = [r for r in (clean(row) for row in raw) if r is not None]
        logger.info("  %d: %d raw -> %d valid", year, len(raw), len(cleaned))
        all_records.extend(cleaned)

    if not all_records:
        return DashboardStats(
            total=0,
            years=[],
            heat_keys={},
            points=[],
            hoods=[],
            hourly=[0] * 24,
            year_monthly={},
            zip_stats=[],
            markers=[],
            generated=datetime.now().strftime("%B %d, %Y at %I:%M %p"),
            peak_hood="—",
            peak_hour=0,
            peak_dow="—",
            avg_monthly=0.0,
        )

    return compute_stats(all_records)


async def _background_refresh(app: FastAPI) -> None:
    """Periodically refresh data in the background."""
    while True:
        await asyncio.sleep(REFRESH_INTERVAL_SECONDS)
        try:
            logger.info("Refreshing data...")
            stats = await asyncio.to_thread(_load_data)
            app.state.stats = stats
            logger.info("Data refreshed: %d records", stats.total)
        except Exception:
            logger.exception("Background refresh failed")


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    """Load data on startup, refresh periodically."""
    logger.info("Loading initial data...")
    app.state.stats = await asyncio.to_thread(_load_data)
    logger.info("Loaded %d records", app.state.stats.total)

    task = asyncio.create_task(_background_refresh(app))
    yield
    task.cancel()


app = FastAPI(
    title="Boston Needle Map API",
    description="REST API for Boston 311 sharps collection request data",
    version="4.0.0",
    lifespan=lifespan,
)

# CORS — allow the Astro frontend
allowed_origins = os.environ.get("ALLOWED_ORIGINS", "http://localhost:4321").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


def _get_stats() -> DashboardStats:
    return app.state.stats  # type: ignore[no-any-return]


@app.get("/api/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    stats = _get_stats()
    return HealthResponse(status="ok", version="4.0.0", generated=stats.generated)


@app.get("/api/stats")
async def get_stats() -> DashboardStats:
    return _get_stats()


@app.get("/api/stats/page")
async def get_page_stats() -> dict[str, object]:
    """Lightweight stats for SSR page — excludes heavy heatmap/marker/points data."""
    stats = _get_stats()
    return {
        "total": stats.total,
        "years": stats.years,
        "hoods": [h.model_dump() for h in stats.hoods],
        "hourly": stats.hourly,
        "year_monthly": stats.year_monthly,
        "zip_stats": [z.model_dump() for z in stats.zip_stats],
        "generated": stats.generated,
        "peak_hood": stats.peak_hood,
        "peak_hour": stats.peak_hour,
        "peak_dow": stats.peak_dow,
        "avg_monthly": stats.avg_monthly,
        "initial_heat": stats.heat_keys.get("all", []),
    }


@app.get("/api/stats/summary", response_model=SummaryResponse)
async def get_summary() -> SummaryResponse:
    stats = _get_stats()
    return SummaryResponse(
        total=stats.total,
        years=stats.years,
        peak_hood=stats.peak_hood,
        peak_hour=stats.peak_hour,
        peak_dow=stats.peak_dow,
        avg_monthly=stats.avg_monthly,
        generated=stats.generated,
        neighborhood_count=len(stats.hoods),
    )


@app.get("/api/neighborhoods", response_model=list[NeighborhoodDetailResponse])
async def get_neighborhoods() -> list[NeighborhoodDetailResponse]:
    stats = _get_stats()
    return [
        NeighborhoodDetailResponse(
            name=h.name,
            count=h.count,
            pct=h.pct,
            top_street=h.top_street,
            avg_resp=h.avg_resp,
            slug=_slugify(h.name),
        )
        for h in stats.hoods
    ]


@app.get("/api/neighborhoods/{slug}", response_model=NeighborhoodDetailResponse)
async def get_neighborhood(slug: str) -> NeighborhoodDetailResponse:
    stats = _get_stats()
    for h in stats.hoods:
        if _slugify(h.name) == slug:
            return NeighborhoodDetailResponse(
                name=h.name,
                count=h.count,
                pct=h.pct,
                top_street=h.top_street,
                avg_resp=h.avg_resp,
                slug=slug,
            )
    raise HTTPException(status_code=404, detail=f"Neighborhood '{slug}' not found")


@app.get("/api/heatmap")
async def get_heatmap(
    year: str = Query(default="all"),
    month: int = Query(default=0, ge=0, le=12),
) -> dict[str, str | list[list[float]]]:
    stats = _get_stats()
    if month == 0:
        key = "all" if year == "all" else year
    else:
        mo_pad = f"{month:02d}"
        key = f"all-{mo_pad}" if year == "all" else f"{year}-{mo_pad}"
    points = stats.heat_keys.get(key, [])
    return {"key": key, "points": points}


@app.get("/api/hourly")
async def get_hourly() -> list[int]:
    return _get_stats().hourly


@app.get("/api/monthly")
async def get_monthly() -> dict[str, list[int]]:
    return _get_stats().year_monthly


@app.get("/api/zips")
async def get_zips() -> list[dict[str, str | int]]:
    stats = _get_stats()
    return [{"zip": z.zip, "count": z.count} for z in stats.zip_stats]


@app.get("/api/markers")
async def get_markers(limit: int = Query(default=3000, ge=1, le=10000)) -> list[dict[str, str | float]]:
    stats = _get_stats()
    markers = stats.markers[:limit]
    return [{"lat": m.lat, "lng": m.lng, "dt": m.dt, "hood": m.hood, "street": m.street, "zip": m.zip} for m in markers]
