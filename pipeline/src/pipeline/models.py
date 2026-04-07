"""Pydantic models for pipeline data."""

from pydantic import BaseModel


class CleanedRecord(BaseModel):
    """A validated, normalized record."""

    lat: float
    lng: float
    dt: str  # ISO format datetime
    year: int
    month: int
    hour: int
    dow: str  # day of week name
    hood: str
    street: str
    zipcode: str
    resp_hrs: float | None = None
    source: str | None = None  # "confirmed" or "detected" (waste only)


class NeighborhoodStat(BaseModel):
    """Stats for a single neighborhood."""

    name: str
    slug: str
    count: int
    pct: float
    top_street: str
    avg_resp: float


class ZipStat(BaseModel):
    """Stats for a single zip code."""

    zip: str
    count: int


class MarkerData(BaseModel):
    """Data for an individual map marker."""

    lat: float
    lng: float
    dt: str
    hood: str
    street: str
    zip: str
    source: str | None = None  # "confirmed" or "detected" (waste only)


class DashboardStats(BaseModel):
    """All computed stats needed by the dashboard."""

    total: int
    years: list[int]
    heat_keys: dict[str, list[list[float]]]
    points: list[list[float | int]]
    hoods: list[NeighborhoodStat]
    hourly: list[int]
    year_monthly: dict[str, list[int]]
    zip_stats: list[ZipStat]
    markers: list[MarkerData]
    generated: str
    peak_hood: str
    peak_hour: int
    peak_dow: str
    avg_monthly: float
