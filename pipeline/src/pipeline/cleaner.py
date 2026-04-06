"""Raw record normalization and validation."""

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from dateutil import parser as dateutil_parser

from pipeline.config import BOSTON_BBOX
from pipeline.models import CleanedRecord

# CKAN Analyze Boston timestamps are UTC with no timezone marker.
# Confirmed by cross-referencing with Open311 API (which uses explicit Z suffix).
_UTC = ZoneInfo("UTC")
_EASTERN = ZoneInfo("America/New_York")


def _parse_datetime(dt_str: str) -> datetime | None:
    """Parse a datetime string and convert from UTC to Eastern."""
    if not dt_str or not dt_str.strip():
        return None
    try:
        result: datetime = dateutil_parser.parse(dt_str)
        # CKAN dates have no tzinfo — treat as UTC, convert to Eastern
        if result.tzinfo is None:
            result = result.replace(tzinfo=_UTC)
        return result.astimezone(_EASTERN)
    except (ValueError, OverflowError):
        return None


def clean(row: dict[str, Any]) -> CleanedRecord | None:
    """Normalize a raw API record. Returns None if invalid."""
    try:
        lat = float(row.get("latitude") or row.get("LATITUDE") or 0)
        lon = float(row.get("longitude") or row.get("LONGITUDE") or 0)
    except (ValueError, TypeError):
        return None

    if not (BOSTON_BBOX["lat_min"] <= lat <= BOSTON_BBOX["lat_max"]):
        return None
    if not (BOSTON_BBOX["lon_min"] <= lon <= BOSTON_BBOX["lon_max"]):
        return None

    dt_str = row.get("open_dt") or row.get("OPEN_DT") or ""
    dt = _parse_datetime(dt_str)
    if dt is None:
        return None

    closed_str = row.get("closed_dt") or row.get("CLOSED_DT") or ""
    closed = _parse_datetime(closed_str)

    hood = (
        row.get("neighborhood") or row.get("NEIGHBORHOOD") or row.get("neighborhood_services_district") or ""
    ).strip()
    street = (row.get("location_street_name") or row.get("LOCATION_STREET_NAME") or "").strip()
    zipcode = (row.get("location_zipcode") or row.get("LOCATION_ZIPCODE") or "").strip()[:5]

    resp_hrs = round((closed - dt).total_seconds() / 3600, 1) if closed else None

    return CleanedRecord(
        lat=lat,
        lng=lon,
        dt=dt.isoformat(),
        year=dt.year,
        month=dt.month,
        hour=dt.hour,
        dow=dt.strftime("%A"),
        hood=hood,
        street=street,
        zipcode=zipcode,
        resp_hrs=resp_hrs,
    )
