"""CKAN API data fetching for Boston 311 records."""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline.config import (
    CKAN_BASE,
    ENCAMPMENT_START_YEAR,
    ENCAMPMENT_TYPES,
    NEEDLE_TYPES,
    RESOURCE_IDS,
    STREET_CLEANING_TYPES,
    UA,
)

logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=2, max=120),
    retry=retry_if_exception_type((urllib.error.URLError, TimeoutError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _api_get(url: str) -> dict[str, Any] | None:
    """GET a CKAN API endpoint, return parsed JSON or None."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=120) as resp:
        try:
            return json.loads(resp.read().decode("utf-8"))  # type: ignore[no-any-return]
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON response: %s", e)
            return None


def _fetch_type_records_sql(resource_id: str, types: set[str]) -> list[dict[str, Any]]:
    """Use CKAN datastore_search_sql to pull rows matching given types."""
    type_clauses = " OR ".join(f"\"type\" = '{t}'" for t in types)
    sql = f'SELECT * FROM "{resource_id}" WHERE ({type_clauses})'
    url = f"{CKAN_BASE}/datastore_search_sql?sql={urllib.parse.quote(sql)}"
    data = _api_get(url)
    if data and data.get("success"):
        return data["result"]["records"]  # type: ignore[no-any-return]
    return []


def _fetch_type_records_paged(resource_id: str, types: set[str]) -> list[dict[str, Any]]:
    """Fallback: page through datastore_search with a TYPE filter."""
    all_records: list[dict[str, Any]] = []
    for record_type in types:
        offset = 0
        limit = 5000
        while True:
            filters = json.dumps({"type": record_type})
            url = (
                f"{CKAN_BASE}/datastore_search"
                f"?resource_id={resource_id}"
                f"&filters={urllib.parse.quote(filters)}"
                f"&limit={limit}&offset={offset}"
            )
            data = _api_get(url)
            if not data or not data.get("success"):
                break
            records = data["result"]["records"]
            all_records.extend(records)
            if len(records) < limit:
                break
            offset += limit
    return all_records


def fetch_needle_records_sql(resource_id: str) -> list[dict[str, Any]]:
    """Use CKAN datastore_search_sql to pull only needle rows (fast)."""
    type_clauses = " OR ".join(f"\"type\" = '{t}'" for t in NEEDLE_TYPES)
    sql = f'SELECT * FROM "{resource_id}" WHERE ({type_clauses}) OR LOWER("type") LIKE \'%needle%\''
    url = f"{CKAN_BASE}/datastore_search_sql?sql={urllib.parse.quote(sql)}"
    data = _api_get(url)
    if data and data.get("success"):
        return data["result"]["records"]  # type: ignore[no-any-return]
    return []


def fetch_year(year: int, types: set[str]) -> list[dict[str, Any]]:
    """Fetch records of the given types for a given year."""
    rid = RESOURCE_IDS.get(year)
    if not rid:
        logger.warning("No resource ID for %d, skipping", year)
        return []

    logger.info("Fetching %d: trying SQL API...", year)
    records = _fetch_type_records_sql(rid, types)
    if records:
        logger.info("Got %d records for %d", len(records), year)
        return records

    logger.info("Retrying %d with paged search...", year)
    records = _fetch_type_records_paged(rid, types)
    logger.info("Got %d records for %d", len(records), year)
    return records


def fetch_needle_year(year: int) -> list[dict[str, Any]]:
    """Fetch needle records for a given year."""
    rid = RESOURCE_IDS.get(year)
    if not rid:
        logger.warning("No resource ID for %d, skipping", year)
        return []

    logger.info("Fetching needles %d: trying SQL API...", year)
    records = fetch_needle_records_sql(rid)
    if records:
        logger.info("Got %d needle records for %d", len(records), year)
        return records

    logger.info("Retrying %d with paged search...", year)
    records = _fetch_type_records_paged(rid, NEEDLE_TYPES)
    logger.info("Got %d needle records for %d", len(records), year)
    return records


def fetch_encampment_year(year: int) -> list[dict[str, Any]]:
    """Fetch encampment records for a given year."""
    if year < ENCAMPMENT_START_YEAR:
        return []
    return fetch_year(year, ENCAMPMENT_TYPES)


def fetch_street_cleaning_year(year: int) -> list[dict[str, Any]]:
    """Fetch street cleaning records for a given year."""
    return fetch_year(year, STREET_CLEANING_TYPES)
