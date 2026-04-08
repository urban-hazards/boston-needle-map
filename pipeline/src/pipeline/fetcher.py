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
    ENCAMPMENT_QUEUE_START_YEAR,
    ENCAMPMENT_QUEUES,
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


def _fetch_queue_records_sql(resource_id: str, queues: set[str]) -> list[dict[str, Any]]:
    """Use CKAN datastore_search_sql to pull rows matching given queues."""
    queue_clauses = " OR ".join(f"\"queue\" = '{q}'" for q in queues)
    sql = f'SELECT * FROM "{resource_id}" WHERE ({queue_clauses})'
    url = f"{CKAN_BASE}/datastore_search_sql?sql={urllib.parse.quote(sql)}"
    data = _api_get(url)
    if data and data.get("success"):
        return data["result"]["records"]  # type: ignore[no-any-return]
    return []


def _fetch_queue_records_paged(resource_id: str, queues: set[str]) -> list[dict[str, Any]]:
    """Fallback: page through datastore_search with a queue filter."""
    all_records: list[dict[str, Any]] = []
    for queue in queues:
        offset = 0
        limit = 5000
        while True:
            filters = json.dumps({"queue": queue})
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


def fetch_by_queue(year: int, queues: set[str]) -> list[dict[str, Any]]:
    """Fetch records routed to the given queues for a given year."""
    rid = RESOURCE_IDS.get(year)
    if not rid:
        logger.warning("No resource ID for %d, skipping", year)
        return []

    logger.info("Fetching queues %d: trying SQL API...", year)
    records = _fetch_queue_records_sql(rid, queues)
    if records:
        logger.info("Got %d queue records for %d", len(records), year)
        return records

    logger.info("Retrying %d with paged search...", year)
    records = _fetch_queue_records_paged(rid, queues)
    logger.info("Got %d queue records for %d", len(records), year)
    return records


def fetch_encampment_year(year: int) -> list[dict[str, Any]]:
    """Fetch encampment records for a given year.

    Uses two strategies and deduplicates:
    1. type="Encampments" (2025+ only — when the button was added)
    2. queue-based fetch (2023+ — catches pre-button tickets routed internally)
    """
    seen_ids: set[str] = set()
    all_records: list[dict[str, Any]] = []
    type_count = 0
    queue_new_count = 0

    # Strategy 1: fetch by type (2025+)
    if year >= ENCAMPMENT_START_YEAR:
        type_records = fetch_year(year, ENCAMPMENT_TYPES)
        for r in type_records:
            cid = str(r.get("case_enquiry_id", ""))
            if cid and cid not in seen_ids:
                seen_ids.add(cid)
                all_records.append(r)
                type_count += 1

    # Strategy 2: fetch by queue (2023+)
    if year >= ENCAMPMENT_QUEUE_START_YEAR:
        queue_records = fetch_by_queue(year, ENCAMPMENT_QUEUES)
        for r in queue_records:
            cid = str(r.get("case_enquiry_id", ""))
            if cid and cid not in seen_ids:
                seen_ids.add(cid)
                all_records.append(r)
                queue_new_count += 1

    logger.info(
        "Encampments %d: %d total (%d from type, %d new from queues)",
        year,
        len(all_records),
        type_count,
        queue_new_count,
    )
    return all_records


def fetch_street_cleaning_year(year: int) -> list[dict[str, Any]]:
    """Fetch street cleaning records for a given year."""
    return fetch_year(year, STREET_CLEANING_TYPES)
