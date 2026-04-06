"""Enrich 311 records with Open311 API description field."""

import json
import logging
import time
import urllib.request
from typing import Any

from pipeline.config import OPEN311_BASE, UA

logger = logging.getLogger(__name__)


def fetch_open311_description(case_id: str) -> str | None:
    """Look up a single ticket's description from the Open311 API."""
    url = f"{OPEN311_BASE}/requests/{case_id}.json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, list) and data:
                return data[0].get("description")  # type: ignore[no-any-return]
            elif isinstance(data, dict):
                return data.get("description")
    except Exception:
        pass
    return None


def enrich_records(
    records: list[dict[str, Any]],
    description_cache: dict[str, str | None],
    delay: float = 0.2,
    max_records: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str | None]]:
    """Enrich records with Open311 descriptions.

    Instead of using filesystem caching, accepts a description_cache dict
    and returns (enriched_records, updated_cache). The caller is responsible
    for loading/saving the cache from S3.
    """
    to_process = records[:max_records] if max_records else records
    enriched = 0
    skipped = 0
    failed = 0

    for i, record in enumerate(to_process):
        case_id = record.get("case_enquiry_id") or ""
        if not case_id:
            continue

        case_id = str(case_id)

        if case_id in description_cache:
            record["open311_description"] = description_cache[case_id]
            skipped += 1
            continue

        description = fetch_open311_description(case_id)
        description_cache[case_id] = description
        record["open311_description"] = description

        if description:
            enriched += 1
        else:
            failed += 1

        # Progress update every 50 records
        if (i + 1) % 50 == 0:
            logger.info(
                "  ... %d/%d (enriched: %d, cached: %d, failed: %d)",
                i + 1,
                len(to_process),
                enriched,
                skipped,
                failed,
            )

        time.sleep(delay)

    logger.info("Enrichment complete: %d new, %d cached, %d failed", enriched, skipped, failed)
    return to_process, description_cache
