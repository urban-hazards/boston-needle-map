"""Main pipeline orchestrator."""

import logging
from dataclasses import asdict
from datetime import datetime
from typing import Any

from pipeline import storage
from pipeline.analytics import compute_stats
from pipeline.classifier import WasteClassifier
from pipeline.cleaner import clean
from pipeline.config import (
    ENCAMPMENT_START_YEAR,
    ENCAMPMENT_TYPES,
    NEEDLE_TYPES,
    RESOURCE_IDS,
    STREET_CLEANING_TYPES,
)
from pipeline.enricher import enrich_records
from pipeline.fetcher import fetch_year
from pipeline.models import CleanedRecord

logger = logging.getLogger(__name__)

# Dataset name -> (types, start_year)
DATASET_CONFIG: dict[str, tuple[set[str], int]] = {
    "needles": (NEEDLE_TYPES, 2015),
    "encampments": (ENCAMPMENT_TYPES, ENCAMPMENT_START_YEAR),
    "waste": (STREET_CLEANING_TYPES, 2015),
}

ALL_DATASETS = list(DATASET_CONFIG.keys())


def _fetch_dataset_years(
    dataset: str,
    types: set[str],
    force: bool,
    start_year: int,
) -> list[dict[str, Any]]:
    """Fetch raw records from CKAN, caching each year in S3.

    Always re-fetches the current year. Skips previous years if raw
    data already exists in S3 (unless force=True).
    """
    current_year = datetime.now().year
    all_raw: list[dict[str, Any]] = []

    for year in sorted(RESOURCE_IDS.keys()):
        if year < start_year:
            continue

        s3_key = f"raw/{dataset}_{year}.json"
        is_current = year == current_year

        # Use cached raw data if available and not current year
        if not force and not is_current and storage.file_exists(s3_key):
            logger.info("Loading cached raw/%s_%d.json from S3", dataset, year)
            cached = storage.read_json(s3_key)
            if cached is not None:
                all_raw.extend(cached)
                continue

        # Fetch from CKAN
        records = fetch_year(year, types)
        if records:
            storage.write_json(s3_key, records)
            all_raw.extend(records)

    return all_raw


def _process_dataset(dataset: str, raw_records: list[dict[str, Any]]) -> int:
    """Clean records, compute stats, and write output files to S3.

    Returns the number of cleaned records.
    """
    # Clean / normalize
    cleaned: list[CleanedRecord] = []
    for row in raw_records:
        rec = clean(row)
        if rec is not None:
            cleaned.append(rec)

    logger.info("Cleaned %d / %d records for %s", len(cleaned), len(raw_records), dataset)

    if not cleaned:
        logger.warning("No valid records for %s, skipping stats", dataset)
        return 0

    # Compute dashboard stats
    stats = compute_stats(cleaned)

    # Build the PageStats shape expected by the frontend
    page_stats: dict[str, Any] = {
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

    # Write output files
    storage.write_json(f"{dataset}/stats.json", page_stats)
    storage.write_json(f"{dataset}/points.json", stats.points)
    storage.write_json(f"{dataset}/markers.json", [m.model_dump() for m in stats.markers])
    storage.write_json(f"{dataset}/monthly.json", stats.year_monthly)

    # Write full heatmap keys (for year/month filtering)
    storage.write_json(f"{dataset}/heatmap.json", stats.heat_keys)

    return len(cleaned)


def _process_waste(raw_records: list[dict[str, Any]], force: bool) -> int:
    """Classify street cleaning records for human waste, enrich, compute stats.

    Returns the number of high-confidence waste matches.
    """
    # Load description cache from S3
    description_cache: dict[str, str | None] = storage.read_json("enriched/descriptions.json") or {}
    logger.info("Loaded %d cached descriptions", len(description_cache))

    # Enrich with Open311 descriptions
    enriched_records, description_cache = enrich_records(raw_records, description_cache)

    # Save updated cache
    storage.write_json("enriched/descriptions.json", description_cache)

    # Classify
    classifier = WasteClassifier()
    results = classifier.classify_batch(enriched_records)

    # Save full classification results
    storage.write_json("waste/classified.json", [asdict(r) for r in results])

    # Filter to high + medium confidence for dashboard
    matches = [r for r in results if r.confidence in ("high", "medium")]
    n_high = sum(1 for r in matches if r.confidence == "high")
    n_medium = sum(1 for r in matches if r.confidence == "medium")
    logger.info(
        "Found %d waste matches (%d high, %d medium) out of %d records",
        len(matches),
        n_high,
        n_medium,
        len(results),
    )

    # Build CleanedRecords from the matching raw records
    match_ids = {r.case_id for r in matches}
    matched_raw = [rec for rec in enriched_records if str(rec.get("case_enquiry_id", "")) in match_ids]

    cleaned: list[CleanedRecord] = []
    for row in matched_raw:
        rec = clean(row)
        if rec is not None:
            cleaned.append(rec)

    if not cleaned:
        logger.warning("No valid waste records after cleaning")
        return 0

    # Compute stats and write
    stats = compute_stats(cleaned)

    page_stats: dict[str, Any] = {
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

    storage.write_json("waste/stats.json", page_stats)
    storage.write_json("waste/points.json", stats.points)
    storage.write_json("waste/markers.json", [m.model_dump() for m in stats.markers])
    storage.write_json("waste/monthly.json", stats.year_monthly)
    storage.write_json("waste/heatmap.json", stats.heat_keys)

    return len(cleaned)


def run_pipeline(
    datasets: list[str] | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Main entry point. Fetch, clean, compute, and store.

    Args:
        datasets: List of dataset names to process. Defaults to all.
        force: If True, re-fetch all years from CKAN (ignore cached raw data).

    Returns:
        Dict mapping dataset name to count of processed records.
    """
    to_run = datasets or ALL_DATASETS
    counts: dict[str, int] = {}
    started = datetime.now()

    for dataset in to_run:
        if dataset not in DATASET_CONFIG:
            logger.error("Unknown dataset: %s (available: %s)", dataset, ", ".join(ALL_DATASETS))
            continue

        types, start_year = DATASET_CONFIG[dataset]
        logger.info("=== Processing %s ===", dataset)

        # Fetch raw data
        raw = _fetch_dataset_years(dataset, types, force, start_year)
        logger.info("Fetched %d raw records for %s", len(raw), dataset)

        if not raw:
            logger.warning("No records for %s, skipping", dataset)
            counts[dataset] = 0
            continue

        # Process
        if dataset == "waste":
            counts[dataset] = _process_waste(raw, force)
        else:
            counts[dataset] = _process_dataset(dataset, raw)

    # Write metadata
    metadata = {
        "last_run": started.isoformat(),
        "duration_seconds": (datetime.now() - started).total_seconds(),
        "counts": counts,
    }
    storage.write_json("metadata/last_run.json", metadata)
    logger.info("Pipeline complete: %s", counts)

    return counts
