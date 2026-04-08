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
    ENCAMPMENT_QUEUE_START_YEAR,
    ENCAMPMENT_TYPES,
    NEEDLE_TYPES,
    RESOURCE_IDS,
    STREET_CLEANING_TYPES,
)
from pipeline.enricher import enrich_records
from pipeline.fetcher import fetch_encampment_year, fetch_year
from pipeline.models import CleanedRecord

logger = logging.getLogger(__name__)

# Dataset name -> (types, start_year)
# Note: encampments uses ENCAMPMENT_QUEUE_START_YEAR because fetch_encampment_year()
# now pulls from both type (2025+) and queue (2023+). See fetcher.py.
DATASET_CONFIG: dict[str, tuple[set[str], int]] = {
    "needles": (NEEDLE_TYPES, 2015),
    "encampments": (ENCAMPMENT_TYPES, ENCAMPMENT_QUEUE_START_YEAR),
    "waste": (STREET_CLEANING_TYPES, 2024),
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

        # Encampments uses a versioned cache key: pre-PR runs cached
        # type-only data under `raw/encampments_{year}.json`. The new
        # combined type + queue fetcher writes to `_v2_` so old caches
        # are ignored on first deploy (no `--force` needed).
        s3_key = f"raw/encampments_v2_{year}.json" if dataset == "encampments" else f"raw/{dataset}_{year}.json"
        is_current = year == current_year

        # Use cached raw data if available and not current year
        if not force and not is_current and storage.file_exists(s3_key):
            logger.info("Loading cached %s from S3", s3_key)
            cached = storage.read_json(s3_key)
            if cached is not None:
                all_raw.extend(cached)
                continue

        # Fetch from CKAN — encampments use a special fetcher that combines
        # type-based and queue-based results
        records = fetch_encampment_year(year) if dataset == "encampments" else fetch_year(year, types)
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
    # Step 1: Classify on closure_reason first (fast, no API calls)
    classifier = WasteClassifier()
    initial_results = classifier.classify_batch(raw_records)

    # Step 2: Identify records that need enrichment:
    # - High/medium confidence matches (already have signal)
    # - Records routed to INFO_HumanWaste queue (confirmed waste)
    to_enrich: list[dict[str, Any]] = []
    for rec, res in zip(raw_records, initial_results, strict=True):
        queue = rec.get("queue", "")
        if res.confidence in ("high", "medium") or "HumanWaste" in queue:
            to_enrich.append(rec)

    logger.info("Found %d records to enrich (matches + INFO_HumanWaste)", len(to_enrich))

    # Step 3: Enrich only the targeted records via Open311
    description_cache: dict[str, str | None] = storage.read_json("enriched/descriptions.json") or {}
    logger.info("Loaded %d cached descriptions", len(description_cache))

    enriched_records, description_cache = enrich_records(to_enrich, description_cache)
    storage.write_json("enriched/descriptions.json", description_cache)

    # Step 4: Re-classify enriched records (descriptions may reveal more signal)
    enriched_results = classifier.classify_batch(enriched_records)

    # Step 5: Combine results — enriched matches + initial matches not in enriched set
    enriched_ids = {str(r.get("case_enquiry_id")) for r in enriched_records}
    all_matches: list[dict[str, Any]] = []
    all_results = []

    # Add enriched matches
    for rec, res in zip(enriched_records, enriched_results, strict=True):
        queue = rec.get("queue", "")
        if res.confidence in ("high", "medium") or "HumanWaste" in queue:
            all_matches.append(rec)
            all_results.append(res)

    # Add initial matches not already in enriched set
    for rec, res in zip(raw_records, initial_results, strict=True):
        cid = str(rec.get("case_enquiry_id", ""))
        if cid not in enriched_ids and res.confidence in ("high", "medium"):
            all_matches.append(rec)
            all_results.append(res)

    n_high = sum(1 for r in all_results if r.confidence == "high")
    n_medium = sum(1 for r in all_results if r.confidence == "medium")
    logger.info(
        "Found %d waste matches (%d high, %d medium) out of %d records",
        len(all_matches),
        n_high,
        n_medium,
        len(raw_records),
    )

    # Save full classification results
    storage.write_json("waste/classified.json", [asdict(r) for r in all_results])

    cleaned: list[CleanedRecord] = []
    for row in all_matches:
        cleaned_rec = clean(row)
        if cleaned_rec is not None:
            cleaned.append(cleaned_rec)

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
