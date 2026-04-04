"""Caching layer — uses Redis when available, falls back to filesystem."""

import json
import logging
import os
from typing import Any

from boston_needle_map.config import CACHE_DIR

logger = logging.getLogger(__name__)

# Default TTL: 24 hours
DEFAULT_TTL_SECONDS = 86400

_redis_client: Any = None
_redis_checked = False


def _get_redis() -> Any:
    """Get Redis client, or None if unavailable."""
    global _redis_client, _redis_checked  # noqa: PLW0603
    if _redis_checked:
        return _redis_client

    _redis_checked = True
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        logger.info("No REDIS_URL set, using filesystem cache")
        return None

    try:
        import redis

        _redis_client = redis.from_url(redis_url, decode_responses=True)
        _redis_client.ping()
        logger.info("Connected to Redis at %s", redis_url.split("@")[-1] if "@" in redis_url else redis_url)
        return _redis_client
    except Exception as e:
        logger.warning("Redis unavailable (%s), falling back to filesystem cache", e)
        _redis_client = None
        return None


def _cache_key(year: int) -> str:
    return f"boston311:year:{year}"


def load_cached(year: int, max_age: int = DEFAULT_TTL_SECONDS) -> list[dict[str, Any]] | None:
    """Load cached records for a year. Tries Redis first, then filesystem."""
    r = _get_redis()
    if r is not None:
        try:
            data = r.get(_cache_key(year))
            if data is not None:
                records = json.loads(data)
                logger.info("  ✓ Redis cache hit for %d (%d records)", year, len(records))
                return records  # type: ignore[no-any-return]
            logger.info("  ○ Redis cache miss for %d", year)
            return None
        except Exception as e:
            logger.warning("  ⚠ Redis read error for %d: %s", year, e)

    # Filesystem fallback
    return _load_file(year, max_age)


def save_cache(year: int, records: list[dict[str, Any]], ttl: int = DEFAULT_TTL_SECONDS) -> None:
    """Save raw API records to cache. Writes to Redis if available, also filesystem."""
    r = _get_redis()
    if r is not None:
        try:
            r.set(_cache_key(year), json.dumps(records), ex=ttl)
            logger.info("  💾 Cached %d records for %d in Redis (TTL: %dh)", len(records), year, ttl // 3600)
        except Exception as e:
            logger.warning("  ⚠ Redis write error for %d: %s", year, e)

    # Always write filesystem as backup
    _save_file(year, records)


def clear_cache() -> None:
    """Remove all cached data from Redis and filesystem."""
    r = _get_redis()
    if r is not None:
        try:
            keys = r.keys("boston311:year:*")
            if keys:
                r.delete(*keys)
                logger.info("  Cleared %d key(s) from Redis", len(keys))
        except Exception as e:
            logger.warning("  ⚠ Redis clear error: %s", e)

    _clear_files()


# --- Filesystem fallback ---


def _load_file(year: int, max_age: int) -> list[dict[str, Any]] | None:
    import time

    path = CACHE_DIR / f"year_{year}.json"
    if not path.exists():
        return None

    age = time.time() - path.stat().st_mtime
    if age > max_age:
        logger.info("  ⏳ File cache for %d is stale (%.1fh old)", year, age / 3600)
        return None

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        logger.info("  ✓ File cache hit for %d (%d records)", year, len(data))
        return data  # type: ignore[no-any-return]
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("  ⚠ File cache read error for %d: %s", year, e)
        return None


def _save_file(year: int, records: list[dict[str, Any]]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"year_{year}.json"
    path.write_text(json.dumps(records), encoding="utf-8")


def _clear_files() -> None:
    if not CACHE_DIR.exists():
        return
    count = 0
    for f in CACHE_DIR.glob("year_*.json"):
        f.unlink()
        count += 1
    if count:
        logger.info("  Cleared %d file(s) from %s/", count, CACHE_DIR)
