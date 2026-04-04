"""Cache adapter — Redis in production, filesystem for local dev."""

from __future__ import annotations

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 86400  # 24 hours


class CacheBackend(ABC):
    """Abstract cache interface."""

    @abstractmethod
    def get(self, key: str) -> list[dict[str, Any]] | None: ...

    @abstractmethod
    def set(self, key: str, data: list[dict[str, Any]], ttl: int = DEFAULT_TTL_SECONDS) -> None: ...

    @abstractmethod
    def clear(self, pattern: str) -> None: ...


class RedisCache(CacheBackend):
    """Redis-backed cache for production."""

    def __init__(self, redis_url: str) -> None:
        import redis as redis_lib

        self._client: Any = redis_lib.from_url(redis_url, decode_responses=True)
        self._client.ping()
        host = redis_url.split("@")[-1] if "@" in redis_url else redis_url
        logger.info("Cache: connected to Redis at %s", host)

    def get(self, key: str) -> list[dict[str, Any]] | None:
        data: str | None = self._client.get(key)
        if data is None:
            return None
        return json.loads(data)  # type: ignore[no-any-return]

    def set(self, key: str, data: list[dict[str, Any]], ttl: int = DEFAULT_TTL_SECONDS) -> None:
        self._client.set(key, json.dumps(data), ex=ttl)

    def clear(self, pattern: str) -> None:
        keys: list[str] = self._client.keys(pattern)
        if keys:
            self._client.delete(*keys)
            logger.info("Cleared %d key(s) from Redis", len(keys))


class FileCache(CacheBackend):
    """Filesystem-backed cache for local development."""

    def __init__(self, cache_dir: Path) -> None:
        self._dir = cache_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        logger.info("Cache: using filesystem at %s/", self._dir)

    def _path(self, key: str) -> Path:
        safe_key = key.replace(":", "_")
        return self._dir / f"{safe_key}.json"

    def get(self, key: str) -> list[dict[str, Any]] | None:
        path = self._path(key)
        if not path.exists():
            return None

        age = time.time() - path.stat().st_mtime
        if age > DEFAULT_TTL_SECONDS:
            logger.info("File cache stale for %s (%.1fh old)", key, age / 3600)
            return None

        try:
            return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("File cache read error for %s: %s", key, e)
            return None

    def set(self, key: str, data: list[dict[str, Any]], ttl: int = DEFAULT_TTL_SECONDS) -> None:
        path = self._path(key)
        path.write_text(json.dumps(data), encoding="utf-8")

    def clear(self, pattern: str) -> None:
        count = 0
        for f in self._dir.glob("*.json"):
            f.unlink()
            count += 1
        if count:
            logger.info("Cleared %d file(s) from %s/", count, self._dir)


# --- Singleton ---

_backend: CacheBackend | None = None


def _get_backend() -> CacheBackend:
    global _backend  # noqa: PLW0603
    if _backend is not None:
        return _backend

    redis_url = os.environ.get("REDIS_URL")
    if redis_url:
        try:
            _backend = RedisCache(redis_url)
            return _backend
        except Exception as e:
            logger.warning("Redis unavailable (%s), falling back to file cache", e)

    from boston_needle_map.config import CACHE_DIR

    _backend = FileCache(CACHE_DIR)
    return _backend


# --- Public API (same interface as before) ---


def _cache_key(year: int) -> str:
    return f"boston311:year:{year}"


def load_cached(year: int, max_age: int = DEFAULT_TTL_SECONDS) -> list[dict[str, Any]] | None:
    """Load cached records for a year."""
    records = _get_backend().get(_cache_key(year))
    if records is not None:
        logger.info("  ✓ Cache hit for %d (%d records)", year, len(records))
    else:
        logger.info("  ○ Cache miss for %d", year)
    return records


def save_cache(year: int, records: list[dict[str, Any]], ttl: int = DEFAULT_TTL_SECONDS) -> None:
    """Save raw API records to cache."""
    _get_backend().set(_cache_key(year), records, ttl)
    logger.info("  💾 Cached %d records for %d (TTL: %dh)", len(records), year, ttl // 3600)


def clear_cache() -> None:
    """Remove all cached data."""
    _get_backend().clear("boston311:year:*")
