"""S3-compatible storage wrapper using boto3."""

import json
import logging
from functools import lru_cache
from typing import Any

import boto3

from pipeline.config import BUCKET, S3_ACCESS_KEY, S3_ENDPOINT, S3_REGION, S3_SECRET_KEY

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_client() -> Any:
    """Create and cache an S3 client."""
    kwargs: dict[str, Any] = {
        "service_name": "s3",
        "region_name": S3_REGION,
    }
    if S3_ACCESS_KEY:
        kwargs["aws_access_key_id"] = S3_ACCESS_KEY
    if S3_SECRET_KEY:
        kwargs["aws_secret_access_key"] = S3_SECRET_KEY
    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT
    return boto3.client(**kwargs)


def write_json(key: str, data: Any, bucket: str | None = None) -> None:
    """Serialize data as JSON and upload to the bucket."""
    bucket = bucket or BUCKET
    body = json.dumps(data, default=str, separators=(",", ":"))
    _get_client().put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"), ContentType="application/json")
    logger.info("Wrote s3://%s/%s (%d bytes)", bucket, key, len(body))


def read_json(key: str, bucket: str | None = None) -> Any | None:
    """Download and parse a JSON object. Returns None on 404."""
    bucket = bucket or BUCKET
    try:
        resp = _get_client().get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8")
        return json.loads(body)
    except _get_client().exceptions.NoSuchKey:
        return None
    except Exception:
        logger.warning("Failed to read s3://%s/%s", bucket, key, exc_info=True)
        return None


def file_exists(key: str, bucket: str | None = None) -> bool:
    """Check if a key exists in the bucket via HEAD request."""
    bucket = bucket or BUCKET
    try:
        _get_client().head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def list_keys(prefix: str, bucket: str | None = None) -> list[str]:
    """List object keys under a prefix."""
    bucket = bucket or BUCKET
    try:
        resp = _get_client().list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]
    except Exception:
        logger.warning("Failed to list s3://%s/%s", bucket, prefix, exc_info=True)
        return []
