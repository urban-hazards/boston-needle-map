"""Shared test fixtures."""

import os

# Set env vars at module-import time, BEFORE any pipeline import triggers
# `load_dotenv()` inside pipeline.config. Because load_dotenv defaults to
# override=False, these values take precedence over any local .env file.
# In particular, ENDPOINT must be empty so boto3 routes to moto's mock
# instead of a running MinIO instance on localhost:9000.
os.environ.setdefault("BUCKET", "test-bucket")
os.environ.setdefault("ACCESS_KEY_ID", "testing")
os.environ.setdefault("SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("REGION", "us-east-1")
os.environ["ENDPOINT"] = ""

from collections.abc import Generator  # noqa: E402
from typing import Any  # noqa: E402

import boto3  # noqa: E402
import pytest  # noqa: E402
from moto import mock_aws  # noqa: E402


@pytest.fixture
def s3_bucket() -> Generator[tuple[Any, str], None, None]:
    """Create a mock S3 bucket using moto."""
    bucket_name = "test-bucket"

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=bucket_name)

        # Clear the cached client so storage module picks up the mock
        from pipeline import storage

        storage._get_client.cache_clear()

        yield client, bucket_name

        # Cleanup: clear cache again so other tests get fresh client
        storage._get_client.cache_clear()
