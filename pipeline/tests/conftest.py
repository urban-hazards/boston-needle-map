"""Shared test fixtures."""

import os
from collections.abc import Generator
from typing import Any

import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def s3_bucket() -> Generator[tuple[Any, str], None, None]:
    """Create a mock S3 bucket using moto."""
    bucket_name = "test-bucket"

    # Set env vars before importing storage (which caches the client)
    os.environ["BUCKET"] = bucket_name
    os.environ["ACCESS_KEY_ID"] = "testing"
    os.environ["SECRET_ACCESS_KEY"] = "testing"
    os.environ["REGION"] = "us-east-1"
    # Clear endpoint so boto3 uses moto's mock
    os.environ.pop("ENDPOINT", None)

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=bucket_name)

        # Clear the cached client so storage module picks up the mock
        from pipeline import storage

        storage._get_client.cache_clear()

        yield client, bucket_name

        # Cleanup: clear cache again so other tests get fresh client
        storage._get_client.cache_clear()
