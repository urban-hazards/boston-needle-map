"""Tests for S3 storage module."""

from typing import Any

from pipeline import storage


class TestWriteAndReadJson:
    """Test write_json / read_json roundtrip."""

    def test_roundtrip(self, s3_bucket: tuple[Any, str]) -> None:
        """Write JSON and read it back."""
        _client, bucket = s3_bucket
        data = {"hello": "world", "count": 42, "nested": [1, 2, 3]}

        storage.write_json("test/data.json", data, bucket=bucket)
        result = storage.read_json("test/data.json", bucket=bucket)

        assert result is not None
        assert result["hello"] == "world"
        assert result["count"] == 42
        assert result["nested"] == [1, 2, 3]

    def test_roundtrip_list(self, s3_bucket: tuple[Any, str]) -> None:
        """Write a list and read it back."""
        _client, bucket = s3_bucket
        data = [{"a": 1}, {"b": 2}]

        storage.write_json("test/list.json", data, bucket=bucket)
        result = storage.read_json("test/list.json", bucket=bucket)

        assert result is not None
        assert len(result) == 2
        assert result[0]["a"] == 1


class TestReadJsonMissing:
    """Test read_json returns None for missing keys."""

    def test_missing_key_returns_none(self, s3_bucket: tuple[Any, str]) -> None:
        """Reading a nonexistent key returns None."""
        _client, bucket = s3_bucket
        result = storage.read_json("does/not/exist.json", bucket=bucket)
        assert result is None


class TestFileExists:
    """Test file_exists."""

    def test_exists_true(self, s3_bucket: tuple[Any, str]) -> None:
        """file_exists returns True for existing keys."""
        _client, bucket = s3_bucket
        storage.write_json("exists.json", {"ok": True}, bucket=bucket)
        assert storage.file_exists("exists.json", bucket=bucket) is True

    def test_exists_false(self, s3_bucket: tuple[Any, str]) -> None:
        """file_exists returns False for missing keys."""
        _client, bucket = s3_bucket
        assert storage.file_exists("nope.json", bucket=bucket) is False


class TestListKeys:
    """Test list_keys."""

    def test_list_with_prefix(self, s3_bucket: tuple[Any, str]) -> None:
        """list_keys returns keys under a prefix."""
        _client, bucket = s3_bucket
        storage.write_json("raw/needles_2024.json", [], bucket=bucket)
        storage.write_json("raw/needles_2025.json", [], bucket=bucket)
        storage.write_json("other/file.json", [], bucket=bucket)

        keys = storage.list_keys("raw/", bucket=bucket)
        assert len(keys) == 2
        assert "raw/needles_2024.json" in keys
        assert "raw/needles_2025.json" in keys

    def test_list_empty_prefix(self, s3_bucket: tuple[Any, str]) -> None:
        """list_keys returns empty list for unknown prefix."""
        _client, bucket = s3_bucket
        keys = storage.list_keys("nonexistent/", bucket=bucket)
        assert keys == []
