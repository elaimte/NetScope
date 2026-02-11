"""Tests for the CSV upload endpoint.

Covers all validation paths: file metadata, CSV content parsing,
column checks, data type validation, and successful ingestion.
"""

from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.routers.upload import (
    _validate_csv_content,
    _validate_file_metadata,
)


# ---------------------------------------------------------------------------
# Helper to build valid CSV bytes
# ---------------------------------------------------------------------------

VALID_CSV = (
    b"username,mac_address,start_time,usage_time,upload,download\n"
    b"user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:30:00,1000.0,2000.0\n"
)


def _make_upload(filename="test.csv", content_type="text/csv"):
    """Create a minimal mock UploadFile for metadata validation."""
    mock = MagicMock()
    mock.filename = filename
    mock.content_type = content_type
    return mock


# ---------------------------------------------------------------------------
# _validate_file_metadata
# ---------------------------------------------------------------------------


class TestValidateFileMetadata:
    """Tests for file extension and MIME type validation."""

    def test_valid_csv_file(self):
        """Valid .csv file should not raise."""
        _validate_file_metadata(_make_upload("data.csv", "text/csv"))

    def test_valid_csv_uppercase(self):
        """Uppercase .CSV extension should be accepted."""
        _validate_file_metadata(_make_upload("DATA.CSV", "text/csv"))

    def test_no_filename(self):
        """Missing filename should raise 400."""
        with pytest.raises(HTTPException) as exc:
            _validate_file_metadata(_make_upload(filename="", content_type="text/csv"))
        assert exc.value.status_code == 400
        assert "No filename" in exc.value.detail

    def test_none_filename(self):
        """None filename should raise 400."""
        with pytest.raises(HTTPException) as exc:
            _validate_file_metadata(_make_upload(filename=None, content_type="text/csv"))
        assert exc.value.status_code == 400
        assert "No filename" in exc.value.detail

    def test_wrong_extension_txt(self):
        """.txt file should be rejected."""
        with pytest.raises(HTTPException) as exc:
            _validate_file_metadata(_make_upload("data.txt", "text/plain"))
        assert exc.value.status_code == 400
        assert "Only .csv files" in exc.value.detail

    def test_wrong_extension_xlsx(self):
        """.xlsx file should be rejected."""
        with pytest.raises(HTTPException) as exc:
            _validate_file_metadata(_make_upload("data.xlsx", "application/vnd.openxmlformats"))
        assert exc.value.status_code == 400
        assert "Only .csv files" in exc.value.detail

    def test_invalid_mime_type(self):
        """Unsupported MIME type should be rejected."""
        with pytest.raises(HTTPException) as exc:
            _validate_file_metadata(_make_upload("data.csv", "application/json"))
        assert exc.value.status_code == 400
        assert "Invalid MIME type" in exc.value.detail

    def test_none_content_type_accepted(self):
        """None content_type should pass (not checked)."""
        _validate_file_metadata(_make_upload("data.csv", None))

    def test_allowed_mime_types(self):
        """All allowed MIME types should pass."""
        for mime in ["text/csv", "application/csv", "application/vnd.ms-excel",
                     "text/plain", "application/octet-stream"]:
            _validate_file_metadata(_make_upload("data.csv", mime))


# ---------------------------------------------------------------------------
# _validate_csv_content
# ---------------------------------------------------------------------------


class TestValidateCsvContent:
    """Tests for CSV content validation."""

    def test_valid_csv(self):
        """Valid CSV bytes should return a DataFrame."""
        df = _validate_csv_content(VALID_CSV)
        assert len(df) == 1

    def test_empty_bytes(self):
        """Empty bytes should raise 400."""
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(b"")
        assert exc.value.status_code == 400
        assert "empty" in exc.value.detail.lower()

    def test_file_too_large(self):
        """Bytes exceeding max size should raise 400."""
        huge = b"x" * (50 * 1024 * 1024 + 1)
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(huge)
        assert exc.value.status_code == 400
        assert "too large" in exc.value.detail.lower()

    def test_non_utf8(self):
        """Non-UTF-8 bytes should raise 400."""
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(b"\xff\xfe\x00\x01invalid")
        assert exc.value.status_code == 400
        assert "UTF-8" in exc.value.detail

    def test_csv_empty_data(self):
        """CSV with no parseable content should raise 400 (EmptyDataError)."""
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(b"\n\n\n")
        assert exc.value.status_code == 400

    def test_csv_parser_error(self):
        """Malformed CSV should raise 400 (ParserError)."""
        bad_csv = b'a,b\n1,2,3,4\n"unclosed'
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(bad_csv)
        assert exc.value.status_code == 400

    def test_missing_columns(self):
        """CSV missing required columns should raise 400."""
        csv = b"username,mac_address\nuser1,AA:BB:CC:DD:EE:FF\n"
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "Missing required columns" in exc.value.detail

    def test_headers_only_no_data(self):
        """CSV with headers but zero data rows should raise 400."""
        csv = b"username,mac_address,start_time,usage_time,upload,download\n"
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "no data rows" in exc.value.detail

    def test_invalid_upload_numeric(self):
        """Non-numeric upload values should raise 400."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b"user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,abc,2000.0\n"
        )
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "upload" in exc.value.detail.lower()

    def test_invalid_download_numeric(self):
        """Non-numeric download values should raise 400."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b"user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,xyz\n"
        )
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "download" in exc.value.detail.lower()

    def test_negative_upload(self):
        """Negative upload values should raise 400."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b"user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,-500,2000.0\n"
        )
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "Negative" in exc.value.detail
        assert "upload" in exc.value.detail.lower()

    def test_negative_download(self):
        """Negative download values should raise 400."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b"user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,-100\n"
        )
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "Negative" in exc.value.detail
        assert "download" in exc.value.detail.lower()

    def test_invalid_usage_time(self):
        """Bad usage_time format should raise 400."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b"user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,badtime,1000.0,2000.0\n"
        )
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "usage_time" in exc.value.detail

    def test_invalid_start_time(self):
        """Bad start_time format should raise 400."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b"user1,AA:BB:CC:DD:EE:FF,not-a-date,1:00:00,1000.0,2000.0\n"
        )
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "start_time" in exc.value.detail

    def test_empty_username(self):
        """Empty/blank username should raise 400."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b" ,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "username" in exc.value.detail.lower()

    def test_empty_mac_address(self):
        """Empty/blank mac_address should raise 400."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b"user1, ,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        with pytest.raises(HTTPException) as exc:
            _validate_csv_content(csv)
        assert exc.value.status_code == 400
        assert "mac_address" in exc.value.detail.lower()


# ---------------------------------------------------------------------------
# POST /api/v1/upload (integration via TestClient)
# ---------------------------------------------------------------------------


class TestUploadEndpoint:
    """Integration tests for the upload endpoint via the FastAPI test client."""

    def test_successful_upload(self, client, test_session):
        """Valid CSV upload should return success and ingest records."""
        response = client.post(
            "/api/v1/upload?clear_existing=true&batch_size=100",
            files={"file": ("test.csv", VALID_CSV, "text/csv")},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["records_ingested"] == 1

    def test_successful_upload_no_clear(self, client, test_session, sample_records):
        """Upload with clear_existing=false should append to existing data."""
        from app.models import UsageRecord

        before = test_session.query(UsageRecord).count()

        response = client.post(
            "/api/v1/upload?clear_existing=false&batch_size=100",
            files={"file": ("test.csv", VALID_CSV, "text/csv")},
        )
        assert response.status_code == 200
        assert response.json()["records_ingested"] == 1

        after = test_session.query(UsageRecord).count()
        assert after == before + 1

    def test_upload_wrong_extension(self, client):
        """Uploading a .txt file should return 400."""
        response = client.post(
            "/api/v1/upload",
            files={"file": ("data.txt", b"some text", "text/plain")},
        )
        assert response.status_code == 400
        assert "Only .csv files" in response.json()["detail"]

    def test_upload_missing_columns(self, client):
        """CSV with wrong columns should return 400."""
        bad_csv = b"name,value\na,1\n"
        response = client.post(
            "/api/v1/upload",
            files={"file": ("test.csv", bad_csv, "text/csv")},
        )
        assert response.status_code == 400
        assert "Missing required columns" in response.json()["detail"]

    def test_upload_empty_csv(self, client):
        """Empty CSV (headers only) should return 400."""
        csv = b"username,mac_address,start_time,usage_time,upload,download\n"
        response = client.post(
            "/api/v1/upload",
            files={"file": ("test.csv", csv, "text/csv")},
        )
        assert response.status_code == 400
        assert "no data rows" in response.json()["detail"]

    def test_upload_invalid_numeric(self, client):
        """Non-numeric upload value should return 400."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b"user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,abc,2000.0\n"
        )
        response = client.post(
            "/api/v1/upload",
            files={"file": ("test.csv", csv, "text/csv")},
        )
        assert response.status_code == 400

    def test_upload_value_error_from_ingestion(self, client):
        """ValueError during ingestion should return 400."""
        with patch(
            "app.routers.upload.ingest_data",
            side_effect=ValueError("bad data"),
        ):
            response = client.post(
                "/api/v1/upload",
                files={"file": ("test.csv", VALID_CSV, "text/csv")},
            )
            assert response.status_code == 400
            assert "Data error" in response.json()["detail"]

    def test_upload_unexpected_error_from_ingestion(self, client):
        """Unexpected exception during ingestion should return 500."""
        with patch(
            "app.routers.upload.ingest_data",
            side_effect=RuntimeError("disk full"),
        ):
            response = client.post(
                "/api/v1/upload",
                files={"file": ("test.csv", VALID_CSV, "text/csv")},
            )
            assert response.status_code == 500
            assert "Ingestion failed" in response.json()["detail"]

    def test_upload_multi_row_csv(self, client):
        """Multi-row valid CSV should ingest all records."""
        csv = (
            b"username,mac_address,start_time,usage_time,upload,download\n"
            b"user1,AA:BB:CC:DD:EE:01,2022-12-01 10:00:00,1:00:00,100,200\n"
            b"user2,AA:BB:CC:DD:EE:02,2022-12-02 11:00:00,2:00:00,300,400\n"
            b"user3,AA:BB:CC:DD:EE:03,2022-12-03 12:00:00,3:00:00,500,600\n"
        )
        response = client.post(
            "/api/v1/upload?clear_existing=true&batch_size=100",
            files={"file": ("data.csv", csv, "text/csv")},
        )
        assert response.status_code == 200
        assert response.json()["records_ingested"] == 3
