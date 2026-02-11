"""Tests for the data ingestion service.

Covers parse_usage_time, validate_dataframe, and ingest_data with
all general cases, branches, and edge cases.
"""

import pandas as pd
import pytest

from app.models import UsageRecord
from app.services.ingestion import ingest_data, parse_usage_time, validate_dataframe


# ---------------------------------------------------------------------------
# parse_usage_time
# ---------------------------------------------------------------------------


class TestParseUsageTime:
    """Tests for the parse_usage_time helper function."""

    def test_valid_single_digit_hours(self):
        """Single-digit hour should parse correctly."""
        assert parse_usage_time("1:30:45") == 1 * 3600 + 30 * 60 + 45

    def test_valid_double_digit_hours(self):
        """Double-digit hour should parse correctly."""
        assert parse_usage_time("12:05:09") == 12 * 3600 + 5 * 60 + 9

    def test_valid_zero_duration(self):
        """Zero duration should return 0."""
        assert parse_usage_time("0:00:00") == 0

    def test_valid_large_hours(self):
        """Hours greater than 24 should be accepted."""
        assert parse_usage_time("100:00:00") == 100 * 3600

    def test_valid_max_minutes_and_seconds(self):
        """59 minutes and 59 seconds should be valid."""
        assert parse_usage_time("0:59:59") == 59 * 60 + 59

    def test_strips_whitespace(self):
        """Leading/trailing whitespace should be stripped."""
        assert parse_usage_time("  1:30:45  ") == 1 * 3600 + 30 * 60 + 45

    def test_too_few_parts_raises_value_error(self):
        """Time strings with fewer than 3 colon-separated parts should raise."""
        with pytest.raises(ValueError, match="expected H:MM:SS"):
            parse_usage_time("1:30")

    def test_too_many_parts_raises_value_error(self):
        """Time strings with more than 3 colon-separated parts should raise."""
        with pytest.raises(ValueError, match="expected H:MM:SS"):
            parse_usage_time("1:30:45:00")

    def test_non_numeric_components_raises_value_error(self):
        """Non-numeric hour/minute/second components should raise."""
        with pytest.raises(ValueError, match="non-numeric"):
            parse_usage_time("abc:30:45")

    def test_minutes_above_59_raises_value_error(self):
        """Minutes > 59 should raise."""
        with pytest.raises(ValueError, match="out of range"):
            parse_usage_time("1:60:00")

    def test_seconds_above_59_raises_value_error(self):
        """Seconds > 59 should raise."""
        with pytest.raises(ValueError, match="out of range"):
            parse_usage_time("1:00:60")

    def test_negative_hours_raises_value_error(self):
        """Negative hours should raise."""
        with pytest.raises(ValueError, match="out of range"):
            parse_usage_time("-1:00:00")

    def test_negative_minutes_raises_value_error(self):
        """Negative minutes should raise."""
        with pytest.raises(ValueError, match="out of range"):
            parse_usage_time("1:-1:00")

    def test_negative_seconds_raises_value_error(self):
        """Negative seconds should raise."""
        with pytest.raises(ValueError, match="out of range"):
            parse_usage_time("1:00:-1")

    def test_empty_string_raises_value_error(self):
        """Empty/blank string should raise (too few parts after split)."""
        with pytest.raises(ValueError):
            parse_usage_time("")


# ---------------------------------------------------------------------------
# validate_dataframe
# ---------------------------------------------------------------------------


class TestValidateDataframe:
    """Tests for the validate_dataframe function."""

    def test_valid_dataframe(self):
        """DataFrame with all required columns should not raise."""
        df = pd.DataFrame(
            {
                "username": ["u1"],
                "mac_address": ["AA:BB:CC:DD:EE:FF"],
                "start_time": ["2022-12-01"],
                "usage_time": ["1:00:00"],
                "upload": [1000],
                "download": [2000],
            }
        )
        validate_dataframe(df)  # should not raise

    def test_missing_columns_raises_value_error(self):
        """DataFrame missing required columns should raise ValueError."""
        df = pd.DataFrame({"username": ["u1"], "mac_address": ["AA:BB:CC:DD:EE:FF"]})
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_dataframe(df)

    def test_strips_column_whitespace(self):
        """Column names with leading/trailing spaces should be stripped."""
        df = pd.DataFrame(
            {
                " username": ["u1"],
                " mac_address ": ["AA:BB:CC:DD:EE:FF"],
                "start_time ": ["2022-12-01"],
                "usage_time": ["1:00:00"],
                " upload": [1000],
                "download ": [2000],
            }
        )
        validate_dataframe(df)  # should not raise after stripping


# ---------------------------------------------------------------------------
# ingest_data
# ---------------------------------------------------------------------------


class TestIngestData:
    """Tests for the ingest_data function (end-to-end ingestion)."""

    def test_successful_ingestion(self, test_session, tmp_path):
        """Valid CSV should be ingested into the database."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:30:00,1000.0,2000.0\n"
            "user2,BB:CC:DD:EE:FF:00,2022-12-02 11:00:00,2:00:00,3000.0,4000.0\n"
        )
        count = ingest_data(str(csv_file), test_session, batch_size=10)
        assert count == 2

        records = test_session.query(UsageRecord).all()
        assert len(records) == 2

    def test_total_kb_and_usage_time_calculation(self, test_session, tmp_path):
        """Total KB should be upload+download; usage_time should be in seconds."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:30:45,1000.5,2000.5\n"
        )
        ingest_data(str(csv_file), test_session)

        record = test_session.query(UsageRecord).first()
        assert record.total_kb == pytest.approx(3001.0)
        assert record.usage_time_seconds == 1 * 3600 + 30 * 60 + 45

    def test_empty_csv_returns_zero(self, test_session, tmp_path):
        """CSV with headers but no data rows should return 0."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
        )
        count = ingest_data(str(csv_file), test_session)
        assert count == 0

    def test_clear_existing_true_removes_old_data(self, test_session, tmp_path, sample_records):
        """clear_existing=True should delete old records before inserting new ones."""
        old_count = test_session.query(UsageRecord).count()
        assert old_count > 0

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "newUser,CC:DD:EE:FF:00:11,2022-12-01 10:00:00,1:00:00,500.0,500.0\n"
        )
        count = ingest_data(str(csv_file), test_session, clear_existing=True)
        assert count == 1

        records = test_session.query(UsageRecord).all()
        assert len(records) == 1
        assert records[0].username == "newUser"

    def test_clear_existing_false_keeps_old_data(self, test_session, tmp_path, sample_records):
        """clear_existing=False should keep old records and add new ones."""
        existing_count = test_session.query(UsageRecord).count()

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "newUser,CC:DD:EE:FF:00:11,2022-12-01 10:00:00,1:00:00,500.0,500.0\n"
        )
        count = ingest_data(str(csv_file), test_session, clear_existing=False)
        assert count == 1

        total = test_session.query(UsageRecord).count()
        assert total == existing_count + 1

    def test_invalid_numeric_upload_raises_value_error(self, test_session, tmp_path):
        """Non-numeric upload values should raise ValueError."""
        csv_file = tmp_path / "bad.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,abc,2000.0\n"
        )
        with pytest.raises(ValueError, match="Invalid numeric values"):
            ingest_data(str(csv_file), test_session)

    def test_invalid_numeric_download_raises_value_error(self, test_session, tmp_path):
        """Non-numeric download values should raise ValueError."""
        csv_file = tmp_path / "bad.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,xyz\n"
        )
        with pytest.raises(ValueError, match="Invalid numeric values"):
            ingest_data(str(csv_file), test_session)

    def test_batch_insertion_with_small_batch_size(self, test_session, tmp_path):
        """Multiple batches should all be inserted correctly."""
        lines = ["username,mac_address,start_time,usage_time,upload,download"]
        for i in range(10):
            lines.append(
                f"user{i},AA:BB:CC:DD:EE:{i:02X},"
                f"2022-12-01 {i:02d}:00:00,1:00:00,{i * 100}.0,{i * 200}.0"
            )
        csv_file = tmp_path / "batch.csv"
        csv_file.write_text("\n".join(lines) + "\n")

        count = ingest_data(str(csv_file), test_session, batch_size=3)
        assert count == 10

        records = test_session.query(UsageRecord).all()
        assert len(records) == 10

    def test_file_not_found_raises(self, test_session):
        """Ingesting a nonexistent file should raise FileNotFoundError."""
        with pytest.raises((FileNotFoundError, OSError)):
            ingest_data("/nonexistent/path/file.csv", test_session)

    def test_missing_columns_in_csv_raises(self, test_session, tmp_path):
        """CSV missing required columns should raise ValueError."""
        csv_file = tmp_path / "bad_columns.csv"
        csv_file.write_text("username,mac_address\nuser1,AA:BB:CC:DD:EE:FF\n")
        with pytest.raises(ValueError, match="Missing required columns"):
            ingest_data(str(csv_file), test_session)

    def test_strips_string_fields(self, test_session, tmp_path):
        """Username and MAC address whitespace should be stripped."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "  spaced_user  ,  AA:BB:CC:DD:EE:FF  ,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        ingest_data(str(csv_file), test_session)
        record = test_session.query(UsageRecord).first()
        assert record.username == "spaced_user"
        assert record.mac_address == "AA:BB:CC:DD:EE:FF"
