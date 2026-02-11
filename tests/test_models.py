"""Tests for SQLAlchemy ORM models."""

from datetime import datetime

from app.models import UsageRecord


class TestUsageRecord:
    """Tests for the UsageRecord ORM model."""

    def test_repr(self):
        """__repr__ should include username, start_time, and total_kb."""
        record = UsageRecord(
            username="testUser",
            mac_address="AA:BB:CC:DD:EE:FF",
            start_time=datetime(2022, 12, 1, 10, 0, 0),
            usage_time_seconds=3600,
            upload_kb=1000.0,
            download_kb=2000.0,
            total_kb=3000.0,
        )
        result = repr(record)
        assert "testUser" in result
        assert "3000.0" in result

    def test_table_name(self):
        """Table name should be 'usage_records'."""
        assert UsageRecord.__tablename__ == "usage_records"

    def test_column_attributes(self):
        """UsageRecord should have all expected column attributes."""
        record = UsageRecord(
            username="user1",
            mac_address="11:22:33:44:55:66",
            start_time=datetime(2022, 12, 1),
            usage_time_seconds=7200,
            upload_kb=500.0,
            download_kb=1500.0,
            total_kb=2000.0,
        )
        assert record.username == "user1"
        assert record.mac_address == "11:22:33:44:55:66"
        assert record.start_time == datetime(2022, 12, 1)
        assert record.usage_time_seconds == 7200
        assert record.upload_kb == 500.0
        assert record.download_kb == 1500.0
        assert record.total_kb == 2000.0
