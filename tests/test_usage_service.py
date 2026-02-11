"""Tests for the usage analytics service layer.

Covers get_top_users and get_user_details with normal flows,
edge cases, empty data, and pagination.
"""

from datetime import datetime

from app.services.usage_service import get_top_users, get_user_details


# Reference date that captures all sample_records data
REF_DATE = datetime(2022, 12, 15, 23, 59, 59)


class TestGetTopUsers:
    """Tests for the get_top_users service function."""

    def test_empty_database(self, test_session):
        """No records should yield empty results with zero counts."""
        result = get_top_users(test_session, REF_DATE, page=1, per_page=10)
        assert result.total_users == 0
        assert result.total_pages == 0
        assert result.data == []
        assert result.page == 1
        assert result.per_page == 10

    def test_with_data_returns_ranked_users(self, test_session, sample_records):
        """Users should be returned ranked by 30-day total usage descending."""
        result = get_top_users(test_session, REF_DATE, page=1, per_page=10)
        assert result.total_users == 3
        assert len(result.data) == 3

        # heavyUser1 should be rank 1 (highest usage)
        assert result.data[0].username == "heavyUser1"
        assert result.data[0].rank == 1

        # mediumUser2 should be rank 2
        assert result.data[1].username == "mediumUser2"
        assert result.data[1].rank == 2

        # lightUser3 should be rank 3
        assert result.data[2].username == "lightUser3"
        assert result.data[2].rank == 3

    def test_descending_total_order(self, test_session, sample_records):
        """Each user's 30-day total_kb should be >= the next user's."""
        result = get_top_users(test_session, REF_DATE, page=1, per_page=10)
        for i in range(len(result.data) - 1):
            assert (
                result.data[i].usage_30_days.total_kb
                >= result.data[i + 1].usage_30_days.total_kb
            )

    def test_pagination_first_page(self, test_session, sample_records):
        """First page with per_page=1 should return 1 user."""
        result = get_top_users(test_session, REF_DATE, page=1, per_page=1)
        assert result.per_page == 1
        assert len(result.data) == 1
        assert result.data[0].rank == 1
        assert result.total_pages == 3

    def test_pagination_second_page(self, test_session, sample_records):
        """Second page with per_page=1 should return the second-ranked user."""
        result = get_top_users(test_session, REF_DATE, page=2, per_page=1)
        assert result.page == 2
        assert len(result.data) == 1
        assert result.data[0].rank == 2

    def test_pagination_beyond_data(self, test_session, sample_records):
        """Requesting a page beyond available data should return an empty list."""
        result = get_top_users(test_session, REF_DATE, page=100, per_page=10)
        assert result.data == []
        assert result.page == 100

    def test_reference_date_iso_format(self, test_session, sample_records):
        """reference_date in the response should be ISO formatted."""
        result = get_top_users(test_session, REF_DATE, page=1, per_page=10)
        assert result.reference_date == REF_DATE.isoformat()

    def test_usage_period_breakdowns(self, test_session, sample_records):
        """1-day, 7-day, and 30-day periods should have correct session counts."""
        result = get_top_users(test_session, REF_DATE, page=1, per_page=10)
        heavy = result.data[0]  # heavyUser1

        # 30-day: 4 sessions (all records: 12/15, 12/14, 12/10, 11/20)
        assert heavy.usage_30_days.sessions == 4

        # 7-day (>= 12/8 23:59:59): 12/15, 12/14, 12/10 → 3 sessions
        assert heavy.usage_7_days.sessions == 3

        # 1-day (>= 12/14 23:59:59): 12/15 → 1 session
        assert heavy.usage_1_day.sessions == 1

    def test_narrow_reference_date(self, test_session, sample_records):
        """A reference date that excludes some users should reduce total_users."""
        # Only heavyUser1 and mediumUser2 have records on/after 2022-12-10
        narrow_ref = datetime(2022, 12, 15, 23, 59, 59)
        result = get_top_users(test_session, narrow_ref, page=1, per_page=10)
        assert result.total_users == 3  # lightUser3 on 11/18 is within 30-day window


class TestGetUserDetails:
    """Tests for the get_user_details service function."""

    def test_user_not_found(self, test_session):
        """Non-existent user should return None."""
        result = get_user_details(test_session, "nonExistentUser", REF_DATE)
        assert result is None

    def test_user_found_with_data(self, test_session, sample_records):
        """Existing user with activity in the window should return usage data."""
        result = get_user_details(test_session, "heavyUser1", REF_DATE)
        assert result is not None
        assert result.username == "heavyUser1"
        assert result.timestamp == REF_DATE.isoformat()
        assert result.usage_30_days.sessions == 4
        assert result.usage_30_days.total_kb > 0

    def test_user_found_no_data_in_window(self, test_session, sample_records):
        """User with no activity in the given time window should get zero usage."""
        # Use a timestamp before any sample data
        old_ts = datetime(2022, 1, 1, 0, 0, 0)
        result = get_user_details(test_session, "heavyUser1", old_ts)
        assert result is not None
        assert result.username == "heavyUser1"
        assert result.usage_30_days.sessions == 0
        assert result.usage_30_days.total_kb == 0.0
        assert result.usage_7_days.sessions == 0
        assert result.usage_1_day.sessions == 0

    def test_user_detail_period_values(self, test_session, sample_records):
        """Verify upload, download, total, and sessions for each period."""
        result = get_user_details(test_session, "heavyUser1", REF_DATE)

        # 1-day: 12/15 record only → upload=5M, download=8M, total=13M
        assert result.usage_1_day.upload_kb == 5000000.0
        assert result.usage_1_day.download_kb == 8000000.0
        assert result.usage_1_day.total_kb == 13000000.0
        assert result.usage_1_day.sessions == 1

    def test_medium_user_details(self, test_session, sample_records):
        """Verify mediumUser2 details to cross-check aggregation logic."""
        result = get_user_details(test_session, "mediumUser2", REF_DATE)
        assert result is not None
        assert result.usage_30_days.sessions == 3

    def test_light_user_old_records(self, test_session, sample_records):
        """lightUser3 only has old records; 1-day and 7-day should be zero."""
        result = get_user_details(test_session, "lightUser3", REF_DATE)
        assert result is not None
        assert result.usage_1_day.sessions == 0
        assert result.usage_7_days.sessions == 0
        assert result.usage_30_days.sessions == 1
