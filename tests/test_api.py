"""Tests for all HTTP API endpoints.

Covers health check, top-users listing, and user-details endpoints
with all request variations, error cases, and edge cases.
"""


class TestHealthCheck:
    """Tests for the GET / health check endpoint."""

    def test_returns_healthy_status(self, client):
        """Health check should return status 'healthy'."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "service" in data


class TestListTopUsersEndpoint:
    """Tests for GET /api/v1/users/top."""

    def test_no_data_no_reference_date_returns_400(self, client):
        """Empty database with no reference_date should return 400."""
        response = client.get("/api/v1/users/top")
        assert response.status_code == 400
        assert "No data available" in response.json()["detail"]

    def test_default_params_with_data(self, client, sample_records):
        """Default parameters (no page, no per_page) should return first page."""
        response = client.get("/api/v1/users/top")
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 1
        assert data["per_page"] == 10  # default
        assert len(data["data"]) == 3  # all 3 sample users

    def test_custom_pagination(self, client, sample_records):
        """Custom page and per_page should be respected."""
        response = client.get("/api/v1/users/top?page=1&per_page=2")
        assert response.status_code == 200
        data = response.json()
        assert data["per_page"] == 2
        assert len(data["data"]) == 2

    def test_explicit_per_page(self, client, sample_records):
        """Explicitly setting per_page should override the default."""
        response = client.get("/api/v1/users/top?per_page=1")
        assert response.status_code == 200
        assert response.json()["per_page"] == 1
        assert len(response.json()["data"]) == 1

    def test_with_valid_reference_date(self, client, sample_records):
        """Providing a valid reference_date should filter data accordingly."""
        response = client.get(
            "/api/v1/users/top?reference_date=2022-12-15T23:59:59"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["reference_date"] == "2022-12-15T23:59:59"
        assert len(data["data"]) > 0

    def test_invalid_reference_date_returns_400(self, client):
        """Invalid reference_date format should return 400."""
        response = client.get("/api/v1/users/top?reference_date=not-a-date")
        assert response.status_code == 400
        assert "Invalid reference_date format" in response.json()["detail"]

    def test_page_beyond_data_returns_empty(self, client, sample_records):
        """Requesting a page beyond available data should return empty list."""
        response = client.get("/api/v1/users/top?page=100&per_page=10")
        assert response.status_code == 200
        assert len(response.json()["data"]) == 0

    def test_ranking_order(self, client, sample_records):
        """Users should be ordered by descending 30-day total usage."""
        response = client.get(
            "/api/v1/users/top?reference_date=2022-12-15T23:59:59&per_page=10"
        )
        assert response.status_code == 200
        data = response.json()["data"]
        for i in range(len(data) - 1):
            assert (
                data[i]["usage_30_days"]["total_kb"]
                >= data[i + 1]["usage_30_days"]["total_kb"]
            )

    def test_response_structure(self, client, sample_records):
        """Response should contain all required top-level and nested fields."""
        response = client.get("/api/v1/users/top")
        assert response.status_code == 200
        data = response.json()

        # Top-level fields
        assert "page" in data
        assert "per_page" in data
        assert "total_users" in data
        assert "total_pages" in data
        assert "reference_date" in data
        assert "data" in data

        # Entry fields
        entry = data["data"][0]
        assert "rank" in entry
        assert "username" in entry
        assert "usage_1_day" in entry
        assert "usage_7_days" in entry
        assert "usage_30_days" in entry

        # Period fields
        period = entry["usage_30_days"]
        assert "upload_kb" in period
        assert "download_kb" in period
        assert "total_kb" in period
        assert "sessions" in period

    def test_second_page_ranks(self, client, sample_records):
        """Second page ranks should continue from first page."""
        response = client.get(
            "/api/v1/users/top?page=2&per_page=1&reference_date=2022-12-15T23:59:59"
        )
        assert response.status_code == 200
        data = response.json()
        if data["data"]:
            assert data["data"][0]["rank"] == 2


class TestUserDetailsEndpoint:
    """Tests for GET /api/v1/users/details."""

    def test_valid_user_and_timestamp(self, client, sample_records):
        """Valid user with data should return usage details."""
        response = client.get(
            "/api/v1/users/details?username=heavyUser1&timestamp=2022-12-15T23:59:59"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "heavyUser1"
        assert data["timestamp"] == "2022-12-15T23:59:59"
        assert data["usage_30_days"]["sessions"] > 0

    def test_invalid_timestamp_returns_400(self, client):
        """Invalid timestamp format should return 400."""
        response = client.get(
            "/api/v1/users/details?username=test&timestamp=not-a-date"
        )
        assert response.status_code == 400
        assert "Invalid timestamp format" in response.json()["detail"]

    def test_nonexistent_user_returns_404(self, client, sample_records):
        """Non-existent user should return 404."""
        response = client.get(
            "/api/v1/users/details"
            "?username=nonExistentUser&timestamp=2022-12-15T23:59:59"
        )
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]

    def test_user_exists_no_data_in_window(self, client, sample_records):
        """User with no activity in the time window should return zero usage."""
        response = client.get(
            "/api/v1/users/details"
            "?username=heavyUser1&timestamp=2022-01-01T00:00:00"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "heavyUser1"
        assert data["usage_30_days"]["sessions"] == 0
        assert data["usage_30_days"]["total_kb"] == 0.0
        assert data["usage_7_days"]["sessions"] == 0
        assert data["usage_1_day"]["sessions"] == 0

    def test_response_structure(self, client, sample_records):
        """Response should contain all required fields."""
        response = client.get(
            "/api/v1/users/details?username=heavyUser1&timestamp=2022-12-15T23:59:59"
        )
        assert response.status_code == 200
        data = response.json()

        assert "username" in data
        assert "timestamp" in data
        assert "usage_1_day" in data
        assert "usage_7_days" in data
        assert "usage_30_days" in data

        for period_key in ("usage_1_day", "usage_7_days", "usage_30_days"):
            period = data[period_key]
            assert "upload_kb" in period
            assert "download_kb" in period
            assert "total_kb" in period
            assert "sessions" in period

    def test_different_users(self, client, sample_records):
        """Different users should return different results."""
        r1 = client.get(
            "/api/v1/users/details?username=heavyUser1&timestamp=2022-12-15T23:59:59"
        )
        r2 = client.get(
            "/api/v1/users/details?username=lightUser3&timestamp=2022-12-15T23:59:59"
        )
        assert r1.status_code == 200
        assert r2.status_code == 200
        assert (
            r1.json()["usage_30_days"]["total_kb"]
            > r2.json()["usage_30_days"]["total_kb"]
        )
