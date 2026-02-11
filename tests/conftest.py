"""Shared test fixtures for the Internet Usage Monitoring Service tests.

Provides a test database (in-memory SQLite), test session, and a FastAPI
test client with the database dependency overridden.
"""

import os
from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base, get_db
from app.main import app
from app.models import UsageRecord

# Use in-memory SQLite for tests
TEST_DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture()
def test_engine():
    """Create a test database engine with in-memory SQLite.

    Uses StaticPool so a single connection is shared across threads,
    which is required because TestClient dispatches requests in a
    separate thread while the test runs on the main thread.
    """
    engine = create_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture()
def test_session(test_engine):
    """Create a test database session."""
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    session = TestSession()
    yield session
    session.close()


@pytest.fixture()
def client(test_session):
    """Create a FastAPI test client with the test database injected."""

    def override_get_db():
        yield test_session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture()
def sample_records(test_session):
    """Insert sample usage records into the test database.

    Creates data for 3 users across a date range (2022-11-15 to 2022-12-15)
    to allow testing of 1-day, 7-day, and 30-day aggregation windows.

    Users:
        - heavyUser1: High usage, multiple sessions
        - mediumUser2: Medium usage
        - lightUser3: Low usage, fewer sessions
    """
    records = [
        # heavyUser1 - high usage across the full range
        UsageRecord(
            username="heavyUser1",
            mac_address="AA:BB:CC:DD:EE:01",
            start_time=datetime(2022, 12, 15, 10, 0, 0),
            usage_time_seconds=3600,
            upload_kb=5000000.0,
            download_kb=8000000.0,
            total_kb=13000000.0,
        ),
        UsageRecord(
            username="heavyUser1",
            mac_address="AA:BB:CC:DD:EE:01",
            start_time=datetime(2022, 12, 14, 14, 0, 0),
            usage_time_seconds=7200,
            upload_kb=3000000.0,
            download_kb=5000000.0,
            total_kb=8000000.0,
        ),
        UsageRecord(
            username="heavyUser1",
            mac_address="AA:BB:CC:DD:EE:01",
            start_time=datetime(2022, 12, 10, 8, 0, 0),
            usage_time_seconds=5400,
            upload_kb=2000000.0,
            download_kb=3000000.0,
            total_kb=5000000.0,
        ),
        UsageRecord(
            username="heavyUser1",
            mac_address="AA:BB:CC:DD:EE:01",
            start_time=datetime(2022, 11, 20, 12, 0, 0),
            usage_time_seconds=10800,
            upload_kb=4000000.0,
            download_kb=6000000.0,
            total_kb=10000000.0,
        ),
        # mediumUser2 - medium usage
        UsageRecord(
            username="mediumUser2",
            mac_address="AA:BB:CC:DD:EE:02",
            start_time=datetime(2022, 12, 15, 9, 0, 0),
            usage_time_seconds=1800,
            upload_kb=1000000.0,
            download_kb=2000000.0,
            total_kb=3000000.0,
        ),
        UsageRecord(
            username="mediumUser2",
            mac_address="AA:BB:CC:DD:EE:02",
            start_time=datetime(2022, 12, 12, 16, 0, 0),
            usage_time_seconds=3600,
            upload_kb=1500000.0,
            download_kb=2500000.0,
            total_kb=4000000.0,
        ),
        UsageRecord(
            username="mediumUser2",
            mac_address="AA:BB:CC:DD:EE:02",
            start_time=datetime(2022, 11, 25, 20, 0, 0),
            usage_time_seconds=5400,
            upload_kb=2000000.0,
            download_kb=3000000.0,
            total_kb=5000000.0,
        ),
        # lightUser3 - low usage, only old records
        UsageRecord(
            username="lightUser3",
            mac_address="AA:BB:CC:DD:EE:03",
            start_time=datetime(2022, 11, 18, 11, 0, 0),
            usage_time_seconds=900,
            upload_kb=500000.0,
            download_kb=800000.0,
            total_kb=1300000.0,
        ),
    ]
    test_session.add_all(records)
    test_session.commit()
    return records
