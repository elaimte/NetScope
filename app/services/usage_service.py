"""Usage analytics service providing business logic for the API endpoints.

This module contains optimized database queries for computing internet
usage statistics across different time periods. It uses conditional
aggregation in single queries to minimize database round-trips.
"""

import math
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import case, func, text
from sqlalchemy.orm import Session

from app.models import UsageRecord
from app.schemas import (
    TopUserEntry,
    TopUsersResponse,
    UsagePeriod,
    UserDetailsResponse,
)


def _build_usage_aggregation(ref_date: datetime):
    """Build conditional aggregation columns for 1-day, 7-day, and 30-day periods.

    Uses SQL CASE expressions to compute usage for all three periods in a
    single table scan, avoiding multiple round-trips to the database.

    Args:
        ref_date: The reference date (end of the 30-day window).

    Returns:
        Tuple of (date boundaries, aggregation columns list).
    """
    date_1d = ref_date - timedelta(days=1)
    date_7d = ref_date - timedelta(days=7)
    date_30d = ref_date - timedelta(days=30)

    # 1-day aggregations
    upload_1d = func.coalesce(
        func.sum(
            case(
                (UsageRecord.start_time >= date_1d, UsageRecord.upload_kb),
                else_=0,
            )
        ),
        0,
    ).label("upload_1d")

    download_1d = func.coalesce(
        func.sum(
            case(
                (UsageRecord.start_time >= date_1d, UsageRecord.download_kb),
                else_=0,
            )
        ),
        0,
    ).label("download_1d")

    total_1d = func.coalesce(
        func.sum(
            case(
                (UsageRecord.start_time >= date_1d, UsageRecord.total_kb),
                else_=0,
            )
        ),
        0,
    ).label("total_1d")

    sessions_1d = func.sum(
        case(
            (UsageRecord.start_time >= date_1d, 1),
            else_=0,
        )
    ).label("sessions_1d")

    # 7-day aggregations
    upload_7d = func.coalesce(
        func.sum(
            case(
                (UsageRecord.start_time >= date_7d, UsageRecord.upload_kb),
                else_=0,
            )
        ),
        0,
    ).label("upload_7d")

    download_7d = func.coalesce(
        func.sum(
            case(
                (UsageRecord.start_time >= date_7d, UsageRecord.download_kb),
                else_=0,
            )
        ),
        0,
    ).label("download_7d")

    total_7d = func.coalesce(
        func.sum(
            case(
                (UsageRecord.start_time >= date_7d, UsageRecord.total_kb),
                else_=0,
            )
        ),
        0,
    ).label("total_7d")

    sessions_7d = func.sum(
        case(
            (UsageRecord.start_time >= date_7d, 1),
            else_=0,
        )
    ).label("sessions_7d")

    # 30-day aggregations (all records in the filtered window)
    upload_30d = func.coalesce(func.sum(UsageRecord.upload_kb), 0).label("upload_30d")
    download_30d = func.coalesce(func.sum(UsageRecord.download_kb), 0).label(
        "download_30d"
    )
    total_30d = func.coalesce(func.sum(UsageRecord.total_kb), 0).label("total_30d")
    sessions_30d = func.count(UsageRecord.id).label("sessions_30d")

    columns = [
        upload_1d,
        download_1d,
        total_1d,
        sessions_1d,
        upload_7d,
        download_7d,
        total_7d,
        sessions_7d,
        upload_30d,
        download_30d,
        total_30d,
        sessions_30d,
    ]

    return date_30d, columns


def _row_to_usage_periods(row) -> dict:
    """Convert a database result row into UsagePeriod dictionaries.

    Args:
        row: A SQLAlchemy result row with aggregation columns.

    Returns:
        Dictionary with usage_1_day, usage_7_days, and usage_30_days UsagePeriod objects.
    """
    return {
        "usage_1_day": UsagePeriod(
            upload_kb=round(float(row.upload_1d), 2),
            download_kb=round(float(row.download_1d), 2),
            total_kb=round(float(row.total_1d), 2),
            sessions=int(row.sessions_1d),
        ),
        "usage_7_days": UsagePeriod(
            upload_kb=round(float(row.upload_7d), 2),
            download_kb=round(float(row.download_7d), 2),
            total_kb=round(float(row.total_7d), 2),
            sessions=int(row.sessions_7d),
        ),
        "usage_30_days": UsagePeriod(
            upload_kb=round(float(row.upload_30d), 2),
            download_kb=round(float(row.download_30d), 2),
            total_kb=round(float(row.total_30d), 2),
            sessions=int(row.sessions_30d),
        ),
    }


def get_top_users(
    db: Session,
    reference_date: datetime,
    page: int,
    per_page: int,
) -> TopUsersResponse:
    """Get paginated list of top users ranked by total internet usage.

    Retrieves users sorted by their total internet usage (upload + download)
    in the 30-day window ending at reference_date. For each user, usage
    statistics for 1-day, 7-day, and 30-day periods are calculated in
    a single optimized query.

    Args:
        db: Database session.
        reference_date: The end date of the 30-day analysis window.
        page: Page number (1-indexed).
        per_page: Number of results per page.

    Returns:
        TopUsersResponse with paginated user usage data.
    """
    date_30d, agg_columns = _build_usage_aggregation(reference_date)
    offset = (page - 1) * per_page

    # Count total unique users in the 30-day window
    total_users = (
        db.query(func.count(func.distinct(UsageRecord.username)))
        .filter(
            UsageRecord.start_time >= date_30d,
            UsageRecord.start_time <= reference_date,
        )
        .scalar()
    ) or 0

    total_pages = math.ceil(total_users / per_page) if total_users > 0 else 0

    # Fetch aggregated usage data with pagination
    query = (
        db.query(UsageRecord.username, *agg_columns)
        .filter(
            UsageRecord.start_time >= date_30d,
            UsageRecord.start_time <= reference_date,
        )
        .group_by(UsageRecord.username)
        .order_by(text("total_30d DESC"))
        .offset(offset)
        .limit(per_page)
    )

    results = query.all()

    # Build response entries
    data = []
    for idx, row in enumerate(results):
        periods = _row_to_usage_periods(row)
        entry = TopUserEntry(
            rank=offset + idx + 1,
            username=row.username,
            **periods,
        )
        data.append(entry)

    return TopUsersResponse(
        page=page,
        per_page=per_page,
        total_users=total_users,
        total_pages=total_pages,
        reference_date=reference_date.isoformat(),
        data=data,
    )


def get_user_details(
    db: Session,
    username: str,
    timestamp: datetime,
) -> Optional[UserDetailsResponse]:
    """Get detailed internet usage for a specific user relative to a timestamp.

    Searches for the user by exact name and computes their usage statistics
    for 1-day, 7-day, and 30-day periods ending at the provided timestamp.

    Args:
        db: Database session.
        username: Exact username to search for.
        timestamp: Reference timestamp for the analysis window.

    Returns:
        UserDetailsResponse with usage details, or None if user is not found.
    """
    # First, verify the user exists in the database
    user_exists = (
        db.query(UsageRecord.username)
        .filter(UsageRecord.username == username)
        .first()
    )
    if user_exists is None:
        return None

    date_30d, agg_columns = _build_usage_aggregation(timestamp)

    # Fetch aggregated usage for the specific user
    row = (
        db.query(UsageRecord.username, *agg_columns)
        .filter(
            UsageRecord.username == username,
            UsageRecord.start_time >= date_30d,
            UsageRecord.start_time <= timestamp,
        )
        .group_by(UsageRecord.username)
        .first()
    )

    # User exists but has no data in the given time window
    if row is None:
        empty_period = UsagePeriod(
            upload_kb=0.0, download_kb=0.0, total_kb=0.0, sessions=0
        )
        return UserDetailsResponse(
            username=username,
            timestamp=timestamp.isoformat(),
            usage_1_day=empty_period,
            usage_7_days=empty_period,
            usage_30_days=empty_period,
        )

    periods = _row_to_usage_periods(row)
    return UserDetailsResponse(
        username=username,
        timestamp=timestamp.isoformat(),
        **periods,
    )
