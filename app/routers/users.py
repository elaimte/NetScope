"""User API endpoints for internet usage analytics.

Provides endpoints for:
- Listing top users by internet usage (paginated)
- Getting detailed usage information for a specific user
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.schemas import ErrorResponse, TopUsersResponse, UserDetailsResponse
from app.services.usage_service import get_top_users, get_user_details

router = APIRouter(prefix="/api/v1/users", tags=["Users"])


@router.get(
    "/top",
    response_model=TopUsersResponse,
    summary="Get top users by internet usage",
    description=(
        "Returns a paginated list of users ranked by their total internet usage "
        "(upload + download) in the last 30 days relative to the reference date. "
        "Each user entry includes usage breakdowns for 1-day, 7-day, and 30-day periods."
    ),
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request parameters"},
    },
)
def list_top_users(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    per_page: int = Query(
        None,
        ge=1,
        le=100,
        description=(
            f"Results per page (1-{settings.MAX_PAGE_SIZE}, "
            f"default: {settings.DEFAULT_PAGE_SIZE})"
        ),
    ),
    reference_date: Optional[str] = Query(
        None,
        description=(
            "End date for the 30-day analysis window (ISO format: YYYY-MM-DDTHH:MM:SS). "
            "Defaults to the latest record date in the database."
        ),
    ),
    db: Session = Depends(get_db),
) -> TopUsersResponse:
    """List top users by their overall internet usage in the last 30 days."""
    if per_page is None:
        per_page = settings.DEFAULT_PAGE_SIZE

    # Parse or determine reference date
    if reference_date is not None:
        try:
            ref_date = datetime.fromisoformat(reference_date)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid reference_date format: '{reference_date}'. "
                    "Expected ISO format (e.g., 2022-12-01T00:00:00)."
                ),
            )
    else:
        # Use the latest record date in the database
        from app.models import UsageRecord
        from sqlalchemy import func

        max_date = db.query(func.max(UsageRecord.start_time)).scalar()
        if max_date is None:
            raise HTTPException(
                status_code=400,
                detail="No data available in the database. Please ingest data first.",
            )
        ref_date = max_date

    return get_top_users(db, ref_date, page, per_page)


@router.get(
    "/details",
    response_model=UserDetailsResponse,
    summary="Get user usage details",
    description=(
        "Search for a user by their exact name and return their internet usage "
        "consumption details for 1-day, 7-day, and 30-day periods relative to "
        "the provided timestamp."
    ),
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request parameters"},
        404: {"model": ErrorResponse, "description": "User not found"},
    },
)
def user_details(
    username: str = Query(
        ..., min_length=1, description="Exact username to search for"
    ),
    timestamp: str = Query(
        ...,
        description=(
            "Reference timestamp for usage calculation "
            "(ISO format: YYYY-MM-DDTHH:MM:SS)"
        ),
    ),
    db: Session = Depends(get_db),
) -> UserDetailsResponse:
    """Get a user's internet usage details relative to a given timestamp."""
    try:
        ts = datetime.fromisoformat(timestamp)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid timestamp format: '{timestamp}'. "
                "Expected ISO format (e.g., 2022-12-01T00:00:00)."
            ),
        )

    result = get_user_details(db, username, ts)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"User '{username}' not found.",
        )

    return result
