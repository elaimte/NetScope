"""Pydantic schemas for API request validation and response serialization."""

from typing import List

from pydantic import BaseModel, Field


class UsagePeriod(BaseModel):
    """Internet usage statistics for a specific time period."""

    upload_kb: float = Field(..., description="Upload data in Kilobits")
    download_kb: float = Field(..., description="Download data in Kilobits")
    total_kb: float = Field(..., description="Total usage (upload + download) in Kilobits")
    sessions: int = Field(..., description="Number of usage sessions in this period")


class TopUserEntry(BaseModel):
    """A single user entry in the top users listing."""

    rank: int = Field(..., description="User rank based on 30-day total usage")
    username: str = Field(..., description="Username")
    usage_1_day: UsagePeriod = Field(..., description="Usage in the last 1 day")
    usage_7_days: UsagePeriod = Field(..., description="Usage in the last 7 days")
    usage_30_days: UsagePeriod = Field(..., description="Usage in the last 30 days")


class TopUsersResponse(BaseModel):
    """Paginated response for the top users endpoint."""

    page: int = Field(..., description="Current page number")
    per_page: int = Field(..., description="Number of results per page")
    total_users: int = Field(..., description="Total number of users with activity")
    total_pages: int = Field(..., description="Total number of pages")
    reference_date: str = Field(
        ..., description="Reference date for the 30-day window (ISO format)"
    )
    data: List[TopUserEntry] = Field(..., description="List of top users")


class UserDetailsResponse(BaseModel):
    """Response for the user details endpoint."""

    username: str = Field(..., description="Username")
    timestamp: str = Field(
        ..., description="Reference timestamp for usage calculation (ISO format)"
    )
    usage_1_day: UsagePeriod = Field(
        ..., description="Usage in the 1 day before the timestamp"
    )
    usage_7_days: UsagePeriod = Field(
        ..., description="Usage in the 7 days before the timestamp"
    )
    usage_30_days: UsagePeriod = Field(
        ..., description="Usage in the 30 days before the timestamp"
    )


class ErrorResponse(BaseModel):
    """Standard error response."""

    detail: str = Field(..., description="Error message")
