"""Data ingestion service for loading CSV data into the database.

This module provides functions to parse and ingest the internet usage
dataset CSV file into the database. It handles data validation,
transformation, and efficient batch insertion.
"""

import logging
import math
from datetime import datetime

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.models import UsageRecord

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"username", "mac_address", "start_time", "usage_time", "upload", "download"}


def parse_usage_time(time_str: str) -> int:
    """Convert a time duration string (H:MM:SS or HH:MM:SS) to total seconds.

    Args:
        time_str: Duration string in the format "H:MM:SS" or "HH:MM:SS".

    Returns:
        Total number of seconds represented by the duration string.

    Raises:
        ValueError: If the time string format is invalid.
    """
    time_str = time_str.strip()
    parts = time_str.split(":")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid time format '{time_str}': expected H:MM:SS or HH:MM:SS"
        )
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
    except ValueError:
        raise ValueError(
            f"Invalid time format '{time_str}': non-numeric components"
        )
    if minutes < 0 or minutes > 59 or seconds < 0 or seconds > 59 or hours < 0:
        raise ValueError(
            f"Invalid time format '{time_str}': values out of range"
        )
    return hours * 3600 + minutes * 60 + seconds


def validate_dataframe(df: pd.DataFrame) -> None:
    """Validate that the DataFrame has all required columns.

    Args:
        df: The pandas DataFrame to validate.

    Raises:
        ValueError: If required columns are missing.
    """
    df.columns = df.columns.str.strip()
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def ingest_data(
    csv_path: str,
    session: Session,
    batch_size: int = 5000,
    clear_existing: bool = True,
) -> int:
    """Ingest CSV data into the database.

    Reads the CSV file, validates its structure, transforms the data,
    and inserts records into the database in batches for efficiency.

    Args:
        csv_path: Path to the CSV file to ingest.
        session: SQLAlchemy database session.
        batch_size: Number of records to insert per batch (default: 5000).
        clear_existing: Whether to clear existing records before ingestion.

    Returns:
        The number of records successfully ingested.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
        ValueError: If the CSV file has invalid structure or data.
    """
    logger.info("Reading CSV file: %s", csv_path)
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()

    validate_dataframe(df)

    if df.empty:
        logger.warning("CSV file is empty (no data rows): %s", csv_path)
        return 0

    # Transform data
    df["username"] = df["username"].astype(str).str.strip()
    df["mac_address"] = df["mac_address"].astype(str).str.strip()
    df["start_time"] = pd.to_datetime(df["start_time"].str.strip())
    df["usage_time_seconds"] = df["usage_time"].apply(parse_usage_time)
    df["upload_kb"] = pd.to_numeric(df["upload"], errors="coerce")
    df["download_kb"] = pd.to_numeric(df["download"], errors="coerce")

    # Validate numeric columns have no NaN values
    if df["upload_kb"].isna().any() or df["download_kb"].isna().any():
        raise ValueError("Invalid numeric values found in upload or download columns")

    df["total_kb"] = df["upload_kb"] + df["download_kb"]

    if clear_existing:
        logger.info("Clearing existing records from the database")
        session.execute(delete(UsageRecord))
        session.commit()

    # Prepare records for bulk insert
    records = df[
        [
            "username",
            "mac_address",
            "start_time",
            "usage_time_seconds",
            "upload_kb",
            "download_kb",
            "total_kb",
        ]
    ].to_dict("records")

    # Convert start_time from pandas Timestamp to Python datetime
    for record in records:
        if isinstance(record["start_time"], pd.Timestamp):
            record["start_time"] = record["start_time"].to_pydatetime()

    # Batch insert for efficiency
    total_batches = math.ceil(len(records) / batch_size)
    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        session.bulk_insert_mappings(UsageRecord, batch)
        session.commit()
        total_inserted += len(batch)
        batch_num = (i // batch_size) + 1
        logger.info(
            "Inserted batch %d/%d (%d records)", batch_num, total_batches, len(batch)
        )

    logger.info("Ingestion complete. Total records inserted: %d", total_inserted)
    return total_inserted
