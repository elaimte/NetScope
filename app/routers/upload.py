"""CSV upload endpoint with comprehensive validation.

Accepts a CSV file upload, validates its format, headers, and data,
then ingests it into the database using the ingestion service.
"""

import io
import logging
import os
import tempfile
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.ingestion import (
    REQUIRED_COLUMNS,
    ingest_data,
    parse_usage_time,
    validate_dataframe,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Upload"])

# Allowed file extensions and MIME types
ALLOWED_EXTENSIONS = {".csv"}
ALLOWED_MIME_TYPES = {
    "text/csv",
    "application/csv",
    "application/vnd.ms-excel",
    "text/plain",
    "application/octet-stream",
}
MAX_FILE_SIZE_MB = 50
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


def _validate_file_metadata(file: UploadFile) -> None:
    """Validate file extension and MIME type.

    Args:
        file: The uploaded file object.

    Raises:
        HTTPException: If the file has an invalid extension or MIME type.
    """
    # Check filename exists
    if not file.filename:
        raise HTTPException(
            status_code=400,
            detail="No filename provided. Please upload a file with a .csv extension.",
        )

    # Check extension
    filename_lower = file.filename.lower()
    if not any(filename_lower.endswith(ext) for ext in ALLOWED_EXTENSIONS):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid file type: '{file.filename}'. "
                "Only .csv files are accepted."
            ),
        )

    # Check MIME type
    if file.content_type and file.content_type not in ALLOWED_MIME_TYPES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid MIME type: '{file.content_type}'. "
                "Expected a CSV file (text/csv)."
            ),
        )


def _validate_csv_content(contents: bytes) -> pd.DataFrame:
    """Parse and validate the CSV file contents.

    Validates:
    - File is not empty
    - File is valid UTF-8 text
    - File is parseable as CSV
    - All required columns are present
    - No empty rows (at least 1 data row)
    - Numeric columns contain valid numbers
    - usage_time column has valid H:MM:SS format
    - start_time column has valid datetime format

    Args:
        contents: Raw bytes of the uploaded file.

    Returns:
        A validated pandas DataFrame.

    Raises:
        HTTPException: If any validation check fails.
    """
    # Check file size
    if len(contents) > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Maximum size is {MAX_FILE_SIZE_MB} MB.",
        )

    # Check not empty
    if len(contents) == 0:
        raise HTTPException(
            status_code=400,
            detail="Uploaded file is empty.",
        )

    # Decode UTF-8
    try:
        text = contents.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=400,
            detail="File is not valid UTF-8 text. Please upload a UTF-8 encoded CSV.",
        )

    # Parse CSV
    try:
        df = pd.read_csv(io.StringIO(text))
    except pd.errors.EmptyDataError:
        raise HTTPException(
            status_code=400,
            detail="CSV file contains no data. Please upload a file with headers and data rows.",
        )
    except pd.errors.ParserError as e:
        raise HTTPException(
            status_code=400,
            detail=f"CSV parsing error: {e}. Please check the file format.",
        )

    # Strip column names
    df.columns = df.columns.str.strip()

    # Check required columns
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Missing required columns: {sorted(missing)}. "
                f"Expected columns: {sorted(REQUIRED_COLUMNS)}."
            ),
        )

    # Check at least 1 data row
    if df.empty:
        raise HTTPException(
            status_code=400,
            detail="CSV file has headers but no data rows.",
        )

    # Validate numeric columns
    upload_numeric = pd.to_numeric(df["upload"], errors="coerce")
    download_numeric = pd.to_numeric(df["download"], errors="coerce")

    bad_upload_rows = df[upload_numeric.isna()].index.tolist()
    bad_download_rows = df[download_numeric.isna()].index.tolist()

    if bad_upload_rows:
        rows_preview = bad_upload_rows[:5]
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid numeric values in 'upload' column at row(s): "
                f"{[r + 2 for r in rows_preview]} (1-indexed, including header). "
                "Upload values must be numbers."
            ),
        )

    if bad_download_rows:
        rows_preview = bad_download_rows[:5]
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid numeric values in 'download' column at row(s): "
                f"{[r + 2 for r in rows_preview]} (1-indexed, including header). "
                "Download values must be numbers."
            ),
        )

    # Validate negative values
    if (upload_numeric < 0).any():
        bad_rows = df[upload_numeric < 0].index.tolist()[:5]
        raise HTTPException(
            status_code=400,
            detail=(
                f"Negative values in 'upload' column at row(s): "
                f"{[r + 2 for r in bad_rows]}. Upload values must be non-negative."
            ),
        )

    if (download_numeric < 0).any():
        bad_rows = df[download_numeric < 0].index.tolist()[:5]
        raise HTTPException(
            status_code=400,
            detail=(
                f"Negative values in 'download' column at row(s): "
                f"{[r + 2 for r in bad_rows]}. Download values must be non-negative."
            ),
        )

    # Validate usage_time format
    bad_time_rows = []
    for idx, val in df["usage_time"].items():
        try:
            parse_usage_time(str(val))
        except ValueError:
            bad_time_rows.append(idx)
    if bad_time_rows:
        rows_preview = bad_time_rows[:5]
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid 'usage_time' format at row(s): "
                f"{[r + 2 for r in rows_preview]}. "
                "Expected format: H:MM:SS or HH:MM:SS."
            ),
        )

    # Validate start_time format
    try:
        pd.to_datetime(df["start_time"].str.strip())
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid 'start_time' format. "
                "Expected datetime format (e.g. 2022-12-01 10:00:00)."
            ),
        )

    # Validate username is not empty
    df["username"] = df["username"].astype(str).str.strip()
    empty_users = df[df["username"].isin(["", "nan"])].index.tolist()
    if empty_users:
        rows_preview = empty_users[:5]
        raise HTTPException(
            status_code=400,
            detail=(
                f"Empty username at row(s): "
                f"{[r + 2 for r in rows_preview]}. Username cannot be blank."
            ),
        )

    # Validate mac_address is not empty
    df["mac_address"] = df["mac_address"].astype(str).str.strip()
    empty_macs = df[df["mac_address"].isin(["", "nan"])].index.tolist()
    if empty_macs:
        rows_preview = empty_macs[:5]
        raise HTTPException(
            status_code=400,
            detail=(
                f"Empty mac_address at row(s): "
                f"{[r + 2 for r in rows_preview]}. MAC address cannot be blank."
            ),
        )

    return df


@router.post(
    "/upload",
    summary="Upload a CSV dataset",
    description=(
        "Upload a CSV file containing internet usage data. "
        "The file is validated for format, column headers, data types, "
        "and value constraints before being ingested into the database."
    ),
    responses={
        400: {"description": "Validation error (bad file format, missing columns, etc.)"},
    },
)
async def upload_csv(
    file: UploadFile = File(..., description="CSV file to upload"),
    clear_existing: bool = Query(
        True,
        description="Clear existing data before importing (default: true)",
    ),
    batch_size: int = Query(
        5000,
        ge=100,
        le=50000,
        description="Records per batch insert (default: 5000)",
    ),
    db: Session = Depends(get_db),
):
    """Upload and ingest a CSV dataset into the database.

    Performs comprehensive validation before ingestion:
    - File must be .csv format
    - Must contain required columns: username, mac_address, start_time,
      usage_time, upload, download
    - Numeric columns must contain valid non-negative numbers
    - usage_time must be in H:MM:SS or HH:MM:SS format
    - start_time must be valid datetime
    - username and mac_address cannot be empty
    """
    # Step 1: Validate file metadata
    _validate_file_metadata(file)

    # Step 2: Read file contents
    contents = await file.read()

    # Step 3: Validate CSV content
    _validate_csv_content(contents)

    # Step 4: Write to a temp file and ingest using existing service
    tmp_file = None
    try:
        tmp_file = tempfile.NamedTemporaryFile(
            mode="wb", suffix=".csv", delete=False
        )
        tmp_file.write(contents)
        tmp_file.close()

        count = ingest_data(
            csv_path=tmp_file.name,
            session=db,
            batch_size=batch_size,
            clear_existing=clear_existing,
        )

        logger.info("CSV upload ingested %d records", count)

        return {
            "status": "success",
            "message": f"Successfully ingested {count:,} records.",
            "records_ingested": count,
            "clear_existing": clear_existing,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Data error: {e}")
    except Exception as e:
        logger.error("Upload ingestion failed: %s", e)
        raise HTTPException(
            status_code=500,
            detail=f"Ingestion failed: {e}",
        )
    finally:
        if tmp_file and os.path.exists(tmp_file.name):
            os.unlink(tmp_file.name)
