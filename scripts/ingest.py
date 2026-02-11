"""CLI script to ingest internet usage data from a CSV file into the database.

Usage:
    python scripts/ingest.py --csv dataset.csv [--batch-size 5000] [--database-url sqlite:///./internet_usage.db]

This script reads the provided CSV dataset and loads it into the configured
database. It creates the necessary tables if they don't exist and supports
batch insertion for efficient data loading.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Ensure project root is in the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.services.ingestion import ingest_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main(args=None):
    """Main entry point for the ingestion CLI script.

    Args:
        args: Command-line arguments (defaults to sys.argv if None).

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    parser = argparse.ArgumentParser(
        description="Ingest internet usage data from CSV into the database"
    )
    parser.add_argument(
        "--csv",
        type=str,
        required=True,
        help="Path to the CSV file to ingest",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Number of records per batch insert (default: 5000)",
    )
    parser.add_argument(
        "--database-url",
        type=str,
        default=None,
        help="Database URL (default: uses DATABASE_URL env var or sqlite:///./internet_usage.db)",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear existing data before ingestion",
    )

    parsed_args = parser.parse_args(args)

    # Resolve CSV path
    csv_path = Path(parsed_args.csv)
    if not csv_path.exists():
        logger.error("CSV file not found: %s", csv_path)
        return 1

    # Set up database connection
    if parsed_args.database_url:
        database_url = parsed_args.database_url
    else:
        from app.config import settings
        database_url = settings.DATABASE_URL

    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    db_engine = create_engine(database_url, connect_args=connect_args)
    Base.metadata.create_all(bind=db_engine)
    Session = sessionmaker(bind=db_engine)
    session = Session()

    try:
        logger.info("Starting data ingestion from: %s", csv_path)
        start_time = time.time()

        count = ingest_data(
            csv_path=str(csv_path),
            session=session,
            batch_size=parsed_args.batch_size,
            clear_existing=not parsed_args.no_clear,
        )

        elapsed = time.time() - start_time
        logger.info(
            "Successfully ingested %d records in %.2f seconds", count, elapsed
        )
        return 0

    except FileNotFoundError as e:
        logger.error("File not found: %s", e)
        return 1
    except ValueError as e:
        logger.error("Data validation error: %s", e)
        return 1
    except Exception as e:
        logger.error("Unexpected error during ingestion: %s", e)
        return 1
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
