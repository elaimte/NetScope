# Internet Usage Monitoring Service

A high-performance HTTP service that provides internet usage analytics for different users. Built with **FastAPI** and **SQLAlchemy**, it exposes RESTful APIs for querying paginated top-user rankings and detailed per-user consumption data.

---

## Tech Stack

| Component        | Technology               |
| ---------------- | ------------------------ |
| **Framework**    | FastAPI 0.104            |
| **ORM**          | SQLAlchemy 2.0           |
| **Database**     | SQLite (default)         |
| **Validation**   | Pydantic 2.5             |
| **Data Loading** | Pandas 2.1               |
| **Testing**      | Pytest 7.4 + pytest-cov  |
| **Language**     | Python 3.10+             |

---

## Prerequisites

- Python 3.10 or higher
- pip (Python package manager)

---

## Project Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd MishiPay
```

### 2. Create a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate   # macOS / Linux
# venv\Scripts\activate    # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Data Ingestion

Before using the service, ingest the provided CSV dataset into the database:

```bash
python scripts/ingest.py --csv dataset.csv
```

### Options

| Flag               | Description                                      | Default                            |
| ------------------ | ------------------------------------------------ | ---------------------------------- |
| `--csv`            | Path to the CSV file (**required**)               | —                                  |
| `--batch-size`     | Records per batch insert                          | `5000`                             |
| `--database-url`   | SQLAlchemy database URL                           | `sqlite:///./internet_usage.db`    |
| `--no-clear`       | Keep existing data (don't clear before ingestion) | clears existing data by default    |

### Examples

```bash
# Basic ingestion (clears existing data)
python scripts/ingest.py --csv dataset.csv

# Custom batch size
python scripts/ingest.py --csv dataset.csv --batch-size 10000

# Append to existing data
python scripts/ingest.py --csv dataset.csv --no-clear
```

---

## Running the Service

```bash
uvicorn app.main:app --reload
```

The service starts at **http://127.0.0.1:8000** by default.

---

## API Documentation

FastAPI automatically generates **OpenAPI 3.0** documentation:

| URL                                        | Description           |
| ------------------------------------------ | --------------------- |
| http://127.0.0.1:8000/docs                 | Swagger UI (interactive) |
| http://127.0.0.1:8000/redoc                | ReDoc (readable)      |
| http://127.0.0.1:8000/openapi.json         | Raw OpenAPI 3.0 JSON  |

### Endpoints

#### `GET /` — Health Check

Returns the service health status.

**Response:**
```json
{
  "status": "healthy",
  "service": "Internet Usage Monitoring Service"
}
```

---

#### `GET /api/v1/users/top` — Top Users by Internet Usage

Returns a **paginated** list of users ranked by their total internet usage (upload + download) in the last 30 days. Each user entry includes usage breakdowns for 1-day, 7-day, and 30-day periods.

**Query Parameters:**

| Parameter        | Type   | Required | Description                                                |
| ---------------- | ------ | -------- | ---------------------------------------------------------- |
| `page`           | int    | No       | Page number (default: `1`, min: `1`)                       |
| `per_page`       | int    | No       | Results per page (default: `10`, max: `100`)               |
| `reference_date` | string | No       | End date of 30-day window (ISO format). Defaults to latest record date. |

**Example Request:**
```
GET /api/v1/users/top?page=1&per_page=5&reference_date=2022-12-17T23:59:59
```

**Example Response:**
```json
{
  "page": 1,
  "per_page": 5,
  "total_users": 150,
  "total_pages": 30,
  "reference_date": "2022-12-17T23:59:59",
  "data": [
    {
      "rank": 1,
      "username": "brainyHeron5",
      "usage_1_day": {
        "upload_kb": 512000.0,
        "download_kb": 1024000.0,
        "total_kb": 1536000.0,
        "sessions": 3
      },
      "usage_7_days": {
        "upload_kb": 3500000.0,
        "download_kb": 7000000.0,
        "total_kb": 10500000.0,
        "sessions": 12
      },
      "usage_30_days": {
        "upload_kb": 15000000.0,
        "download_kb": 28000000.0,
        "total_kb": 43000000.0,
        "sessions": 45
      }
    }
  ]
}
```

**Error Responses:**
- `400` — Invalid `reference_date` format or no data in database

---

#### `GET /api/v1/users/details` — User Usage Details

Search for a user by their **exact name** and return their internet usage consumption details relative to the provided timestamp.

**Query Parameters:**

| Parameter   | Type   | Required | Description                                        |
| ----------- | ------ | -------- | -------------------------------------------------- |
| `username`  | string | Yes      | Exact username to search for                       |
| `timestamp` | string | Yes      | Reference timestamp (ISO format, e.g. `2022-12-15T23:59:59`) |

**Example Request:**
```
GET /api/v1/users/details?username=brainyHeron5&timestamp=2022-12-15T23:59:59
```

**Example Response:**
```json
{
  "username": "brainyHeron5",
  "timestamp": "2022-12-15T23:59:59",
  "usage_1_day": {
    "upload_kb": 512000.0,
    "download_kb": 1024000.0,
    "total_kb": 1536000.0,
    "sessions": 3
  },
  "usage_7_days": {
    "upload_kb": 3500000.0,
    "download_kb": 7000000.0,
    "total_kb": 10500000.0,
    "sessions": 12
  },
  "usage_30_days": {
    "upload_kb": 15000000.0,
    "download_kb": 28000000.0,
    "total_kb": 43000000.0,
    "sessions": 45
  }
}
```

**Error Responses:**
- `400` — Invalid `timestamp` format
- `404` — User not found

---

## Running Tests

Run the full test suite with coverage:

```bash
pytest
```

This will:
- Run all tests in the `tests/` directory
- Generate a coverage report for `app/` and `scripts/`
- Fail if coverage drops below **100%**

### Coverage Report

```bash
# Terminal report (default)
pytest

# HTML report (opens in browser)
pytest --cov-report=html
open htmlcov/index.html
```

### Run Specific Tests

```bash
# Run only API tests
pytest tests/test_api.py -v

# Run only ingestion tests
pytest tests/test_ingestion.py -v

# Run a specific test
pytest tests/test_api.py::TestListTopUsersEndpoint::test_ranking_order -v
```

---

## Project Structure

```
MishiPay/
├── app/
│   ├── __init__.py              # Package marker
│   ├── config.py                # Application settings (env-based)
│   ├── database.py              # Database engine, session, Base
│   ├── main.py                  # FastAPI app entry point
│   ├── models.py                # SQLAlchemy ORM models
│   ├── schemas.py               # Pydantic request/response schemas
│   ├── routers/
│   │   ├── __init__.py
│   │   └── users.py             # User API endpoints
│   └── services/
│       ├── __init__.py
│       ├── ingestion.py         # CSV ingestion logic
│       └── usage_service.py     # Usage analytics queries
├── scripts/
│   ├── __init__.py
│   └── ingest.py                # CLI ingestion script
├── tests/
│   ├── __init__.py
│   ├── conftest.py              # Shared test fixtures
│   ├── test_api.py              # API endpoint tests
│   ├── test_config.py           # Configuration tests
│   ├── test_database.py         # Database module tests
│   ├── test_ingestion.py        # Ingestion service tests
│   ├── test_ingest_script.py    # CLI script tests
│   ├── test_models.py           # ORM model tests
│   └── test_usage_service.py    # Usage service tests
├── dataset.csv                  # Internet usage dataset
├── pyproject.toml               # Pytest & coverage configuration
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## Design Decisions

### Performance Optimizations

1. **Conditional SQL Aggregation** — Usage statistics for 1-day, 7-day, and 30-day periods are computed in a **single query** using SQL `CASE` expressions, avoiding multiple database round-trips.

2. **Denormalized `total_kb` Column** — Pre-computed `upload_kb + download_kb` is stored during ingestion so ranking queries avoid on-the-fly computation.

3. **Database Indexes** — Composite indexes on `(username, start_time)` and individual indexes on `username` and `start_time` accelerate both filtered aggregations and user lookups.

4. **Batch Insertion** — The ingestion script inserts records in configurable batches (default: 5000) to balance memory usage and insert performance.

### Data Units

Upload and download columns in the dataset represent data in **Kilobits (Kb)**. All API responses return values in the same unit.

---

## Environment Variables

| Variable        | Description             | Default                         |
| --------------- | ----------------------- | ------------------------------- |
| `DATABASE_URL`  | SQLAlchemy database URL | `sqlite:///./internet_usage.db` |

Create a `.env` file in the project root to override defaults:

```env
DATABASE_URL=sqlite:///./internet_usage.db
```
