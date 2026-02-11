FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ app/
COPY scripts/ scripts/
COPY static/ static/

# Copy tests & config, run tests to generate coverage report
COPY tests/ tests/
COPY pyproject.toml .
RUN python -m pytest --tb=short -q 2>&1 && rm -rf tests/ .pytest_cache

# Expose port (Railway uses $PORT, default 8000 for local)
EXPOSE ${PORT:-8000}

# Start the service — uses $PORT if set (Railway), otherwise 8000
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
