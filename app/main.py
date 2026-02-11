"""FastAPI application entry point for the Internet Usage Monitoring Service.

This service provides HTTP APIs for monitoring and analyzing internet usage
data across different users. It supports paginated top-user listings and
detailed per-user usage queries.

Run with: uvicorn app.main:app --reload
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.routers.upload import router as upload_router
from app.routers.users import router as users_router

app = FastAPI(
    title=settings.APP_NAME,
    description=(
        "A reusable HTTP service that returns internet usage analytics for "
        "different users. Provides paginated top-user listings by overall "
        "usage and detailed per-user consumption queries."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.include_router(users_router)
app.include_router(upload_router)


@app.on_event("startup")
def on_startup():  # pragma: no cover
    """Create database tables on startup if they don't exist."""
    from app.database import Base, engine

    Base.metadata.create_all(bind=engine)

# Serve the dashboard UI and coverage report
_project_root = Path(__file__).resolve().parent.parent
_static_dir = _project_root / "static"
_htmlcov_dir = _project_root / "htmlcov"

if _static_dir.is_dir():  # pragma: no cover
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

    @app.get("/dashboard", include_in_schema=False)
    def dashboard():
        """Serve the analytics dashboard."""
        return FileResponse(str(_static_dir / "index.html"))

if _htmlcov_dir.is_dir():  # pragma: no cover
    app.mount("/coverage", StaticFiles(directory=str(_htmlcov_dir), html=True), name="coverage")


@app.get("/", tags=["Health"])
def health_check():
    """Health check endpoint to verify the service is running."""
    return {"status": "healthy", "service": settings.APP_NAME}
