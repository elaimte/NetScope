"""Tests for database connection and session management."""

from unittest.mock import MagicMock, patch

from app.database import Base, SessionLocal, _build_engine, engine, get_db


class TestBuildEngine:
    """Tests for the _build_engine helper function."""

    def test_sqlite_url_sets_check_same_thread(self):
        """SQLite URLs should include check_same_thread=False in connect_args."""
        with patch("app.database.create_engine") as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            _build_engine("sqlite:///test.db")
            mock_create_engine.assert_called_once_with(
                "sqlite:///test.db",
                connect_args={"check_same_thread": False},
            )

    def test_non_sqlite_url_empty_connect_args(self):
        """Non-SQLite URLs should have empty connect_args."""
        with patch("app.database.create_engine") as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            _build_engine("postgresql://user:pass@localhost/db")
            mock_create_engine.assert_called_once_with(
                "postgresql://user:pass@localhost/db",
                connect_args={},
            )


class TestModuleLevelObjects:
    """Tests for module-level engine and session factory."""

    def test_engine_is_created(self):
        """Module-level engine should exist."""
        assert engine is not None

    def test_session_local_is_created(self):
        """Module-level SessionLocal should exist."""
        assert SessionLocal is not None


class TestBase:
    """Tests for the declarative Base class."""

    def test_base_has_metadata(self):
        """Base should have a metadata attribute for table definitions."""
        assert hasattr(Base, "metadata")


class TestGetDb:
    """Tests for the get_db dependency function."""

    def test_yields_session_and_closes_on_completion(self):
        """get_db should yield a session and close it when the generator exits."""
        mock_session = MagicMock()
        with patch("app.database.SessionLocal", return_value=mock_session):
            gen = get_db()
            session = next(gen)
            assert session is mock_session
            # Exhaust the generator to trigger finally block
            gen.close()
            mock_session.close.assert_called_once()

    def test_closes_session_on_exception(self):
        """get_db should close the session even if an exception occurs."""
        mock_session = MagicMock()
        with patch("app.database.SessionLocal", return_value=mock_session):
            gen = get_db()
            next(gen)
            # Simulate an exception being thrown into the generator
            try:
                gen.throw(RuntimeError, "test error")
            except RuntimeError:
                pass
            mock_session.close.assert_called_once()
