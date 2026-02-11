"""Tests for application configuration."""

from app.config import Settings, settings


class TestSettings:
    """Tests for the Settings class and module-level settings instance."""

    def test_default_database_url(self):
        """Default DATABASE_URL should be a local SQLite file."""
        assert settings.DATABASE_URL == "sqlite:///./internet_usage.db"

    def test_default_app_name(self):
        """Default APP_NAME should be the service name."""
        assert settings.APP_NAME == "Internet Usage Monitoring Service"

    def test_default_page_size(self):
        """Default page size should be 10."""
        assert settings.DEFAULT_PAGE_SIZE == 10

    def test_max_page_size(self):
        """Maximum page size should be 100."""
        assert settings.MAX_PAGE_SIZE == 100

    def test_settings_is_instance_of_settings_class(self):
        """Module-level settings should be an instance of Settings."""
        assert isinstance(settings, Settings)
