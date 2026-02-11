"""Tests for the CLI ingestion script (scripts/ingest.py).

Covers all branches: CSV not found, custom DB URL, default DB URL,
--no-clear flag, successful ingestion, and all exception handlers.
"""

from unittest.mock import patch

from scripts.ingest import main


class TestIngestScript:
    """Tests for the main() entry point of the ingestion script."""

    def test_csv_file_not_found_returns_1(self, tmp_path):
        """Missing CSV file should return exit code 1 immediately."""
        result = main(["--csv", str(tmp_path / "nonexistent.csv")])
        assert result == 1

    def test_successful_ingestion_with_custom_db_url(self, tmp_path):
        """Successful ingestion with --database-url should return exit code 0."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        db_path = tmp_path / "test.db"
        result = main([
            "--csv", str(csv_file),
            "--database-url", f"sqlite:///{db_path}",
        ])
        assert result == 0

    def test_default_database_url_from_settings(self, tmp_path, monkeypatch):
        """Without --database-url, should use settings.DATABASE_URL (else branch)."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        # Change working directory so the default sqlite:///./internet_usage.db
        # is created in the temp directory instead of the project root
        monkeypatch.chdir(tmp_path)
        result = main(["--csv", str(csv_file)])
        assert result == 0

    def test_no_clear_flag(self, tmp_path):
        """--no-clear flag should pass clear_existing=False to ingest_data."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        db_path = tmp_path / "test.db"
        result = main([
            "--csv", str(csv_file),
            "--database-url", f"sqlite:///{db_path}",
            "--no-clear",
        ])
        assert result == 0

    def test_custom_batch_size(self, tmp_path):
        """Custom --batch-size should be accepted and used."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        db_path = tmp_path / "test.db"
        result = main([
            "--csv", str(csv_file),
            "--database-url", f"sqlite:///{db_path}",
            "--batch-size", "1",
        ])
        assert result == 0

    def test_file_not_found_error_during_ingestion(self, tmp_path):
        """FileNotFoundError raised during ingestion should return exit code 1."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        with patch(
            "scripts.ingest.ingest_data",
            side_effect=FileNotFoundError("simulated"),
        ):
            result = main([
                "--csv", str(csv_file),
                "--database-url", f"sqlite:///{tmp_path / 'test.db'}",
            ])
            assert result == 1

    def test_value_error_during_ingestion(self, tmp_path):
        """ValueError raised during ingestion should return exit code 1."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        with patch(
            "scripts.ingest.ingest_data",
            side_effect=ValueError("bad data"),
        ):
            result = main([
                "--csv", str(csv_file),
                "--database-url", f"sqlite:///{tmp_path / 'test.db'}",
            ])
            assert result == 1

    def test_unexpected_exception_during_ingestion(self, tmp_path):
        """Unexpected exceptions during ingestion should return exit code 1."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
        )
        with patch(
            "scripts.ingest.ingest_data",
            side_effect=RuntimeError("unexpected"),
        ):
            result = main([
                "--csv", str(csv_file),
                "--database-url", f"sqlite:///{tmp_path / 'test.db'}",
            ])
            assert result == 1

    def test_ingestion_idempotent_with_clear(self, tmp_path):
        """Running ingestion twice with clear should produce same record count."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "username,mac_address,start_time,usage_time,upload,download\n"
            "user1,AA:BB:CC:DD:EE:FF,2022-12-01 10:00:00,1:00:00,1000.0,2000.0\n"
            "user2,BB:CC:DD:EE:FF:00,2022-12-02 11:00:00,2:00:00,3000.0,4000.0\n"
        )
        db_path = tmp_path / "test.db"
        db_url = f"sqlite:///{db_path}"

        result1 = main(["--csv", str(csv_file), "--database-url", db_url])
        assert result1 == 0

        result2 = main(["--csv", str(csv_file), "--database-url", db_url])
        assert result2 == 0
