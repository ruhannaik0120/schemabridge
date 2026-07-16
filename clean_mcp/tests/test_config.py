"""Configuration tests for the MCP server."""

import os

import pytest

import config as config_module

from config import Config, ConfigError


def _configure_generic_settings(monkeypatch):
    """Install a valid baseline so each test isolates one configuration rule."""

    monkeypatch.setenv("DB_TYPE", "sqlserver")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_DATABASE", "devdb")
    monkeypatch.setenv("DB_USERNAME", "dev_user")
    monkeypatch.setenv("DB_PASSWORD", "dev_pass")
    monkeypatch.setenv("DB_CONNECTION_OPTIONS", '{"driver": "ODBC Driver 18 for SQL Server"}')
    monkeypatch.setenv("DB_TIMEOUT_SECONDS", "20")
    monkeypatch.setenv("DB_MAX_ROWS", "100")


def test_config_validation_passes_with_generic_settings(monkeypatch):
    _configure_generic_settings(monkeypatch)

    Config.load()
    Config.validate()

    profile = Config.connection_config()
    assert Config.DB_TYPE == "sqlserver"
    assert profile.db_type == "sqlserver"
    assert profile.host == "localhost"
    assert profile.database == "devdb"
    assert profile.connection_options == {"driver": "ODBC Driver 18 for SQL Server"}


def test_connection_config_has_no_execution_mode(monkeypatch):
    _configure_generic_settings(monkeypatch)

    Config.load()

    assert not hasattr(Config.connection_config(), "execution_mode")


def test_config_rejects_non_numeric_max_rows(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "sqlserver")
    monkeypatch.setenv("DB_MAX_ROWS", "abc")

    with pytest.raises(ConfigError, match="Expected an integer value"):
        Config.load()


def test_invalid_connection_options_raise_structured_error(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "sqlserver")
    monkeypatch.setenv("DB_CONNECTION_OPTIONS", "not-json")

    with pytest.raises(ConfigError, match="valid JSON"):
        Config.load()


def test_config_rejects_unsupported_db_type(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "oracle")
    monkeypatch.setenv("DB_HOST", "localhost")

    Config.load()

    with pytest.raises(ConfigError, match="DB_TYPE must be one of"):
        Config.validate()


def test_config_allows_demo_without_host(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "demo")
    monkeypatch.setenv("DB_HOST", "")
    monkeypatch.setenv("DB_DATABASE", "qa_demo")

    Config.load()
    Config.validate()

    assert Config.DB_TYPE == "demo"


def test_sqlserver_rejects_partial_credentials_during_startup_validation(monkeypatch):
    _configure_generic_settings(monkeypatch)
    monkeypatch.setenv("DB_PASSWORD", "")

    Config.load()

    with pytest.raises(ConfigError, match="must either both be set or both be empty"):
        Config.validate()


def test_config_rejects_placeholder_host(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "postgresql")
    monkeypatch.setenv("DB_HOST", "<localhost>")

    Config.load()

    with pytest.raises(ConfigError, match="not a <placeholder>"):
        Config.validate()


def test_config_rejects_quoted_host(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "postgresql")
    monkeypatch.setenv("DB_HOST", "'localhost'")

    Config.load()

    with pytest.raises(ConfigError, match="wrapping quotes"):
        Config.validate()


def test_config_rejects_invalid_snowflake_account(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "snowflake")
    monkeypatch.setenv("DB_HOST", "https://org-account.snowflakecomputing.com")
    monkeypatch.setenv("DB_USERNAME", "user")

    Config.load()

    with pytest.raises(ConfigError, match="Snowflake"):
        Config.validate()


def test_config_accepts_snowflake_locator_with_region_segments(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "snowflake")
    monkeypatch.setenv("DB_HOST", "xy12345.ap-south-1")
    monkeypatch.setenv("DB_USERNAME", "user")

    Config.load()

    assert Config.validate().HOST == "xy12345.ap-south-1"


def test_reload_dotenv_clears_removed_recognized_values(monkeypatch, tmp_path):
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("DB_TYPE=demo\nDB_DATABASE=qa_demo\n", encoding="utf-8")
    monkeypatch.setattr(config_module, "_DOTENV_PATH", dotenv_path)
    monkeypatch.setenv("DB_USERNAME", "stale-user")
    monkeypatch.setenv("DB_PASSWORD", "stale-password")

    Config.reload_dotenv(override=True)

    assert "DB_USERNAME" not in os.environ
    assert "DB_PASSWORD" not in os.environ


def test_config_diagnostics_redacts_password(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "postgresql")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PASSWORD", "secret-value")
    monkeypatch.setenv("DB_CONNECTION_OPTIONS", '{"port":5432,"password":"nested-secret"}')

    Config.load()
    diagnostics = Config.diagnostics()

    assert diagnostics["password_present"] is True
    assert "secret-value" not in str(diagnostics)
    assert diagnostics["connection_options"]["password"] == "[REDACTED]"
    assert "host" not in diagnostics


def test_diagnostics_redact_connection_strings_and_private_keys(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "postgresql")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv(
        "DB_CONNECTION_OPTIONS",
        '{"connection_string":"Server=private","private_key":"key-material"}',
    )

    Config.load()
    diagnostics = Config.diagnostics()

    assert diagnostics["connection_options"]["connection_string"] == "[REDACTED]"
    assert diagnostics["connection_options"]["private_key"] == "[REDACTED]"
    assert "key-material" not in str(diagnostics)


def test_diagnostics_redact_nested_and_variant_secret_keys(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "postgresql")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv(
        "DB_CONNECTION_OPTIONS",
        '{"auth":{"clientSecret":"nested-value"},"sslpassword":"ssl-value","apiKey":"api-value"}',
    )

    Config.load()
    diagnostics = Config.diagnostics()

    assert diagnostics["connection_options"]["auth"]["clientSecret"] == "[REDACTED]"
    assert diagnostics["connection_options"]["sslpassword"] == "[REDACTED]"
    assert diagnostics["connection_options"]["apiKey"] == "[REDACTED]"
    assert "nested-value" not in str(diagnostics)


@pytest.mark.parametrize("reserved_key", ["host", "SERVER", "user-id", "PWD", "database", "login_timeout"])
def test_config_rejects_connection_options_that_override_profile_fields(monkeypatch, reserved_key):
    _configure_generic_settings(monkeypatch)
    monkeypatch.setenv("DB_CONNECTION_OPTIONS", '{"' + reserved_key + '":"shadow-target"}')

    Config.load()

    with pytest.raises(ConfigError, match="cannot override profile-controlled fields"):
        Config.validate()


def test_error_redaction_handles_nested_secrets_and_bearer_tokens(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "demo")
    monkeypatch.setenv("DB_DATABASE", "qa_demo")
    monkeypatch.setenv("DB_CONNECTION_OPTIONS", '{"auth":{"clientSecret":"nested-value"}}')
    Config.load()

    redacted = Config.redact_text("nested-value Authorization: Bearer abc.def")

    assert "nested-value" not in redacted
    assert "abc.def" not in redacted
