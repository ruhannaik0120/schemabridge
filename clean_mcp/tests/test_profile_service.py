"""Tests for safe runtime connection-profile switching."""

import json
from threading import Event, Thread

import pytest

import config as config_module
from config import Config, ConfigError
from services import profile_service
from services.runtime_state import runtime_lock


@pytest.fixture
def profiles(monkeypatch):
    """Install named profiles for discovery, switching, and rollback tests."""

    payload = {
        "demo-one": {"db_type": "demo", "database": "qa_demo"},
        "demo-two": {"db_type": "demo", "database": "sales"},
    }
    monkeypatch.setenv("DB_PROFILES_JSON", json.dumps(payload))
    monkeypatch.setenv("DB_TYPE", "demo")
    monkeypatch.setenv("DB_DATABASE", "qa_demo")
    monkeypatch.setattr(profile_service, "_active_profile", "default")
    Config.load()
    return payload


def test_list_profiles_never_returns_credentials(monkeypatch, profiles):
    profiles["demo-one"]["password"] = "top-secret"
    monkeypatch.setenv("DB_PROFILES_JSON", json.dumps(profiles))

    result = profile_service.list_connection_profiles()

    assert result["count"] == 2
    assert "top-secret" not in str(result)
    assert result["profiles"][0]["password_present"] is True


def test_list_profiles_reports_non_secret_readiness_issues(monkeypatch, profiles):
    monkeypatch.setenv(
        "DB_PROFILES_JSON",
        json.dumps(
            {
                **profiles,
                "bad-postgres": {"db_type": "postgresql", "host": "'localhost'", "database": "qa"},
                "bad-snowflake": {
                    "db_type": "snowflake",
                    "host": "https://org-account.snowflakecomputing.com",
                    "username": "user",
                },
            }
        ),
    )

    result = profile_service.list_connection_profiles()
    by_name = {profile["name"]: profile for profile in result["profiles"]}

    assert by_name["bad-postgres"]["ready"] is False
    assert "host_wrapping_quotes" in by_name["bad-postgres"]["issues"]
    assert by_name["bad-snowflake"]["snowflake_account_format_valid"] is False
    assert "org.account" not in str(result)


def test_switch_requires_explicit_confirmation(profiles):
    with pytest.raises(ConfigError, match="explicit user approval"):
        profile_service.switch_connection_profile("demo-two")


def test_reload_configuration_requires_explicit_confirmation(profiles):
    with pytest.raises(ConfigError, match="explicit user approval"):
        profile_service.reload_runtime_configuration()


def test_reload_configuration_resets_active_profile(monkeypatch, profiles):
    monkeypatch.setenv("DB_ACTIVE_PROFILE", "demo-one")
    monkeypatch.setattr(profile_service, "_active_profile", "default")
    monkeypatch.setattr(Config, "reload_dotenv", lambda override=True: Config.load())

    result = profile_service.reload_runtime_configuration(confirm=True)

    assert result["reloaded"] is True
    assert result["active_profile"] == "demo-one"


def test_reload_configuration_rolls_back_an_invalid_file(monkeypatch, tmp_path, profiles):
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "DB_TYPE=postgresql\n"
        "DB_HOST=<localhost>\n"
        "DB_DATABASE=broken\n"
        "DB_ACTIVE_PROFILE=broken\n"
        'DB_PROFILES_JSON={"broken":{"db_type":"postgresql","host":"<localhost>"}}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(config_module, "_DOTENV_PATH", dotenv_path)
    monkeypatch.setenv("DB_ACTIVE_PROFILE", "demo-one")
    monkeypatch.setattr(profile_service, "_active_profile", "demo-one")
    Config.load()

    with pytest.raises(ConfigError, match="not a <placeholder>"):
        profile_service.reload_runtime_configuration(confirm=True)

    assert Config.DB_TYPE == "demo"
    assert Config.DATABASE == "qa_demo"
    assert profile_service.list_connection_profiles()["active_profile"] == "demo-one"


def test_list_profiles_accepts_snowflake_locator_region_format(monkeypatch, profiles):
    monkeypatch.setenv(
        "DB_PROFILES_JSON",
        json.dumps(
            {
                **profiles,
                "snowflake-region": {
                    "db_type": "snowflake",
                    "host": "xy12345.ap-south-1",
                    "database": "qa",
                    "username": "user",
                },
            }
        ),
    )

    result = profile_service.list_connection_profiles()
    snowflake_profile = next(profile for profile in result["profiles"] if profile["name"] == "snowflake-region")

    assert snowflake_profile["snowflake_account_format_valid"] is True
    assert snowflake_profile["ready"] is True


def test_switch_rebuilds_service_and_changes_active_database(profiles):
    result = profile_service.switch_connection_profile("demo-two", confirm=True)

    assert result["switched"] is True
    assert result["active_profile"] == "demo-two"
    assert result["database"] == "sales"
    assert result["connection_status"] == "connected"
    assert Config.DATABASE == "sales"


def test_failed_switch_restores_previous_configuration(monkeypatch, profiles):
    monkeypatch.setenv(
        "DB_PROFILES_JSON",
        json.dumps({**profiles, "broken": {"db_type": "postgresql", "database": "missing-host"}}),
    )

    with pytest.raises(ConfigError, match="DB_HOST"):
        profile_service.switch_connection_profile("broken", confirm=True)

    assert Config.DB_TYPE == "demo"
    assert Config.DATABASE == "qa_demo"
    assert profile_service.list_connection_profiles()["active_profile"] == "default"


def test_profile_switch_waits_for_an_active_runtime_operation(profiles):
    started = Event()
    finished = Event()

    def switch_profile():
        started.set()
        profile_service.switch_connection_profile("demo-two", confirm=True)
        finished.set()

    with runtime_lock:
        worker = Thread(target=switch_profile)
        worker.start()
        assert started.wait(timeout=1)
        assert finished.wait(timeout=0.05) is False

    worker.join(timeout=2)
    assert finished.is_set()
    assert Config.DATABASE == "sales"
