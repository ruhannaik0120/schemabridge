"""Verify Spark routing is automatic for eligible large source tables."""

from __future__ import annotations

import pytest

from schemabridge.spark import SparkTransportSettings
from schemabridge.spark.config import SparkConfigurationError


def test_defaults_use_the_automatic_large_table_threshold() -> None:
    settings = SparkTransportSettings.from_environment()

    assert settings.minimum_source_rows == 1_000_000
    assert settings.should_use_spark(None) is False
    assert settings.should_use_spark(999_999) is False
    assert settings.should_use_spark(1_000_000) is True


def test_settings_accept_threshold_and_local_master(monkeypatch) -> None:
    monkeypatch.setenv("SCHEMABRIDGE_SPARK_MINIMUM_SOURCE_ROWS", "1000")
    monkeypatch.setenv("SCHEMABRIDGE_SPARK_MASTER", "local[2]")

    settings = SparkTransportSettings.from_environment()

    assert settings.master == "local[2]"
    assert settings.should_use_spark(None) is False
    assert settings.should_use_spark(999) is False
    assert settings.should_use_spark(1000) is True


@pytest.mark.parametrize("value", ("-1", "0", "not-a-number"))
def test_invalid_spark_threshold_is_rejected(monkeypatch, value: str) -> None:
    monkeypatch.setenv("SCHEMABRIDGE_SPARK_MINIMUM_SOURCE_ROWS", value)

    with pytest.raises(SparkConfigurationError):
        SparkTransportSettings.from_environment()


def test_blank_threshold_uses_the_safe_default(monkeypatch) -> None:
    monkeypatch.setenv("SCHEMABRIDGE_SPARK_MINIMUM_SOURCE_ROWS", "")

    assert SparkTransportSettings.from_environment().minimum_source_rows == 1_000_000


def test_package_check_is_case_insensitive_and_requires_non_empty_text() -> None:
    settings = SparkTransportSettings(jars_packages="net.snowflake:spark-snowflake_2.12:3.2.1")

    assert settings.has_package("SPARK-SNOWFLAKE") is True
    assert settings.has_package("postgresql") is False
    with pytest.raises(ValueError):
        settings.has_package("")
