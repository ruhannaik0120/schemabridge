"""Tests for connectors bound to immutable connection profiles."""

import json
import logging
import os

import pytest

from config import Config, ConfigError
from connectors.demo.connector import DemoConnector
from connectors.factory import ConnectorFactory
from connectors.mysql.connector import MySQLConnector
from connectors.postgresql.connector import PostgreSQLConnector
from connectors.snowflake.connector import SnowflakeConnector
from connectors.sqlserver.connector import SQLServerConnector
from logger import logger
from models.connection_profile import ConnectionProfile


def _profile(db_type: str, **overrides) -> ConnectionProfile:
    values = {
        "profile_id": f"{db_type}-profile",
        "db_type": db_type,
        "host": "db.example.test" if db_type != "demo" else "",
        "database": "qa_demo",
        "username": "qa_user" if db_type == "snowflake" else "",
        "password": "profile-secret" if db_type == "snowflake" else "",
        "timeout_seconds": 17,
        "max_rows": 123,
    }
    values.update(overrides)
    return ConnectionProfile(**values)


@pytest.mark.parametrize(
    ("db_type", "connector_class"),
    [
        ("demo", DemoConnector),
        ("mysql", MySQLConnector),
        ("postgresql", PostgreSQLConnector),
        ("snowflake", SnowflakeConnector),
        ("sqlserver", SQLServerConnector),
    ],
)
def test_factory_creates_profile_bound_connector(db_type, connector_class):
    profile = _profile(db_type)

    connector = ConnectorFactory.create_for_profile(profile)

    assert isinstance(connector, connector_class)
    assert connector._profile() is profile


def test_factory_requires_connection_profile_without_rendering_input():
    secret = "must-not-appear"

    with pytest.raises(TypeError) as error:
        ConnectorFactory.create_for_profile({"password": secret})

    assert secret not in str(error.value)


@pytest.mark.parametrize(
    ("connector_class", "wrong_profile"),
    [
        (PostgreSQLConnector, _profile("snowflake")),
        (SnowflakeConnector, _profile("postgresql")),
    ],
)
def test_connector_rejects_mismatched_profile_without_rendering_it(connector_class, wrong_profile):
    with pytest.raises(ValueError) as error:
        connector_class(profile=wrong_profile)

    message = str(error.value)
    assert repr(wrong_profile) not in message
    for secret in (wrong_profile.password, wrong_profile.host, wrong_profile.username):
        if secret:
            assert secret not in message
    assert "connection_options" not in message


def test_profile_bound_postgresql_and_snowflake_never_read_config(monkeypatch):
    postgres_profile = _profile(
        "postgresql",
        profile_id="postgres-source",
        host="postgres.internal",
        database="source_db",
        username="source_user",
        password="postgres-secret",
        connection_options={"port": 5433, "tags": ["source"]},
    )
    snowflake_profile = _profile(
        "snowflake",
        profile_id="snowflake-target",
        host="org-account",
        database="TARGET_DB",
        username="TARGET_USER",
        password="snowflake-secret",
        connection_options={
            "warehouse": "TARGET_WH",
            "session_parameters": {"QUERY_TAGS": ["target"]},
        },
    )
    environment_before = dict(os.environ)
    config_before = (
        Config.DB_TYPE,
        Config.HOST,
        Config.DATABASE,
        Config.USERNAME,
        Config.PASSWORD,
        dict(Config.CONNECTION_OPTIONS),
    )
    monkeypatch.setattr(
        Config,
        "connection_config",
        classmethod(lambda cls: pytest.fail("profile-bound connector read Config")),
    )

    postgres = ConnectorFactory.create_for_profile(postgres_profile)
    snowflake = ConnectorFactory.create_for_profile(snowflake_profile)
    postgres_kwargs = postgres._connection_kwargs(postgres._profile(), "source_db")
    snowflake_kwargs = snowflake._connection_kwargs(snowflake._profile(), "TARGET_DB")
    postgres_kwargs["tags"].append("changed")
    snowflake_kwargs["session_parameters"]["QUERY_TAGS"].append("changed")

    assert postgres_kwargs["host"] == "postgres.internal"
    assert postgres_kwargs["dbname"] == "source_db"
    assert postgres_kwargs["connect_timeout"] == 17
    assert snowflake_kwargs["account"] == "org-account"
    assert snowflake_kwargs["database"] == "TARGET_DB"
    assert snowflake_kwargs["login_timeout"] == 17
    assert postgres_profile.connection_options_copy()["tags"] == ["source"]
    assert snowflake_profile.connection_options_copy()["session_parameters"]["QUERY_TAGS"] == ["target"]
    assert dict(os.environ) == environment_before
    assert (
        Config.DB_TYPE,
        Config.HOST,
        Config.DATABASE,
        Config.USERNAME,
        Config.PASSWORD,
        dict(Config.CONNECTION_OPTIONS),
    ) == config_before


def test_profile_bound_connector_repr_hides_credentials():
    profile = _profile(
        "postgresql",
        host="private-host",
        username="private-user",
        password="private-password",
        connection_options={"token_value": "private-token"},
    )

    rendered = repr(ConnectorFactory.create_for_profile(profile))

    assert "private-host" not in rendered
    assert "private-user" not in rendered
    assert "private-password" not in rendered
    assert "private-token" not in rendered


def test_profile_bound_metadata_does_not_serialize_credentials():
    profile = _profile(
        "demo",
        username="private-user",
        password="private-password",
        connection_options={"token_value": "private-token"},
    )
    connector = ConnectorFactory.create_for_profile(profile)

    # logged_in_user remains server-reported operational metadata. QueryService
    # will decide later whether SchemaBridge displays it; profile secrets and
    # secret connection options must never be serialized here.
    rendered = json.dumps(connector.test_connection())

    assert "private-user" not in rendered
    assert "private-password" not in rendered
    assert "private-token" not in rendered


def test_profile_bound_sqlserver_failure_logs_no_connection_details(monkeypatch):
    profile = _profile(
        "sqlserver",
        profile_id="sqlserver-target",
        host="private-sql-host",
        database="private-database",
        username="private-user",
        password="private-password",
        connection_options={"application_name": "private-application"},
    )
    connector = SQLServerConnector(profile=profile)
    original_error = RuntimeError("driver echoed private-password")

    class FailingDriver:
        Error = RuntimeError

        def connect(self, conn_str, timeout=30):
            raise original_error

    records = []

    class RecordHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = RecordHandler()
    logger.addHandler(handler)
    monkeypatch.setattr(connector, "_driver", lambda: FailingDriver())
    try:
        with pytest.raises(RuntimeError) as raised:
            connector.connect()
    finally:
        logger.removeHandler(handler)

    assert raised.value is original_error
    assert any(record.getMessage() == "SQL Server profile-bound connection failed" for record in records)
    assert all(record.exc_info is None for record in records)
    rendered_logs = " ".join(record.getMessage() for record in records)
    for private_value in (
        profile.profile_id,
        profile.host,
        profile.database,
        profile.username,
        profile.password,
        "private-application",
    ):
        assert private_value not in rendered_logs


def test_profile_bound_sqlserver_invalid_option_error_is_generic():
    invalid_key = "private-option;"
    invalid_value = "private-option-value"
    profile = _profile(
        "sqlserver",
        profile_id="private-profile-id",
        host="private-sql-host",
        database="private-database",
        username="private-user",
        password="private-password",
        connection_options={invalid_key: invalid_value},
    )
    connector = SQLServerConnector(profile=profile)

    with pytest.raises(ConfigError) as raised:
        connector._connection_options(profile)

    assert str(raised.value) == "Unsupported SQL Server connection option"
    for private_value in (
        invalid_key,
        invalid_value,
        profile.profile_id,
        profile.host,
        profile.username,
        profile.password,
    ):
        assert private_value not in str(raised.value)


def test_legacy_sqlserver_invalid_option_error_remains_compatible(monkeypatch):
    invalid_key = "legacy-invalid;"
    legacy_profile = _profile(
        "sqlserver",
        connection_options={invalid_key: "legacy-value"},
    )
    monkeypatch.setattr(
        Config,
        "connection_config",
        classmethod(lambda cls: legacy_profile),
    )
    connector = SQLServerConnector()

    with pytest.raises(ConfigError, match="Invalid ODBC connection option name") as raised:
        connector._connection_options(connector._profile())

    assert invalid_key in str(raised.value)


def test_legacy_factory_and_connector_still_use_config(monkeypatch):
    profile = _profile("demo", database="legacy-demo")
    calls = []
    monkeypatch.setattr(
        Config,
        "connection_config",
        classmethod(lambda cls: calls.append(True) or profile),
    )

    connector = ConnectorFactory.create("demo")

    assert connector._profile() is profile
    assert calls == [True]
