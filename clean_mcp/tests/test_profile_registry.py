"""Focused tests for immutable, profile-scoped configuration values."""

import json
import os
from dataclasses import FrozenInstanceError

import pytest

from config import Config
from connectors.factory import ConnectorFactory
from models.connection_profile import ConnectionProfile
from services import profile_service
from services.profile_registry import ProfileRegistry, ProfileRegistryError, UnknownProfileError


def _postgres_profile(**overrides):
    values = {
        "db_type": "postgresql",
        "host": "postgres.internal",
        "database": "source_db",
        "username": "source_user",
        "password": "postgres-secret",
        "connection_options": {"ssl": {"mode": "require", "certificates": ["one", "two"]}},
    }
    values.update(overrides)
    return values


def _snowflake_profile(**overrides):
    values = {
        "db_type": "snowflake",
        "host": "org-account",
        "database": "TARGET_DB",
        "username": "TARGET_USER",
        "password": "snowflake-secret",
        "connection_options": {"warehouse": "COMPUTE_WH", "auth": {"token": "option-secret"}},
    }
    values.update(overrides)
    return values


def test_connection_profile_is_immutable_and_credentials_affect_equality():
    profile = ConnectionProfile(profile_id="postgres-source", **_postgres_profile())
    changed_credentials = ConnectionProfile(
        profile_id="postgres-source", **_postgres_profile(password="different-secret")
    )

    with pytest.raises(FrozenInstanceError):
        profile.database = "other"  # type: ignore[misc]

    assert profile != changed_credentials
    with pytest.raises(TypeError, match="unhashable"):
        hash(profile)


def test_connection_options_are_deeply_immutable_and_detached():
    source_options = {"ssl": {"mode": "require", "certificates": ["one", "two"]}}
    profile = ConnectionProfile(
        profile_id="postgres-source",
        **_postgres_profile(connection_options=source_options),
    )
    source_options["ssl"]["mode"] = "disable"

    assert profile.connection_options["ssl"]["mode"] == "require"
    with pytest.raises(TypeError):
        profile.connection_options["new"] = True  # type: ignore[index]
    with pytest.raises(TypeError):
        profile.connection_options["ssl"]["mode"] = "disable"  # type: ignore[index]
    with pytest.raises(AttributeError):
        profile.connection_options["ssl"]["certificates"].append("three")


def test_connection_option_copies_are_mutable_and_defensive():
    profile = ConnectionProfile(profile_id="postgres-source", **_postgres_profile())

    options = profile.connection_options_copy()
    options["ssl"]["mode"] = "disable"
    options["ssl"]["certificates"].append("three")

    assert profile.connection_options["ssl"]["mode"] == "require"
    assert profile.connection_options["ssl"]["certificates"] == ("one", "two")


def test_nested_set_of_tuples_has_a_defensive_mutable_copy():
    profile = ConnectionProfile(
        profile_id="postgres-source",
        **_postgres_profile(connection_options={"nested": {(1, 2)}}),
    )

    copied = profile.connection_options_copy()
    copied["nested"].add((3, 4))

    assert copied == {"nested": {(1, 2), (3, 4)}}
    assert profile.connection_options["nested"] == frozenset({(1, 2)})


def test_set_containing_frozenset_preserves_hashable_container_types():
    profile = ConnectionProfile(
        profile_id="postgres-source",
        **_postgres_profile(
            connection_options={"value": {frozenset({1, 2})}}
        ),
    )

    copied = profile.connection_options_copy()

    assert copied == {"value": {frozenset({1, 2})}}
    assert isinstance(copied["value"], set)
    assert all(isinstance(item, frozenset) for item in copied["value"])


def test_set_containing_tuple_with_frozenset_remains_hashable():
    profile = ConnectionProfile(
        profile_id="postgres-source",
        **_postgres_profile(
            connection_options={"value": {(frozenset({1}),)}}
        ),
    )

    copied = profile.connection_options_copy()

    assert copied == {"value": {(frozenset({1}),)}}
    item = next(iter(copied["value"]))
    assert isinstance(item, tuple)
    assert isinstance(item[0], frozenset)


def test_standalone_frozenset_remains_a_frozenset_in_copy():
    profile = ConnectionProfile(
        profile_id="postgres-source",
        **_postgres_profile(
            connection_options={"value": frozenset({1, 2})}
        ),
    )

    copied = profile.connection_options_copy()

    assert isinstance(copied["value"], frozenset)
    assert copied["value"] == frozenset({1, 2})


def test_sets_are_independently_mutable_and_tuples_remain_tuples():
    profile = ConnectionProfile(
        profile_id="postgres-source",
        **_postgres_profile(
            connection_options={"values": {1, 2}, "pair": ("left", "right")}
        ),
    )

    copied = profile.connection_options_copy()
    copied["values"].add(3)

    assert copied["values"] == {1, 2, 3}
    assert profile.connection_options["values"] == frozenset({1, 2})
    assert isinstance(copied["pair"], tuple)
    assert copied["pair"] == ("left", "right")


def test_mixed_nested_container_types_are_preserved_and_defensive():
    options = {
        "items": [
            (
                {
                    "labels": {"source", "target"},
                    "frozen_labels": frozenset({"left", "right"}),
                },
                ["nested-list"],
            )
        ]
    }
    profile = ConnectionProfile(
        profile_id="postgres-source",
        **_postgres_profile(connection_options=options),
    )

    copied = profile.connection_options_copy()

    assert isinstance(copied, dict)
    assert isinstance(copied["items"], list)
    assert isinstance(copied["items"][0], tuple)
    assert isinstance(copied["items"][0][0], dict)
    assert isinstance(copied["items"][0][0]["labels"], set)
    assert isinstance(copied["items"][0][0]["frozen_labels"], frozenset)
    assert isinstance(copied["items"][0][1], list)
    copied["items"][0][0]["labels"].add("audit")
    copied["items"][0][1].append("changed")

    assert "audit" not in profile.connection_options["items"][0][0]["labels"]
    assert profile.connection_options["items"][0][1] == ("nested-list",)


def test_no_public_credential_bearing_serialization_method_exists():
    profile = ConnectionProfile(profile_id="postgres-source", **_postgres_profile())

    assert not hasattr(profile, "connection_parameters")


def test_registry_lookup_is_case_insensitive_and_preserves_display_id():
    registry = ProfileRegistry({"Postgres-Source": _postgres_profile()})

    profile = registry.resolve("POSTGRES-SOURCE")

    assert profile.profile_id == "Postgres-Source"
    assert registry.resolve("postgres-source") is profile
    assert registry.safe_profiles()[0]["name"] == "Postgres-Source"


def test_registry_rejects_duplicate_profile_ids_ignoring_case():
    with pytest.raises(ProfileRegistryError, match="unique ignoring case"):
        ProfileRegistry(
            {
                "postgres-source": _postgres_profile(),
                "POSTGRES-SOURCE": _postgres_profile(),
            }
        )


def test_json_registry_rejects_duplicate_profile_ids_ignoring_case():
    raw_json = json.dumps(
        {
            "postgres-source": _postgres_profile(),
            "POSTGRES-SOURCE": _postgres_profile(),
        }
    )

    with pytest.raises(ProfileRegistryError, match="unique ignoring case"):
        ProfileRegistry.from_json(raw_json)


def test_registry_reports_unknown_profile_without_listing_configured_ids():
    registry = ProfileRegistry({"postgres-source": _postgres_profile()})

    with pytest.raises(UnknownProfileError, match="not configured") as error:
        registry.resolve("missing-profile")

    assert "postgres-source" not in str(error.value)


def test_registry_rejects_malformed_json_without_echoing_input():
    secret = "malformed-secret"

    with pytest.raises(ProfileRegistryError, match="valid JSON") as error:
        ProfileRegistry.from_json('{"postgres-source":{"password":"' + secret + '"}')

    assert secret not in str(error.value)


@pytest.mark.parametrize("raw_json", ["", "  \r\n\t "])
def test_blank_json_creates_an_empty_registry(raw_json):
    registry = ProfileRegistry.from_json(raw_json)

    assert len(registry) == 0
    assert registry.safe_profiles() == []


@pytest.mark.parametrize(
    "raw_json",
    [
        "[]",
        '{"profile": []}',
        '{"profile": {"db_type": "postgresql"}}',
        '{"profile": {"db_type": "postgresql", "host": "host", "unexpected": true}}',
    ],
)
def test_malformed_json_profiles_raise_only_registry_errors(raw_json):
    with pytest.raises(ProfileRegistryError):
        ProfileRegistry.from_json(raw_json)


def test_validation_errors_do_not_echo_secret_values():
    secret = "invalid-secret-port"

    with pytest.raises(ProfileRegistryError) as error:
        ProfileRegistry(
            {
                "postgres-source": _postgres_profile(
                    connection_options={"port": secret}
                )
            }
        )

    assert secret not in str(error.value)


@pytest.mark.parametrize(
    "profile",
    [
        {"host": "postgres.internal"},
        {"db_type": "postgresql"},
        {"db_type": "snowflake", "host": "org-account"},
    ],
)
def test_registry_rejects_missing_required_fields(profile):
    with pytest.raises(ProfileRegistryError):
        ProfileRegistry({"invalid-profile": profile})


def test_repr_safe_serialization_and_listing_never_expose_secrets():
    registry = ProfileRegistry(
        {
            "postgres-source": _postgres_profile(),
            "snowflake-target": _snowflake_profile(),
        }
    )
    postgres = registry.resolve("postgres-source")
    rendered = " ".join(
        [repr(postgres), repr(registry), json.dumps(postgres.to_safe_dict()), json.dumps(registry.safe_profiles())]
    )

    assert "postgres-secret" not in rendered
    assert "snowflake-secret" not in rendered
    assert "option-secret" not in rendered
    assert "source_user" not in rendered
    assert "TARGET_USER" not in rendered
    assert "postgres.internal" not in rendered
    assert "org-account" not in rendered
    assert postgres.to_safe_dict()["password_present"] is True


def test_registry_creation_does_not_mutate_environment_or_config(monkeypatch):
    monkeypatch.setenv("DB_ACTIVE_PROFILE", "legacy-active")
    monkeypatch.setattr(profile_service, "_active_profile", "legacy-active")
    monkeypatch.setattr(
        ConnectorFactory,
        "create",
        lambda *args, **kwargs: pytest.fail("registry created a database connector"),
    )
    environment_before = dict(os.environ)
    active_profile_before = profile_service._active_profile
    config_before = {
        "db_type": Config.DB_TYPE,
        "host": Config.HOST,
        "database": Config.DATABASE,
        "username": Config.USERNAME,
        "password": Config.PASSWORD,
        "connection_options": dict(Config.CONNECTION_OPTIONS),
        "timeout": Config.GLOBAL_TIMEOUT_SECONDS,
        "max_rows": Config.GLOBAL_MAX_ROWS,
    }

    registry = ProfileRegistry.from_json(json.dumps({"postgres-source": _postgres_profile()}))
    registry.resolve("POSTGRES-SOURCE")
    registry.safe_profiles()

    assert dict(os.environ) == environment_before
    assert Config.DB_TYPE == config_before["db_type"]
    assert Config.HOST == config_before["host"]
    assert Config.DATABASE == config_before["database"]
    assert Config.USERNAME == config_before["username"]
    assert Config.PASSWORD == config_before["password"]
    assert Config.CONNECTION_OPTIONS == config_before["connection_options"]
    assert Config.GLOBAL_TIMEOUT_SECONDS == config_before["timeout"]
    assert Config.GLOBAL_MAX_ROWS == config_before["max_rows"]
    assert os.environ["DB_ACTIVE_PROFILE"] == "legacy-active"
    assert profile_service._active_profile == active_profile_before


def test_postgresql_and_snowflake_profiles_coexist_in_one_registry():
    registry = ProfileRegistry.from_json(
        json.dumps(
            {
                "postgres-source": _postgres_profile(),
                "snowflake-target": _snowflake_profile(),
            }
        )
    )

    assert len(registry) == 2
    assert registry.resolve("postgres-source").db_type == "postgresql"
    assert registry.resolve("SNOWFLAKE-TARGET").db_type == "snowflake"
