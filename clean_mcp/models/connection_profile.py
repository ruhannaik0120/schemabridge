"""Immutable database connection profile values."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, ClassVar

from connectors.factory import SUPPORTED_CONNECTORS


_RESERVED_CONNECTION_OPTION_KEYS = frozenset(
    {
        "host",
        "server",
        "datasource",
        "address",
        "networkaddress",
        "account",
        "user",
        "username",
        "userid",
        "uid",
        "password",
        "passwd",
        "pwd",
        "database",
        "dbname",
        "initialcatalog",
        "timeout",
        "connectiontimeout",
        "connecttimeout",
        "logintimeout",
        "readtimeout",
        "writetimeout",
        "networktimeout",
        "sockettimeout",
    }
)


class ConnectionProfileError(ValueError):
    """Raised when a connection profile is malformed or unsafe."""


class _FrozenList(tuple):
    """Retain list identity while storing immutable option values."""


class _FrozenSet(frozenset):
    """Retain mutable-set identity while storing immutable option values."""


def _normalized_option_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def _freeze_option(value: Any) -> Any:
    """Create an immutable, detached representation of JSON-like options."""

    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ConnectionProfileError("connection_options keys must be strings.")
            frozen[key] = _freeze_option(item)
        return MappingProxyType(frozen)
    if isinstance(value, list):
        return _FrozenList(_freeze_option(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_option(item) for item in value)
    if isinstance(value, set):
        return _FrozenSet(_freeze_option(item) for item in value)
    if isinstance(value, frozenset):
        return frozenset(_freeze_option(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool, bytes)):
        return value
    raise ConnectionProfileError("connection_options values must be immutable or JSON-compatible.")


def _mutable_option_copy(value: Any) -> Any:
    """Return detached mutable containers suitable for a database driver."""

    if isinstance(value, Mapping):
        return {key: _mutable_option_copy(item) for key, item in value.items()}
    if isinstance(value, _FrozenList):
        return [_mutable_option_copy(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_mutable_option_copy(item) for item in value)
    if isinstance(value, _FrozenSet):
        return {_mutable_option_copy(item) for item in value}
    if isinstance(value, frozenset):
        return frozenset(_mutable_option_copy(item) for item in value)
    return value


def _strict_int(value: object, *, field_name: str, default: int) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        raise ConnectionProfileError(f"{field_name} must be an integer.")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if re.fullmatch(r"[+-]?\d+", stripped):
            return int(stripped)
    raise ConnectionProfileError(f"{field_name} must be an integer.")


def _text(value: object, *, field_name: str, required: bool = False) -> str:
    if value is None:
        result = ""
    elif isinstance(value, str):
        result = value.strip()
    else:
        raise ConnectionProfileError(f"{field_name} must be a string.")
    if required and not result:
        raise ConnectionProfileError(f"{field_name} is required.")
    return result


def _snowflake_account_format_valid(value: str) -> bool:
    if not value or ".snowflakecomputing.com" in value.lower():
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*", value))


@dataclass(frozen=True, slots=True)
class ConnectionProfile:
    """A deeply immutable, vendor-neutral set of connection parameters."""

    profile_id: str
    db_type: str
    host: str = field(default="", repr=False)
    database: str = field(default="", repr=False)
    username: str = field(default="", repr=False)
    password: str = field(default="", repr=False)
    connection_options: Mapping[str, Any] = field(default_factory=dict, repr=False)
    timeout_seconds: int = 30
    max_rows: int = 500

    __hash__: ClassVar[None] = None

    def __post_init__(self) -> None:
        """Normalize safe identifiers, validate fields, and freeze options."""

        profile_id = _text(self.profile_id, field_name="profile_id", required=True)
        db_type = _text(self.db_type, field_name="db_type", required=True).lower()
        host = _text(self.host, field_name="host")
        database = _text(self.database, field_name="database")
        username = _text(self.username, field_name="username")
        if not isinstance(self.password, str):
            raise ConnectionProfileError("password must be a string.")
        if not isinstance(self.connection_options, Mapping):
            raise ConnectionProfileError("connection_options must be a mapping.")

        timeout_seconds = _strict_int(self.timeout_seconds, field_name="timeout_seconds", default=30)
        max_rows = _strict_int(self.max_rows, field_name="max_rows", default=500)
        frozen_options = _freeze_option(dict(self.connection_options))

        if db_type not in SUPPORTED_CONNECTORS:
            supported = ", ".join(sorted(SUPPORTED_CONNECTORS))
            raise ConnectionProfileError(f"db_type must be one of: {supported}.")
        if db_type != "demo" and not host:
            raise ConnectionProfileError("host is required for the selected connector.")
        if host.startswith("<") and host.endswith(">"):
            raise ConnectionProfileError("host must be an actual host/account value, not a placeholder.")
        if len(host) >= 2 and host[0] == host[-1] and host[0] in {"'", '"'}:
            raise ConnectionProfileError("host must not include literal wrapping quotes.")
        if timeout_seconds <= 0:
            raise ConnectionProfileError("timeout_seconds must be greater than zero.")
        if max_rows <= 0:
            raise ConnectionProfileError("max_rows must be greater than zero.")
        if max_rows > 10_000:
            raise ConnectionProfileError("max_rows must not exceed 10000.")

        reserved = sorted(
            key for key in frozen_options if _normalized_option_key(key) in _RESERVED_CONNECTION_OPTION_KEYS
        )
        if reserved:
            raise ConnectionProfileError("connection_options cannot override profile-controlled fields.")
        port = frozen_options.get("port")
        if port is not None:
            try:
                int(port)
            except (TypeError, ValueError):
                raise ConnectionProfileError("connection_options.port must be an integer.") from None

        if db_type == "snowflake":
            if not username:
                raise ConnectionProfileError("username is required for the Snowflake connector.")
            if not _snowflake_account_format_valid(host):
                raise ConnectionProfileError(
                    "host for Snowflake must be an account identifier without a URL or hostname suffix."
                )
        if db_type == "sqlserver" and not database:
            raise ConnectionProfileError("database is required for the SQL Server connector.")
        if db_type == "sqlserver" and bool(username) != bool(self.password):
            raise ConnectionProfileError("username and password must either both be set or both be empty.")
        if db_type == "demo" and not database:
            database = "qa_demo"

        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "db_type", db_type)
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "database", database)
        object.__setattr__(self, "username", username)
        object.__setattr__(self, "connection_options", frozen_options)
        object.__setattr__(self, "timeout_seconds", timeout_seconds)
        object.__setattr__(self, "max_rows", max_rows)

    @property
    def normalized_profile_id(self) -> str:
        """Return the case-insensitive registry key for this profile."""

        return self.profile_id.casefold()

    def to_safe_dict(self) -> dict[str, object]:
        """Return profile metadata suitable for responses and logs."""

        return {
            "name": self.profile_id,
            "db_type": self.db_type,
            "host_present": bool(self.host),
            "database": self.database,
            "database_present": bool(self.database),
            "username_present": bool(self.username),
            "password_present": bool(self.password),
            "connection_options_present": bool(self.connection_options),
            "timeout_seconds": self.timeout_seconds,
            "max_rows": self.max_rows,
        }

    def connection_options_copy(self) -> dict[str, Any]:
        """Return a detached mutable options dictionary for a future driver."""

        return _mutable_option_copy(self.connection_options)
