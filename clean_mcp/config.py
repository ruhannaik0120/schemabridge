"""Generic runtime configuration for the MCP execution framework."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from dotenv import dotenv_values, load_dotenv

from connectors.factory import SUPPORTED_CONNECTORS

_DOTENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=_DOTENV_PATH)

CONFIG_ENV_KEYS = frozenset(
    {
        "DB_TYPE",
        "DB_HOST",
        "DB_DATABASE",
        "DB_USERNAME",
        "DB_PASSWORD",
        "DB_CONNECTION_OPTIONS",
        "DB_TIMEOUT_SECONDS",
        "DB_MAX_ROWS",
        "DB_ACTIVE_PROFILE",
        "DB_PROFILES_JSON",
        "LOG_LEVEL",
    }
)

_SENSITIVE_OPTION_KEY_PARTS = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "privatekey",
        "passphrase",
        "credential",
        "apikey",
        "authorization",
        "connectionstring",
    }
)
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


def _normalize_text(value: str | None, default: str = "") -> str:
    """Trim an optional string and substitute a default for blank values."""

    if value is None:
        return default
    return value.strip() or default


def has_placeholder_delimiters(value: str) -> bool:
    """Return true when a value still looks like a documented placeholder."""

    stripped = value.strip()
    return len(stripped) >= 2 and stripped[0] == "<" and stripped[-1] == ">"


def has_wrapping_quotes(value: str) -> bool:
    """Return true when a value includes literal shell/documentation quotes."""

    stripped = value.strip()
    return len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}


def snowflake_account_format_valid(value: str) -> bool:
    """Validate connector-style account names and locator/region identifiers."""

    normalized = value.strip()
    if not normalized or ".snowflakecomputing.com" in normalized.lower():
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*", normalized))


def _as_int(value: str | None, default: int) -> int:
    """Parse an integer environment value with a clear configuration error."""

    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"Expected an integer value, got: {value!r}") from exc


def _as_dict(value: str | None) -> dict[str, object]:
    """Parse JSON connection options and require an object-shaped value."""

    if value is None or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ConfigError("DB_CONNECTION_OPTIONS must be valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise ConfigError("DB_CONNECTION_OPTIONS must decode to a JSON object.")
    return parsed


def _normalized_option_key(key: object) -> str:
    """Normalize driver option names for security comparisons."""

    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def _is_sensitive_option_key(key: object) -> bool:
    normalized_key = _normalized_option_key(key)
    return any(part in normalized_key for part in _SENSITIVE_OPTION_KEY_PARTS)


def _redact_option_value(value: object, key: object = "") -> object:
    if _is_sensitive_option_key(key):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(item_key): _redact_option_value(item_value, item_key) for item_key, item_value in value.items()}
    if isinstance(value, list):
        return [_redact_option_value(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_option_value(item) for item in value]
    return value


def _redact_connection_options(options: dict[str, object]) -> dict[str, object]:
    """Recursively copy connection options while replacing secret values."""

    # Diagnostics may be shown to an AI client, so secret-like option values
    # are replaced before configuration leaves the process boundary.
    return {str(key): _redact_option_value(value, key) for key, value in options.items()}


def _secret_option_values(value: object, key: object = "") -> list[str]:
    """Collect configured secret values so exception text can be scrubbed."""

    if _is_sensitive_option_key(key):
        return [str(value)] if value not in (None, "") else []
    if isinstance(value, dict):
        secrets: list[str] = []
        for item_key, item_value in value.items():
            secrets.extend(_secret_option_values(item_value, item_key))
        return secrets
    if isinstance(value, (list, tuple)):
        secrets = []
        for item in value:
            secrets.extend(_secret_option_values(item))
        return secrets
    return []


class ConfigError(ValueError):
    """Raised when runtime configuration fails validation."""


@dataclass(frozen=True, slots=True)
class ConnectionConfig:
    """Resolved generic connection settings for the active database."""

    db_type: str
    host: str
    database: str
    username: str = ""
    password: str = ""
    connection_options: dict[str, object] | None = None
    timeout_seconds: int = 30
    max_rows: int = 500

    def safe_dict(self) -> dict[str, object]:
        """Return this connection profile in a form safe for agent responses."""

        return {
            "db_type": self.db_type,
            "host": "[CONFIGURED]" if self.host else "",
            "database": self.database,
            "username": "[CONFIGURED]" if self.username else "",
            "password": "",
            "connection_options": _redact_connection_options(self.connection_options or {}),
            "timeout_seconds": self.timeout_seconds,
            "max_rows": self.max_rows,
        }


class Config:
    """Central configuration surface for the MCP server."""

    DB_TYPE: ClassVar[str] = ""
    HOST: ClassVar[str] = ""
    DATABASE: ClassVar[str] = ""
    USERNAME: ClassVar[str] = ""
    PASSWORD: ClassVar[str] = ""
    CONNECTION_OPTIONS: ClassVar[dict[str, object]] = {}
    GLOBAL_MAX_ROWS: ClassVar[int] = 500
    GLOBAL_TIMEOUT_SECONDS: ClassVar[int] = 30
    LOG_LEVEL: ClassVar[str] = "INFO"

    @classmethod
    def reload_dotenv(cls, *, override: bool = True) -> "Config":
        """Replace recognized runtime settings from the local .env file."""

        if not _DOTENV_PATH.is_file():
            raise ConfigError(f"Local configuration file was not found: {_DOTENV_PATH}.")
        values = dotenv_values(_DOTENV_PATH)
        if override:
            # The local file is authoritative for recognized settings. Clearing
            # absent keys prevents a removed credential from remaining in memory.
            for key in CONFIG_ENV_KEYS:
                os.environ.pop(key, None)
        for key in CONFIG_ENV_KEYS:
            value = values.get(key)
            if value is not None and (override or key not in os.environ):
                os.environ[key] = value
        return cls.load()

    @classmethod
    def load(cls) -> "Config":
        """Refresh process-wide settings from environment variables."""

        # All environment access is centralized here so connectors receive one
        # consistent snapshot instead of interpreting raw strings themselves.
        cls.DB_TYPE = _normalize_text(os.getenv("DB_TYPE")).lower()
        cls.HOST = _normalize_text(os.getenv("DB_HOST"))
        cls.DATABASE = _normalize_text(os.getenv("DB_DATABASE"))
        cls.USERNAME = _normalize_text(os.getenv("DB_USERNAME"))
        cls.PASSWORD = os.getenv("DB_PASSWORD", "")
        cls.CONNECTION_OPTIONS = _as_dict(os.getenv("DB_CONNECTION_OPTIONS"))
        cls.GLOBAL_MAX_ROWS = _as_int(os.getenv("DB_MAX_ROWS"), 500)
        cls.GLOBAL_TIMEOUT_SECONDS = _as_int(os.getenv("DB_TIMEOUT_SECONDS"), 30)
        cls.LOG_LEVEL = _normalize_text(os.getenv("LOG_LEVEL"), "INFO").upper()
        return cls

    @classmethod
    def validate(cls) -> "Config":
        """Load settings and reject all detected configuration problems."""

        cls.load()

        # Collect every problem and report them together. This makes setup much
        # faster than failing one environment variable at a time.
        errors: list[str] = []
        if not cls.DB_TYPE:
            errors.append("DB_TYPE is required.")
        elif cls.DB_TYPE not in SUPPORTED_CONNECTORS:
            supported = ", ".join(sorted(SUPPORTED_CONNECTORS))
            errors.append(f"DB_TYPE must be one of: {supported}.")

        if cls.DB_TYPE != "demo" and not cls.HOST:
            errors.append("DB_HOST is required for the selected connector.")
        if cls.HOST:
            if has_placeholder_delimiters(cls.HOST):
                errors.append("DB_HOST must be an actual host/account value, not a <placeholder>.")
            if has_wrapping_quotes(cls.HOST):
                errors.append("DB_HOST must not include literal wrapping quotes.")

        if cls.LOG_LEVEL not in logging._nameToLevel:
            errors.append("LOG_LEVEL must be a valid logging level.")
        if cls.GLOBAL_MAX_ROWS <= 0:
            errors.append("DB_MAX_ROWS must be greater than zero.")
        elif cls.GLOBAL_MAX_ROWS > 10_000:
            errors.append("DB_MAX_ROWS must not exceed 10000.")
        if cls.GLOBAL_TIMEOUT_SECONDS <= 0:
            errors.append("DB_TIMEOUT_SECONDS must be greater than zero.")

        errors.extend(cls._validate_connection_options())
        errors.extend(cls._validate_connector_requirements())

        if errors:
            raise ConfigError("Configuration validation failed: " + " ".join(errors))

        return cls

    @classmethod
    def _validate_connection_options(cls) -> list[str]:
        """Validate generic options shared across connector implementations."""

        errors: list[str] = []
        reserved = sorted(
            str(key)
            for key in cls.CONNECTION_OPTIONS
            if _normalized_option_key(key) in _RESERVED_CONNECTION_OPTION_KEYS
        )
        if reserved:
            errors.append(
                "DB_CONNECTION_OPTIONS cannot override profile-controlled fields: "
                + ", ".join(reserved)
                + "."
            )
        port = cls.CONNECTION_OPTIONS.get("port")
        if port is not None:
            try:
                int(port)
            except (TypeError, ValueError):
                errors.append("DB_CONNECTION_OPTIONS.port must be an integer.")
        return errors

    @classmethod
    def _validate_connector_requirements(cls) -> list[str]:
        """Apply only the required fields that vary by selected backend."""

        errors: list[str] = []
        if cls.DB_TYPE == "sqlserver" and not cls.DATABASE:
            errors.append("DB_DATABASE is required for the SQL Server connector.")
        if cls.DB_TYPE == "sqlserver" and bool(cls.USERNAME) != bool(cls.PASSWORD):
            errors.append("DB_USERNAME and DB_PASSWORD must either both be set or both be empty.")
        if cls.DB_TYPE == "snowflake" and not cls.USERNAME:
            errors.append("DB_USERNAME is required for the Snowflake connector.")
        if cls.DB_TYPE == "snowflake" and cls.HOST and not snowflake_account_format_valid(cls.HOST):
            errors.append(
                "DB_HOST for Snowflake must be an account identifier without a URL or snowflakecomputing.com suffix."
            )
        if cls.DB_TYPE == "demo" and not cls.DATABASE:
            cls.DATABASE = "qa_demo"
        return errors

    @classmethod
    def connection_config(cls) -> ConnectionConfig:
        """Build the neutral configuration object consumed by connectors."""

        if not cls.DB_TYPE:
            cls.load()
        # Connectors receive one neutral profile instead of reading environment
        # variables independently, which keeps backend logic interchangeable.
        return ConnectionConfig(
            db_type=cls.DB_TYPE,
            host=cls.HOST,
            database=cls.DATABASE,
            username=cls.USERNAME,
            password=cls.PASSWORD,
            connection_options=dict(cls.CONNECTION_OPTIONS),
            timeout_seconds=cls.GLOBAL_TIMEOUT_SECONDS,
            max_rows=cls.GLOBAL_MAX_ROWS,
        )

    @classmethod
    def as_dict(cls) -> dict[str, object]:
        """Return redacted effective settings for internal structured output."""

        if not cls.DB_TYPE:
            cls.load()

        return {
            "db_type": cls.DB_TYPE,
            "host": "[CONFIGURED]" if cls.HOST else "",
            "database": cls.DATABASE,
            "username": "[CONFIGURED]" if cls.USERNAME else "",
            "password": "",
            "connection_options": _redact_connection_options(dict(cls.CONNECTION_OPTIONS)),
            "global_max_rows": cls.GLOBAL_MAX_ROWS,
            "global_timeout_seconds": cls.GLOBAL_TIMEOUT_SECONDS,
            "log_level": cls.LOG_LEVEL,
        }

    @classmethod
    def diagnostics(cls) -> dict[str, object]:
        """Return troubleshooting metadata without returning credential values."""

        if not cls.DB_TYPE:
            cls.load()

        return {
            "db_type": cls.DB_TYPE or "(not set)",
            "host_present": bool(cls.HOST),
            "database": cls.DATABASE,
            "username_present": bool(cls.USERNAME),
            "password_present": bool(cls.PASSWORD),
            "timeout_seconds": cls.GLOBAL_TIMEOUT_SECONDS,
            "max_rows": cls.GLOBAL_MAX_ROWS,
            "connection_options": _redact_connection_options(dict(cls.CONNECTION_OPTIONS)),
            "configuration_source": (
                "environment_with_local_dotenv_fallback" if _DOTENV_PATH.is_file() else "environment"
            ),
            "supported_connectors": sorted(SUPPORTED_CONNECTORS),
        }

    @classmethod
    def redact_text(cls, value: object) -> str:
        """Remove configured credential values from an external error message."""

        text = str(value)
        secrets = [cls.PASSWORD, *_secret_option_values(cls.CONNECTION_OPTIONS)]
        for secret in sorted(set(secrets), key=len, reverse=True):
            if secret:
                text = text.replace(secret, "[REDACTED]")
        text = re.sub(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+", "[REDACTED]", text)
        text = re.sub(
            r"(?i)\b(password|passwd|pwd|token|secret|api[_-]?key|authorization)\s*[:=]\s*([^\s,;]+)",
            r"\1=[REDACTED]",
            text,
        )
        text = re.sub(r"(?i)([a-z][a-z0-9+.-]*://)[^/@\s:]+:[^/@\s]+@", r"\1[REDACTED]@", text)
        return text


# Load defaults at import time; startup validation still performs the strict gate.
Config.load()
