"""Vendor-neutral metadata models used by SchemaBridge."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any


class CanonicalType(str, Enum):
    """Database-independent column type families."""

    STRING = "STRING"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    BINARY = "BINARY"
    SEMI_STRUCTURED = "SEMI_STRUCTURED"
    UNKNOWN = "UNKNOWN"


_SENSITIVE_METADATA_KEYS = frozenset(
    {
        "access_token",
        "account_identifier",
        "api_key",
        "credential",
        "credentials",
        "connection_string",
        "driver_error",
        "dsn",
        "exception",
        "exception_repr",
        "host",
        "hostname",
        "password",
        "passwd",
        "private_key",
        "profile_json",
        "pwd",
        "raw_driver_error",
        "raw_exception",
        "raw_profile_json",
        "refresh_token",
        "secret",
        "stack_trace",
        "token",
        "traceback",
        "user",
        "username",
    }
)

_REDACTED_EXCEPTION_VALUE = "[REDACTED_EXCEPTION]"


def _normalized_metadata_key(value: str) -> str:
    """Normalize case and separators without matching substrings of larger words."""

    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.casefold())).strip("_")


def _freeze(value: Any) -> Any:
    """Recursively isolate mutable vendor metadata from the frozen model."""

    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return value


def _sanitize_metadata_value(value: Any) -> Any:
    if isinstance(value, BaseException):
        return _REDACTED_EXCEPTION_VALUE
    if isinstance(value, Mapping):
        return _sanitize_vendor_metadata(value)
    if isinstance(value, list):
        return [_sanitize_metadata_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_metadata_value(item) for item in value)
    if isinstance(value, set):
        return {_sanitize_metadata_value(item) for item in value}
    if isinstance(value, frozenset):
        return frozenset(_sanitize_metadata_value(item) for item in value)
    return value


def _sanitize_vendor_metadata(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return a detached copy within the narrow vendor-metadata security boundary.

    Only exact, case-insensitive security/runtime field names are removed after
    normalizing separators. Larger unrelated words containing those tokens are
    preserved. Exception objects are replaced recursively with a fixed safe value.
    """

    sanitized: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(key, BaseException):
            continue
        key_text = str(key)
        if _normalized_metadata_key(key_text) in _SENSITIVE_METADATA_KEYS:
            continue
        sanitized[key_text] = _sanitize_metadata_value(item)
    return sanitized


def _json_value(value: Any) -> Any:
    """Convert frozen metadata and driver scalars into JSON-safe values."""

    if isinstance(value, _MetadataModel):
        return value.to_dict()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        items = [_json_value(item) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


class _MetadataModel:
    """Marker for canonical models that are safe to serialize recursively."""

    def to_dict(self) -> dict[str, Any]:
        raise NotImplementedError


def _validate_identifier(value: str | None, field_name: str, *, required: bool) -> None:
    """Validate identifier structure without restricting quoted-name content."""

    if value is None:
        if required:
            raise ValueError(f"{field_name} must be a non-empty string.")
        return
    if not isinstance(value, str):
        expected = "a non-empty string" if required else "a string or None"
        raise TypeError(f"{field_name} must be {expected}.")
    if required and not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    if "\x00" in value:
        raise ValueError(f"{field_name} must not contain NUL characters.")


def _validate_non_negative(value: int | None, field_name: str) -> None:
    if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
        raise TypeError(f"{field_name} must be an integer or None.")
    if value is not None and value < 0:
        raise ValueError(f"{field_name} must not be negative.")


@dataclass(frozen=True, slots=True)
class ColumnMetadata(_MetadataModel):
    """Immutable representation of one database column."""

    catalog_name: str | None
    schema_name: str | None
    table_name: str
    column_name: str
    ordinal_position: int | None
    native_type: str | None
    canonical_type: CanonicalType
    nullable: bool | None
    character_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    datetime_precision: int | None
    is_primary_key: bool | None = None
    is_foreign_key: bool | None = None
    vendor_metadata: Mapping[str, Any] = field(default_factory=dict, repr=False)
    default_expression: str | None = None
    comment: str | None = None
    collation: str | None = None
    is_identity: bool | None = None
    identity_generation: str | None = None
    is_auto_increment: bool | None = None
    is_generated: bool | None = None
    generation_expression: str | None = None
    is_unique_key: bool | None = None
    array_dimensions: int | None = None
    element_native_type: str | None = None
    element_canonical_type: CanonicalType | None = None

    def __post_init__(self) -> None:
        """Validate identity and freeze a detached copy of vendor metadata."""

        _validate_identifier(self.table_name, "table_name", required=True)
        _validate_identifier(self.column_name, "column_name", required=True)
        _validate_identifier(self.catalog_name, "catalog_name", required=False)
        _validate_identifier(self.schema_name, "schema_name", required=False)
        _validate_non_negative(self.ordinal_position, "ordinal_position")
        _validate_non_negative(self.character_length, "character_length")
        _validate_non_negative(self.numeric_precision, "numeric_precision")
        _validate_non_negative(self.datetime_precision, "datetime_precision")
        _validate_non_negative(self.array_dimensions, "array_dimensions")
        if not isinstance(self.canonical_type, CanonicalType):
            raise TypeError("canonical_type must be a CanonicalType.")
        if self.element_canonical_type is not None and not isinstance(self.element_canonical_type, CanonicalType):
            raise TypeError("element_canonical_type must be a CanonicalType or None.")
        if not isinstance(self.vendor_metadata, Mapping):
            raise TypeError("vendor_metadata must be a mapping.")
        object.__setattr__(self, "vendor_metadata", _freeze(_sanitize_vendor_metadata(self.vendor_metadata)))

    def to_dict(self) -> dict[str, Any]:
        """Return a normal dictionary that can be passed directly to json.dumps."""

        return {
            "catalog_name": self.catalog_name,
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "ordinal_position": self.ordinal_position,
            "native_type": self.native_type,
            "canonical_type": self.canonical_type.value,
            "nullable": self.nullable,
            "character_length": self.character_length,
            "numeric_precision": self.numeric_precision,
            "numeric_scale": self.numeric_scale,
            "datetime_precision": self.datetime_precision,
            "is_primary_key": self.is_primary_key,
            "is_foreign_key": self.is_foreign_key,
            "vendor_metadata": _json_value(self.vendor_metadata),
            "default_expression": self.default_expression,
            "comment": self.comment,
            "collation": self.collation,
            "is_identity": self.is_identity,
            "identity_generation": self.identity_generation,
            "is_auto_increment": self.is_auto_increment,
            "is_generated": self.is_generated,
            "generation_expression": self.generation_expression,
            "is_unique_key": self.is_unique_key,
            "array_dimensions": self.array_dimensions,
            "element_native_type": self.element_native_type,
            "element_canonical_type": (
                self.element_canonical_type.value if self.element_canonical_type is not None else None
            ),
        }
