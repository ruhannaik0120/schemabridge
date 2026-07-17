"""Vendor-specific normalization into SchemaBridge metadata models."""

from normalizers.postgresql import normalize_postgresql_column
from normalizers.postgresql_discovery import (
    normalize_postgresql_check_constraint,
    normalize_postgresql_foreign_key,
    normalize_postgresql_key_constraint,
    normalize_postgresql_object,
    normalize_postgresql_schema,
    normalize_postgresql_table,
)
from normalizers.snowflake import normalize_snowflake_column
from normalizers.snowflake_discovery import (
    normalize_snowflake_check_constraint,
    normalize_snowflake_foreign_key,
    normalize_snowflake_key_constraint,
    normalize_snowflake_object,
    normalize_snowflake_schema,
    normalize_snowflake_table,
)

__all__ = [
    "normalize_postgresql_check_constraint",
    "normalize_postgresql_column",
    "normalize_postgresql_foreign_key",
    "normalize_postgresql_key_constraint",
    "normalize_postgresql_object",
    "normalize_postgresql_schema",
    "normalize_postgresql_table",
    "normalize_snowflake_check_constraint",
    "normalize_snowflake_column",
    "normalize_snowflake_foreign_key",
    "normalize_snowflake_key_constraint",
    "normalize_snowflake_object",
    "normalize_snowflake_schema",
    "normalize_snowflake_table",
]
