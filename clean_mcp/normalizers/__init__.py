"""Vendor-specific normalization into SchemaBridge metadata models."""

from normalizers.postgresql import normalize_postgresql_column
from normalizers.snowflake import normalize_snowflake_column

__all__ = ["normalize_postgresql_column", "normalize_snowflake_column"]
