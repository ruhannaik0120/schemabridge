"""Select when a migration is large enough to justify Spark processing.

This module intentionally has no PySpark import.  A normal SchemaBridge
installation stays lightweight; a later transport runner will load PySpark
only when this opt-in policy selects the Spark path.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class SparkConfigurationError(ValueError):
    """Raised when Spark's optional runtime settings are malformed."""


def _positive_integer(value: str | None, *, name: str, default: int) -> int:
    if value is None or not value.strip():
        return default
    try:
        parsed = int(value)
    except ValueError:
        raise SparkConfigurationError(f"{name} must be a positive integer.") from None
    if parsed <= 0:
        raise SparkConfigurationError(f"{name} must be a positive integer.")
    return parsed


@dataclass(frozen=True, slots=True)
class SparkTransportSettings:
    """Immutable opt-in policy for distributed large-table transport.

    ``minimum_source_rows`` is the automatic routing threshold. An unknown
    source row estimate stays on the normal safe path.
    """

    minimum_source_rows: int = 1_000_000
    num_partitions: int = 4
    master: str | None = None
    jars_packages: str | None = None
    application_name: str = "SchemaBridge"

    def __post_init__(self) -> None:
        if (
            isinstance(self.minimum_source_rows, bool)
            or not isinstance(self.minimum_source_rows, int)
            or self.minimum_source_rows <= 0
        ):
            raise ValueError("minimum_source_rows must be a positive integer.")
        if (
            isinstance(self.num_partitions, bool)
            or not isinstance(self.num_partitions, int)
            or self.num_partitions <= 0
        ):
            raise ValueError("num_partitions must be a positive integer.")
        for value, name in (
            (self.master, "master"),
            (self.jars_packages, "jars_packages"),
            (self.application_name, "application_name"),
        ):
            if value is not None and (not isinstance(value, str) or not value.strip() or "\x00" in value):
                raise ValueError(f"{name} is invalid.")

    @classmethod
    def from_environment(cls) -> "SparkTransportSettings":
        """Load optional settings without reading database credentials."""

        master = os.getenv("SCHEMABRIDGE_SPARK_MASTER")
        jars_packages = os.getenv("SCHEMABRIDGE_SPARK_JARS_PACKAGES")
        return cls(
            minimum_source_rows=_positive_integer(
                os.getenv("SCHEMABRIDGE_SPARK_MINIMUM_SOURCE_ROWS"),
                name="SCHEMABRIDGE_SPARK_MINIMUM_SOURCE_ROWS",
                default=1_000_000,
            ),
            num_partitions=_positive_integer(
                os.getenv("SCHEMABRIDGE_SPARK_NUM_PARTITIONS"),
                name="SCHEMABRIDGE_SPARK_NUM_PARTITIONS",
                default=4,
            ),
            master=master.strip() if master and master.strip() else None,
            jars_packages=jars_packages.strip() if jars_packages and jars_packages.strip() else None,
            application_name=os.getenv("SCHEMABRIDGE_SPARK_APPLICATION_NAME", "SchemaBridge").strip(),
        )

    def should_use_spark(self, estimated_row_count: int | None) -> bool:
        """Choose Spark only after an explicit opt-in and a large estimate."""

        return (
            isinstance(estimated_row_count, int)
            and not isinstance(estimated_row_count, bool)
            and estimated_row_count >= self.minimum_source_rows
        )

    def has_package(self, fragment: str) -> bool:
        """Check configured Spark package coordinates without loading Spark."""

        if not isinstance(fragment, str) or not fragment.strip():
            raise ValueError("fragment must be non-empty text.")
        return bool(
            self.jars_packages
            and fragment.casefold() in self.jars_packages.casefold()
        )
