"""Vendor-neutral models for bounded source-to-staging data transport.

These objects describe ephemeral batches while keeping database drivers and
credentials out of the transport contract.  Durable workflow evidence will be
added by the orchestration layer in a later slice; business rows themselves
must not be persisted in the control plane.
"""

from __future__ import annotations

from dataclasses import dataclass

from schemabridge.models.metadata import CanonicalType


def _identifier(value: str | None, name: str, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"{name} is invalid.")


def _positive(value: int, name: str, *, allow_zero: bool = False) -> None:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} is invalid.")


def _integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} is invalid.")


@dataclass(frozen=True, slots=True, kw_only=True)
class TransportRelation:
    """Identify a table without assuming a source or target database vendor."""

    catalog_name: str | None
    schema_name: str
    object_name: str

    def __post_init__(self) -> None:
        _identifier(self.catalog_name, "catalog_name", optional=True)
        _identifier(self.schema_name, "schema_name")
        _identifier(self.object_name, "object_name")


@dataclass(frozen=True, slots=True, kw_only=True)
class StagingColumn:
    """Describe one source-shaped column for a target-side staging table."""

    name: str
    canonical_type: CanonicalType
    nullable: bool | None
    character_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    datetime_precision: int | None = None

    def __post_init__(self) -> None:
        _identifier(self.name, "name")
        if not isinstance(self.canonical_type, CanonicalType):
            raise TypeError("canonical_type must be a CanonicalType.")
        if self.nullable is not None and not isinstance(self.nullable, bool):
            raise TypeError("nullable must be bool or None.")
        for name in (
            "character_length",
            "numeric_precision",
            "datetime_precision",
        ):
            value = getattr(self, name)
            if value is not None:
                _positive(value, name, allow_zero=True)
        # Numeric scales may be negative in some databases. This shared model
        # verifies the shape without imposing one vendor's supported range.
        if self.numeric_scale is not None:
            _integer(self.numeric_scale, "numeric_scale")


@dataclass(frozen=True, slots=True, kw_only=True)
class StagingTableDefinition:
    """Describe the managed landing table that a target writer must prepare."""

    relation: TransportRelation
    columns: tuple[StagingColumn, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.relation, TransportRelation):
            raise TypeError("relation must be a TransportRelation.")
        if not isinstance(self.columns, tuple) or not self.columns:
            raise ValueError("columns must be a non-empty tuple.")
        if not all(isinstance(column, StagingColumn) for column in self.columns):
            raise TypeError("columns must contain StagingColumn values.")
        names = tuple(column.name for column in self.columns)
        if len(set(names)) != len(names):
            raise ValueError("staging column names must be unique.")


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class DataBatch:
    """Carry one bounded, ordered group of rows between connectors in memory."""

    batch_number: int
    column_names: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]

    def __post_init__(self) -> None:
        _positive(self.batch_number, "batch_number")
        if not isinstance(self.column_names, tuple) or not self.column_names:
            raise ValueError("column_names must be a non-empty tuple.")
        for name in self.column_names:
            _identifier(name, "column_name")
        if len(set(self.column_names)) != len(self.column_names):
            raise ValueError("column_names must be unique.")
        if not isinstance(self.rows, tuple) or not self.rows:
            raise ValueError("rows must be a non-empty tuple.")
        width = len(self.column_names)
        if not all(isinstance(row, tuple) and len(row) == width for row in self.rows):
            raise ValueError("every row must match the declared column order.")

    @property
    def row_count(self) -> int:
        """Return the number of rows without exposing row values in repr output."""

        return len(self.rows)

    def __repr__(self) -> str:
        return (
            "DataBatch("
            f"batch_number={self.batch_number!r}, "
            f"column_count={len(self.column_names)!r}, "
            f"row_count={self.row_count!r})"
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class BatchWriteResult:
    """Return sanitized row-count evidence for one staging write."""

    batch_number: int
    rows_received: int
    rows_written: int

    def __post_init__(self) -> None:
        _positive(self.batch_number, "batch_number")
        _positive(self.rows_received, "rows_received")
        _positive(self.rows_written, "rows_written", allow_zero=True)
        if self.rows_written > self.rows_received:
            raise ValueError("rows_written cannot exceed rows_received.")


__all__ = [
    "BatchWriteResult",
    "DataBatch",
    "StagingColumn",
    "StagingTableDefinition",
    "TransportRelation",
]
