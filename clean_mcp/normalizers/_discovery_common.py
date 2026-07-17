"""Shared pure helpers for vendor schema-discovery normalizers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any, Callable, TypeVar

from models.discovery import CoverageStatus, DiscoveryCoverage
from models.metadata import ColumnMetadata, _json_value, _sanitize_vendor_metadata
from normalizers._common import optional_int, optional_text, value_for


_ModelT = TypeVar("_ModelT")


def safe_vendor_metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    """Extract and centrally sanitize explicit vendor metadata from a catalog row."""

    value = value_for(row, "vendor_metadata")
    return _sanitize_vendor_metadata(value) if isinstance(value, Mapping) else {}


def _canonical_json(value: Any) -> str:
    return json.dumps(_json_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _metadata_richness(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, Mapping):
        return sum(_metadata_richness(item) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return sum(_metadata_richness(item) for item in value)
    return 1


def _preference_key(value: Any) -> tuple[int, str]:
    payload = value.to_dict() if hasattr(value, "to_dict") else _sanitize_vendor_metadata(value)
    return -_metadata_richness(payload), _canonical_json(payload)


def preferred_model(current: _ModelT, candidate: _ModelT) -> _ModelT:
    """Choose the richer normalized model, then the lexical canonical JSON minimum."""

    return min((current, candidate), key=_preference_key)


def deduplicate_models(models: Iterable[_ModelT], identity: Callable[[_ModelT], Any]) -> tuple[_ModelT, ...]:
    """Resolve duplicate model identities deterministically and independently of input order."""

    selected: dict[Any, _ModelT] = {}
    for model in models:
        key = identity(model)
        selected[key] = model if key not in selected else preferred_model(selected[key], model)
    return tuple(selected.values())


def deduplicate_and_sort_columns(columns: Iterable[ColumnMetadata]) -> tuple[ColumnMetadata, ...]:
    """Deduplicate columns and order them by ordinal then exact native name."""

    selected = deduplicate_models(columns, lambda item: (item.column_name, item.ordinal_position))
    return tuple(
        sorted(
            selected,
            key=lambda item: (
                item.ordinal_position is None,
                item.ordinal_position if item.ordinal_position is not None else 0,
                item.column_name,
            ),
        )
    )


def required_text(row: Mapping[str, Any], *names: str) -> str:
    """Get a required identifier while preserving its native value."""

    return optional_text(value_for(row, *names)) or ""


def has_field(row: Mapping[str, Any], *names: str) -> bool:
    """Tell an absent catalog field from a present field whose value is NULL."""

    available = {str(key).casefold() for key in row}
    return any(name.casefold() in available for name in names)


def optional_name_tuple(row: Mapping[str, Any], *names: str) -> tuple[str, ...]:
    """Read ordered identifier collections without sorting or case changes."""

    value = value_for(row, *names)
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(item if isinstance(item, str) else str(item) for item in value)
    if isinstance(value, (set, frozenset)):
        raise TypeError("Ordered constraint column collections must not be sets.")
    raise TypeError("Constraint column collections must be strings, lists, or tuples.")


def stable_constraint_identity(row: Mapping[str, Any], index: int, prefix: str) -> tuple[str, str]:
    """Use catalog identities first and a deterministic input fallback last."""

    for name in ("constraint_oid", "constraint_id", "constraint_name", "name"):
        value = optional_text(value_for(row, name))
        if value is not None:
            return prefix, value
    return prefix, f"input-{index}"


def aggregate_key_rows(rows: Iterable[Mapping[str, Any]], *, column_names: tuple[str, ...]) -> list[dict[str, Any]]:
    """Combine duplicate or one-row-per-column key catalog results."""

    grouped: dict[tuple[str, str], list[tuple[int | None, int, Mapping[str, Any], tuple[str, ...]]]] = {}
    for index, row in enumerate(rows):
        identity = stable_constraint_identity(row, index, "key")
        columns = optional_name_tuple(row, *column_names)
        if not columns:
            single = optional_text(value_for(row, "column_name", "local_column_name"))
            columns = (single,) if single is not None else ()
        sequence = optional_int(value_for(row, "key_sequence", "ordinal_position", "column_position"))
        grouped.setdefault(identity, []).append((sequence, index, row, columns))

    aggregated: list[dict[str, Any]] = []
    for fragments in grouped.values():
        representative = min((fragment[2] for fragment in fragments), key=_preference_key)
        first = dict(representative)
        ordered = sorted(
            fragments,
            key=lambda item: (
                item[0] is None,
                item[0] if item[0] is not None else 0,
                item[3],
                _canonical_json(_sanitize_vendor_metadata(item[2])),
            ),
        )
        values: list[str] = []
        for _, _, _, columns in ordered:
            for column in columns:
                if column not in values:
                    values.append(column)
        first["columns"] = tuple(values)
        aggregated.append(first)
    return aggregated


def aggregate_foreign_key_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Combine duplicate or one-row-per-column foreign-key catalog results."""

    grouped: dict[tuple[str, str], list[tuple[int | None, int, Mapping[str, Any], tuple[str, ...], tuple[str, ...]]]] = {}
    for index, row in enumerate(rows):
        identity = stable_constraint_identity(row, index, "foreign-key")
        local = optional_name_tuple(row, "local_columns", "columns", "column_names")
        referenced = optional_name_tuple(row, "referenced_columns", "foreign_column_names")
        if not local:
            local_name = optional_text(value_for(row, "local_column_name", "column_name"))
            local = (local_name,) if local_name is not None else ()
        if not referenced:
            referenced_name = optional_text(value_for(row, "referenced_column_name", "foreign_column_name"))
            referenced = (referenced_name,) if referenced_name is not None else ()
        sequence = optional_int(value_for(row, "key_sequence", "ordinal_position", "column_position"))
        grouped.setdefault(identity, []).append((sequence, index, row, local, referenced))

    aggregated: list[dict[str, Any]] = []
    for fragments in grouped.values():
        representative = min((fragment[2] for fragment in fragments), key=_preference_key)
        first = dict(representative)
        ordered = sorted(
            fragments,
            key=lambda item: (
                item[0] is None,
                item[0] if item[0] is not None else 0,
                item[3],
                item[4],
                _canonical_json(_sanitize_vendor_metadata(item[2])),
            ),
        )
        local_columns: list[str] = []
        referenced_columns: list[str] = []
        for _, _, _, local, referenced in ordered:
            for value in local:
                if value not in local_columns:
                    local_columns.append(value)
            for value in referenced:
                if value not in referenced_columns:
                    referenced_columns.append(value)
        first["local_columns"] = tuple(local_columns)
        first["referenced_columns"] = tuple(referenced_columns)
        aggregated.append(first)
    return aggregated


def _rows_by_constraint_identity(
    rows: tuple[Mapping[str, Any], ...], prefix: str
) -> dict[tuple[str, str], list[Mapping[str, Any]]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for index, row in enumerate(rows):
        grouped.setdefault(stable_constraint_identity(row, index, prefix), []).append(row)
    return grouped


def key_constraint_coverage(rows: tuple[Mapping[str, Any], ...] | None) -> CoverageStatus:
    """Assess whether every discovered key has complete ordered membership."""

    if rows is None:
        return CoverageStatus.UNAVAILABLE
    if not rows:
        return CoverageStatus.COMPLETE
    for fragments in _rows_by_constraint_identity(rows, "key").values():
        aggregated = aggregate_key_rows(fragments, column_names=("columns", "column_names", "local_columns"))[0]
        columns = optional_name_tuple(aggregated, "columns")
        if not columns:
            return CoverageStatus.PARTIAL
        explicit_memberships = [
            optional_name_tuple(row, "columns", "column_names", "local_columns")
            for row in fragments
            if has_field(row, "columns", "column_names", "local_columns")
        ]
        if any(candidate == columns for candidate in explicit_memberships):
            continue
        if len(columns) == 1 and any(
            optional_text(value_for(row, "column_name", "local_column_name")) == columns[0]
            for row in fragments
        ):
            continue
        membership_sequences: dict[str, set[int]] = {}
        membership_missing = False
        for row in fragments:
            values = optional_name_tuple(row, "columns", "column_names", "local_columns")
            if not values:
                single = optional_text(value_for(row, "column_name", "local_column_name"))
                values = (single,) if single is not None else ()
            sequence = optional_int(value_for(row, "key_sequence", "ordinal_position", "column_position"))
            if not values or sequence is None:
                membership_missing = True
                continue
            for value in values:
                membership_sequences.setdefault(value, set()).add(sequence)
        if membership_missing or any(column not in membership_sequences for column in columns):
            return CoverageStatus.PARTIAL
    return CoverageStatus.COMPLETE


def foreign_key_coverage(rows: tuple[Mapping[str, Any], ...] | None) -> CoverageStatus:
    """Assess ordered local and referenced membership for every discovered FK."""

    if rows is None:
        return CoverageStatus.UNAVAILABLE
    if not rows:
        return CoverageStatus.COMPLETE
    for fragments in _rows_by_constraint_identity(rows, "foreign-key").values():
        aggregated = aggregate_foreign_key_rows(fragments)[0]
        local_columns = optional_name_tuple(aggregated, "local_columns")
        referenced_columns = optional_name_tuple(aggregated, "referenced_columns")
        if not local_columns or not referenced_columns or len(local_columns) != len(referenced_columns):
            return CoverageStatus.PARTIAL
        explicit_memberships = [
            (
                optional_name_tuple(row, "local_columns", "columns", "column_names"),
                optional_name_tuple(row, "referenced_columns", "foreign_column_names"),
            )
            for row in fragments
            if has_field(row, "local_columns", "columns", "column_names")
            or has_field(row, "referenced_columns", "foreign_column_names")
        ]
        if any(local == local_columns and referenced == referenced_columns for local, referenced in explicit_memberships):
            continue
        if len(local_columns) == 1 and any(
            optional_text(value_for(row, "local_column_name", "column_name")) == local_columns[0]
            and optional_text(value_for(row, "referenced_column_name", "foreign_column_name"))
            == referenced_columns[0]
            for row in fragments
        ):
            continue
        pairs_with_sequence: set[tuple[str, str]] = set()
        membership_missing = False
        for row in fragments:
            local = optional_name_tuple(row, "local_columns", "columns", "column_names")
            referenced = optional_name_tuple(row, "referenced_columns", "foreign_column_names")
            if not local:
                single_local = optional_text(value_for(row, "local_column_name", "column_name"))
                local = (single_local,) if single_local is not None else ()
            if not referenced:
                single_referenced = optional_text(value_for(row, "referenced_column_name", "foreign_column_name"))
                referenced = (single_referenced,) if single_referenced is not None else ()
            sequence = optional_int(value_for(row, "key_sequence", "ordinal_position", "column_position"))
            if not local or not referenced or len(local) != len(referenced) or sequence is None:
                membership_missing = True
                continue
            pairs_with_sequence.update(zip(local, referenced))
        required_pairs = set(zip(local_columns, referenced_columns))
        if membership_missing or not required_pairs.issubset(pairs_with_sequence):
            return CoverageStatus.PARTIAL
    return CoverageStatus.COMPLETE


def default_coverage(
    *,
    object_is_view: bool,
    primary_key_rows: object,
    unique_constraint_rows: object,
    foreign_key_rows: object,
    check_constraint_rows: object,
    object_row: Mapping[str, Any],
) -> DiscoveryCoverage:
    """Make only caller-supplied result sets complete; unknown inputs stay unknown."""

    has_comment = has_field(object_row, "comment", "table_comment", "object_comment")
    has_row_count = has_field(object_row, "estimated_row_count", "row_count", "reltuples")
    has_partitioning = has_field(object_row, "is_partitioned", "partitioning_expression")
    has_clustering = has_field(object_row, "clustering_expression", "clustering_key")
    has_view_definition = has_field(object_row, "view_definition")
    return DiscoveryCoverage(
        columns=CoverageStatus.COMPLETE,
        primary_key=CoverageStatus.COMPLETE if primary_key_rows is not None else CoverageStatus.UNAVAILABLE,
        unique_constraints=CoverageStatus.COMPLETE if unique_constraint_rows is not None else CoverageStatus.UNAVAILABLE,
        foreign_keys=CoverageStatus.COMPLETE if foreign_key_rows is not None else CoverageStatus.UNAVAILABLE,
        check_constraints=CoverageStatus.COMPLETE if check_constraint_rows is not None else CoverageStatus.UNAVAILABLE,
        comments=CoverageStatus.COMPLETE if has_comment else CoverageStatus.UNAVAILABLE,
        estimated_row_count=CoverageStatus.COMPLETE if has_row_count else CoverageStatus.UNAVAILABLE,
        view_definition=(CoverageStatus.COMPLETE if has_view_definition else CoverageStatus.UNAVAILABLE)
        if object_is_view
        else CoverageStatus.NOT_APPLICABLE,
        partitioning=CoverageStatus.COMPLETE if has_partitioning else CoverageStatus.UNAVAILABLE,
        clustering=CoverageStatus.COMPLETE if has_clustering else CoverageStatus.NOT_APPLICABLE,
    )
