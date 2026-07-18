"""Deterministic, explainable suggestions between two canonical tables."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from models.discovery import TableMetadata
from models.mapping import (
    ColumnCompatibility,
    ColumnMappingSuggestion,
    MappingDecision,
    MappingEvidence,
    TableMappingIdentity,
    TableMappingPlan,
)
from models.metadata import CanonicalType, ColumnMetadata


_SUGGESTION_THRESHOLD = 0.72
_AMBIGUITY_MARGIN = 0.08
_WEIGHTS = {
    "EXACT_NATIVE_NAME": 0.58,
    "CASE_INSENSITIVE_NAME": 0.50,
    "NORMALIZED_NAME": 0.46,
    "TOKEN_NAME_MATCH": 0.18,
    "EXACT_CANONICAL_TYPE": 0.28,
    "SAFE_TYPE_COMPATIBILITY": 0.20,
    "LOSSY_TYPE_COMPATIBILITY": 0.04,
    "ORDINAL_PROXIMITY": 0.04,
    "NULLABILITY_COMPATIBLE": 0.04,
    "NULLABILITY_MISMATCH": -0.22,
    "LENGTH_OR_PRECISION_COMPATIBLE": 0.04,
}
_COMPATIBILITY_PRIORITY = {
    ColumnCompatibility.EXACT: 4,
    ColumnCompatibility.SAFE: 3,
    ColumnCompatibility.LOSSY: 2,
    ColumnCompatibility.UNKNOWN: 1,
    ColumnCompatibility.INCOMPATIBLE: 0,
}


def _comparison_tokens(value: str) -> tuple[str, ...]:
    """Normalize only known presentation separators for deterministic comparison."""
    camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value)
    return tuple(token.casefold() for token in re.split(r"[_\-\s]+", camel_split) if token)


def _ordered_columns(columns: Iterable[ColumnMetadata]) -> tuple[ColumnMetadata, ...]:
    return tuple(
        sorted(
            columns,
            key=lambda item: (
                item.ordinal_position is None,
                item.ordinal_position if item.ordinal_position is not None else 0,
                item.column_name,
            ),
        )
    )


def _dimension_compatibility(source: ColumnMetadata, target: ColumnMetadata) -> tuple[ColumnCompatibility | None, bool]:
    """Return a narrowing classification and whether dimensions prove compatibility."""
    checks = (
        (source.character_length, target.character_length),
        (source.numeric_precision, target.numeric_precision),
        (source.numeric_scale, target.numeric_scale),
        (source.datetime_precision, target.datetime_precision),
    )
    known = [(left, right) for left, right in checks if left is not None and right is not None]
    if any(right < left for left, right in known):
        return ColumnCompatibility.LOSSY, False
    return None, bool(known)


def _type_compatibility(source: ColumnMetadata, target: ColumnMetadata) -> tuple[ColumnCompatibility, bool]:
    """Classify only conservative canonical-type conversions."""
    source_type = source.canonical_type
    target_type = target.canonical_type
    if CanonicalType.UNKNOWN in {source_type, target_type}:
        return ColumnCompatibility.UNKNOWN, False
    if source_type is target_type:
        narrowing, dimensions_compatible = _dimension_compatibility(source, target)
        return narrowing or ColumnCompatibility.EXACT, dimensions_compatible
    if source_type is CanonicalType.INTEGER and target_type is CanonicalType.DECIMAL:
        narrowing, dimensions_compatible = _dimension_compatibility(source, target)
        return narrowing or ColumnCompatibility.SAFE, dimensions_compatible
    if source_type is CanonicalType.DATE and target_type is CanonicalType.TIMESTAMP:
        return ColumnCompatibility.SAFE, False
    if source_type is CanonicalType.DECIMAL and target_type in {
        CanonicalType.INTEGER,
        CanonicalType.FLOAT,
    }:
        return ColumnCompatibility.LOSSY, False
    if source_type is CanonicalType.INTEGER and target_type is CanonicalType.FLOAT:
        return ColumnCompatibility.LOSSY, False
    return ColumnCompatibility.INCOMPATIBLE, False


def _evidence(code: str, explanation: str) -> MappingEvidence:
    return MappingEvidence(code=code, explanation=explanation, contribution=_WEIGHTS[code])


@dataclass(frozen=True, slots=True)
class _Candidate:
    source: ColumnMetadata
    target: ColumnMetadata
    confidence: float
    compatibility: ColumnCompatibility
    evidence: tuple[MappingEvidence, ...]

    def rank(self) -> tuple[float, int, str, str]:
        return (-self.confidence, -_COMPATIBILITY_PRIORITY[self.compatibility], self.source.column_name, self.target.column_name)


def _candidate(source: ColumnMetadata, target: ColumnMetadata) -> _Candidate:
    evidence: list[MappingEvidence] = []
    if source.column_name == target.column_name:
        evidence.append(_evidence("EXACT_NATIVE_NAME", "Column names match exactly."))
    elif source.column_name.casefold() == target.column_name.casefold():
        evidence.append(_evidence("CASE_INSENSITIVE_NAME", "Column names match ignoring case."))
    else:
        source_tokens = _comparison_tokens(source.column_name)
        target_tokens = _comparison_tokens(target.column_name)
        if source_tokens == target_tokens and source_tokens:
            evidence.append(_evidence("NORMALIZED_NAME", "Column names match after separator and camel-case normalization."))
        elif source_tokens and target_tokens and set(source_tokens) == set(target_tokens):
            evidence.append(_evidence("TOKEN_NAME_MATCH", "Column-name tokens match in a different order."))

    compatibility, dimensions_compatible = _type_compatibility(source, target)
    if compatibility is ColumnCompatibility.EXACT:
        evidence.append(_evidence("EXACT_CANONICAL_TYPE", "Canonical column types match."))
    elif compatibility is ColumnCompatibility.SAFE:
        evidence.append(_evidence("SAFE_TYPE_COMPATIBILITY", "Canonical column types have a conservative safe conversion."))
    elif compatibility is ColumnCompatibility.LOSSY:
        evidence.append(_evidence("LOSSY_TYPE_COMPATIBILITY", "Canonical conversion can lose range or precision."))
    elif compatibility is ColumnCompatibility.INCOMPATIBLE:
        evidence.append(MappingEvidence(code="INCOMPATIBLE_TYPE", explanation="Canonical column types are unrelated."))
    else:
        evidence.append(MappingEvidence(code="UNKNOWN_TYPE", explanation="Canonical type evidence is unavailable."))
    if dimensions_compatible:
        evidence.append(_evidence("LENGTH_OR_PRECISION_COMPATIBLE", "Known length or precision does not narrow."))
    if source.nullable is not None and target.nullable is not None:
        if source.nullable is target.nullable or (source.nullable is False and target.nullable is True):
            evidence.append(_evidence("NULLABILITY_COMPATIBLE", "Target nullability accepts source values."))
        else:
            evidence.append(_evidence("NULLABILITY_MISMATCH", "Nullable source values may violate the target."))
    if source.ordinal_position is not None and target.ordinal_position is not None:
        distance = abs(source.ordinal_position - target.ordinal_position)
        if distance <= 1:
            evidence.append(_evidence("ORDINAL_PROXIMITY", "Column ordinals are adjacent or equal."))
    confidence = max(0.0, min(1.0, sum(item.contribution or 0.0 for item in evidence)))
    return _Candidate(source, target, confidence, compatibility, tuple(evidence))


class SchemaMappingService:
    """Build conservative one-to-one mapping suggestions without execution or AI."""

    def suggest(self, source: TableMetadata, target: TableMetadata) -> TableMappingPlan:
        if not isinstance(source, TableMetadata) or not isinstance(target, TableMetadata):
            raise TypeError("source and target must be TableMetadata values.")
        source_columns = _ordered_columns(source.columns)
        target_columns = _ordered_columns(target.columns)
        candidates = tuple(_candidate(left, right) for left in source_columns for right in target_columns)
        viable = tuple(item for item in candidates if item.compatibility is not ColumnCompatibility.INCOMPATIBLE)
        by_source = {
            column.column_name: tuple(sorted((item for item in viable if item.source is column), key=_Candidate.rank))
            for column in source_columns
        }
        by_target = {
            column.column_name: tuple(sorted((item for item in viable if item.target is column), key=_Candidate.rank))
            for column in target_columns
        }

        assigned: dict[str, _Candidate] = {}
        ambiguous: set[str] = set()
        for source_column in source_columns:
            options = by_source[source_column.column_name]
            if not options:
                continue
            best = options[0]
            target_options = by_target[best.target.column_name]
            source_close = any(
                item is not best and best.confidence - item.confidence <= _AMBIGUITY_MARGIN
                for item in options
            )
            target_close = any(
                item is not best and best.confidence - item.confidence <= _AMBIGUITY_MARGIN
                for item in target_options
            )
            if (
                best.confidence >= _SUGGESTION_THRESHOLD
                and best.compatibility in {ColumnCompatibility.EXACT, ColumnCompatibility.SAFE}
                and target_options
                and target_options[0] is best
                and not source_close
                and not target_close
            ):
                assigned[source_column.column_name] = best
            else:
                ambiguous.add(source_column.column_name)

        suggestions: list[ColumnMappingSuggestion] = []
        unmatched_source: list[str] = []
        for source_column in source_columns:
            match = assigned.get(source_column.column_name)
            if match is not None:
                suggestions.append(ColumnMappingSuggestion(
                    source_column=source_column.column_name,
                    target_column=match.target.column_name,
                    confidence=match.confidence,
                    compatibility=match.compatibility,
                    decision=MappingDecision.SUGGESTED,
                    evidence=match.evidence,
                ))
                continue
            diagnostics = tuple(item for item in candidates if item.source is source_column)
            diagnostic = min(diagnostics, key=_Candidate.rank) if diagnostics else None
            if source_column.column_name in ambiguous or any(
                item.compatibility is ColumnCompatibility.INCOMPATIBLE
                and any(
                    evidence.code
                    in {
                        "EXACT_NATIVE_NAME",
                        "CASE_INSENSITIVE_NAME",
                        "NORMALIZED_NAME",
                        "TOKEN_NAME_MATCH",
                    }
                    for evidence in item.evidence
                )
                for item in diagnostics
            ):
                evidence = (diagnostic.evidence if diagnostic is not None else ()) + (
                    MappingEvidence(code="AMBIGUOUS_CANDIDATE", explanation="No unique safe mutual-best target exists."),
                )
                suggestions.append(ColumnMappingSuggestion(
                    source_column=source_column.column_name,
                    target_column=None,
                    confidence=diagnostic.confidence if diagnostic is not None else 0.0,
                    compatibility=diagnostic.compatibility if diagnostic is not None else ColumnCompatibility.UNKNOWN,
                    decision=MappingDecision.AMBIGUOUS,
                    evidence=evidence,
                ))
            else:
                unmatched_source.append(source_column.column_name)
                suggestions.append(ColumnMappingSuggestion(
                    source_column=source_column.column_name,
                    target_column=None,
                    confidence=0.0,
                    compatibility=ColumnCompatibility.UNKNOWN,
                    decision=MappingDecision.UNMATCHED,
                    evidence=(MappingEvidence(code="NO_VIABLE_CANDIDATE", explanation="No safe candidate was found."),),
                ))

        assigned_targets = {item.target.column_name for item in assigned.values()}
        return TableMappingPlan(
            source_table=TableMappingIdentity(
                catalog_name=source.catalog_name,
                schema_name=source.schema_name,
                table_name=source.object_name,
                system=source.system,
            ),
            target_table=TableMappingIdentity(
                catalog_name=target.catalog_name,
                schema_name=target.schema_name,
                table_name=target.object_name,
                system=target.system,
            ),
            suggestions=tuple(suggestions),
            unmatched_source_columns=tuple(unmatched_source),
            unmatched_target_columns=tuple(
                column.column_name for column in target_columns if column.column_name not in assigned_targets
            ),
            ambiguous_source_columns=tuple(
                column.column_name for column in source_columns if column.column_name in ambiguous
            ),
            warnings=(),
        )


__all__ = ["SchemaMappingService"]
