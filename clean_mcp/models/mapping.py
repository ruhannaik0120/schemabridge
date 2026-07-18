"""Immutable, explainable schema-mapping suggestion models."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import Enum

from models.metadata import _MetadataModel, _json_value, _validate_identifier


class ColumnCompatibility(str, Enum):
    EXACT = "EXACT"
    SAFE = "SAFE"
    LOSSY = "LOSSY"
    INCOMPATIBLE = "INCOMPATIBLE"
    UNKNOWN = "UNKNOWN"


class MappingDecision(str, Enum):
    SUGGESTED = "SUGGESTED"
    AMBIGUOUS = "AMBIGUOUS"
    UNMATCHED = "UNMATCHED"


def _validate_fixed_code(value: str, field_name: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", value) is None:
        raise ValueError(f"{field_name} must be a fixed safe code.")


@dataclass(frozen=True, slots=True, kw_only=True)
class MappingEvidence(_MetadataModel):
    code: str
    explanation: str
    contribution: float | None = None

    def __post_init__(self) -> None:
        _validate_fixed_code(self.code, "code")
        if not isinstance(self.explanation, str) or not self.explanation:
            raise ValueError("explanation must be a non-empty string.")
        if self.contribution is not None:
            if isinstance(self.contribution, bool) or not isinstance(self.contribution, (int, float)):
                raise TypeError("contribution must be a finite number or None.")
            if not math.isfinite(float(self.contribution)):
                raise ValueError("contribution must be finite.")
            object.__setattr__(self, "contribution", float(self.contribution))

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class TableMappingIdentity(_MetadataModel):
    catalog_name: str | None
    schema_name: str
    table_name: str
    system: str

    def __post_init__(self) -> None:
        _validate_identifier(self.catalog_name, "catalog_name", required=False)
        _validate_identifier(self.schema_name, "schema_name", required=True)
        _validate_identifier(self.table_name, "table_name", required=True)
        _validate_identifier(self.system, "system", required=True)

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class ColumnMappingSuggestion(_MetadataModel):
    source_column: str
    target_column: str | None
    confidence: float
    compatibility: ColumnCompatibility
    decision: MappingDecision
    evidence: tuple[MappingEvidence, ...]

    def __post_init__(self) -> None:
        _validate_identifier(self.source_column, "source_column", required=True)
        _validate_identifier(self.target_column, "target_column", required=False)
        if isinstance(self.confidence, bool) or not isinstance(self.confidence, (int, float)):
            raise TypeError("confidence must be a finite number.")
        if not math.isfinite(float(self.confidence)) or not 0.0 <= float(self.confidence) <= 1.0:
            raise ValueError("confidence must be between 0.0 and 1.0.")
        object.__setattr__(self, "confidence", float(self.confidence))
        if not isinstance(self.compatibility, ColumnCompatibility):
            raise TypeError("compatibility must be a ColumnCompatibility.")
        if not isinstance(self.decision, MappingDecision):
            raise TypeError("decision must be a MappingDecision.")
        if not isinstance(self.evidence, tuple) or not all(
            isinstance(item, MappingEvidence) for item in self.evidence
        ):
            raise TypeError("evidence must be a tuple of MappingEvidence values.")
        if self.decision is MappingDecision.SUGGESTED and self.target_column is None:
            raise ValueError("suggested mappings require a target_column.")
        if self.decision is not MappingDecision.SUGGESTED and self.target_column is not None:
            raise ValueError("only suggested mappings may expose a target_column.")

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class TableMappingPlan(_MetadataModel):
    source_table: TableMappingIdentity
    target_table: TableMappingIdentity
    suggestions: tuple[ColumnMappingSuggestion, ...]
    unmatched_source_columns: tuple[str, ...]
    unmatched_target_columns: tuple[str, ...]
    ambiguous_source_columns: tuple[str, ...]
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.source_table, TableMappingIdentity) or not isinstance(
            self.target_table, TableMappingIdentity
        ):
            raise TypeError("source_table and target_table must be TableMappingIdentity values.")
        if not isinstance(self.suggestions, tuple) or not all(
            isinstance(item, ColumnMappingSuggestion) for item in self.suggestions
        ):
            raise TypeError("suggestions must be a tuple of ColumnMappingSuggestion values.")
        sources = tuple(item.source_column for item in self.suggestions)
        targets = tuple(
            item.target_column
            for item in self.suggestions
            if item.decision is MappingDecision.SUGGESTED
        )
        if len(set(sources)) != len(sources) or len(set(targets)) != len(targets):
            raise ValueError("final mapping assignments must be one-to-one.")
        for field_name in (
            "unmatched_source_columns",
            "unmatched_target_columns",
            "ambiguous_source_columns",
        ):
            values = getattr(self, field_name)
            if not isinstance(values, tuple) or len(set(values)) != len(values):
                raise ValueError(f"{field_name} must be a tuple of unique column names.")
            for value in values:
                _validate_identifier(value, field_name, required=True)
        if not isinstance(self.warnings, tuple):
            raise TypeError("warnings must be a tuple.")
        for warning in self.warnings:
            _validate_fixed_code(warning, "warnings")
        if self.warnings != tuple(sorted(set(self.warnings))):
            raise ValueError("warnings must be sorted and deduplicated.")

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


__all__ = [
    "ColumnCompatibility",
    "ColumnMappingSuggestion",
    "MappingDecision",
    "MappingEvidence",
    "TableMappingIdentity",
    "TableMappingPlan",
]
