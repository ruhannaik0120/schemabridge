"""Define the common skeleton implemented by every target database adapter."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol, runtime_checkable

from schemabridge.models.mapping import (
    ApprovedTableMappingPlan,
    GeneratedTransformationSql,
    SqlDialect,
    TransformationStatementType,
)
from schemabridge.models.transport import TransportRelation


class UnsupportedTargetOperationError(ValueError):
    """Raised when a registered target has not implemented an operation."""


class TargetExecutionDisposition(str, Enum):
    """Describe whether the remote transaction outcome can be proven."""

    SUCCEEDED = "SUCCEEDED"
    CONFIRMED_FAILED_ROLLED_BACK = "CONFIRMED_FAILED_ROLLED_BACK"
    OUTCOME_UNCERTAIN = "OUTCOME_UNCERTAIN"


@dataclass(frozen=True, slots=True)
class TargetExecutionCapabilities:
    """State exactly which target-side operations an adapter can perform."""

    supports_select_preview: bool
    supports_insert_select_preview: bool
    supports_insert_select_execution: bool

    def supports_preview(self, statement_type: TransformationStatementType) -> bool:
        """Return whether this adapter can compile the requested preview."""

        if statement_type is TransformationStatementType.SELECT:
            return self.supports_select_preview
        if statement_type is TransformationStatementType.INSERT_SELECT:
            return self.supports_insert_select_preview
        return False


@dataclass(frozen=True, slots=True)
class PreparedMigrationTarget:
    """Credential-free execution context resolved from a named profile."""

    profile_id: str
    database: str
    connector_type: str
    timeout_seconds: int
    service: object


@dataclass(frozen=True, slots=True)
class TargetExecutionResult:
    """Sanitized result returned from any target database adapter."""

    disposition: TargetExecutionDisposition
    affected_rows: int | None = None
    failure_category: str | None = None


@runtime_checkable
class TargetTransformationCompiler(Protocol):
    """Compile one approved plan using a target database's SQL dialect."""

    def compile_select(
        self,
        plan: ApprovedTableMappingPlan,
        *,
        staging_relation: TransportRelation,
        source_alias: str = "src",
    ) -> GeneratedTransformationSql: ...

    def compile_insert_select(
        self,
        plan: ApprovedTableMappingPlan,
        *,
        staging_relation: TransportRelation,
        source_alias: str = "src",
    ) -> GeneratedTransformationSql: ...


@runtime_checkable
class TargetExecutionAdapter(Protocol):
    """Bundle all target-specific compilation and execution behavior."""

    @property
    def database_type(self) -> str: ...

    @property
    def dialect(self) -> SqlDialect: ...

    @property
    def capabilities(self) -> TargetExecutionCapabilities: ...

    @property
    def compiler(self) -> TargetTransformationCompiler: ...

    def validate_preview(self, preview: GeneratedTransformationSql) -> None: ...

    def execute(
        self,
        target: PreparedMigrationTarget,
        preview: GeneratedTransformationSql,
    ) -> TargetExecutionResult: ...


__all__ = [
    "PreparedMigrationTarget",
    "TargetExecutionAdapter",
    "TargetExecutionCapabilities",
    "TargetExecutionDisposition",
    "TargetExecutionResult",
    "TargetTransformationCompiler",
    "UnsupportedTargetOperationError",
]
