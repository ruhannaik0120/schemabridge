"""Coordinate the durable planning half of a migration workflow.

The orchestrator sequences profile-bound discovery, deterministic mapping,
human approval, and transformation preview.  It checks workflow state and
artifact lineage, delegates pure domain work, and asks the persistence service
to atomically record the resulting artifact, state change, and audit evidence.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from schemabridge.models.mapping import (
    ApprovedTableMappingPlan,
    MappingApprovalStatus,
    MappingReviewDecision,
    TableMappingPlan,
    TransformationStatementType,
)
from schemabridge.models.workflow import (
    AuditActorType,
    MigrationWorkflow,
    MigrationWorkflowStatus,
    WorkflowArtifact,
    WorkflowArtifactType,
)
from schemabridge.persistence.artifact_codec import (
    approved_mapping_plan_from_artifact,
    mapping_plan_from_artifact,
    table_metadata_from_artifact,
    transformation_sql_from_artifact,
    workflow_transport_evidence_from_artifact,
)
from schemabridge.persistence.errors import (
    WorkflowArtifactValidationError,
    WorkflowConnectorOperationError,
    WorkflowMappingApprovalRequiredError,
    WorkflowOperationUnavailableError,
    WorkflowPreviewCompilationError,
    WorkflowRequiredArtifactError,
    WorkflowStaleArtifactReferenceError,
)
from schemabridge.persistence.serialization import request_hash
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from schemabridge.models.transport import TransportRelation
from schemabridge.target_execution import TargetExecutionRegistry


@dataclass(frozen=True, slots=True)
class WorkflowPlanningResult:
    """Return the updated workflow, persisted artifact, and typed artifact value."""

    workflow: MigrationWorkflow
    artifact: WorkflowArtifact
    result: Any


class WorkflowPlanningOrchestrator:
    """Enforce planning preconditions around stateless domain services."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        *,
        discovery_resolver: Callable[[str], object],
        mapping_service: object,
        approval_service: object,
        target_registry: TargetExecutionRegistry,
    ) -> None:
        """Bind persistence and the injected discovery/compilation boundaries."""

        self.persistence = persistence
        self.discovery_resolver = discovery_resolver
        self.mapping_service = mapping_service
        self.approval_service = approval_service
        self.target_registry = target_registry

    @staticmethod
    def _context(
        *,
        idempotency_key: str,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
    ) -> dict[str, object]:
        """Build the audit and idempotency context passed to persistence."""

        return {
            "idempotency_key": idempotency_key,
            "actor_type": actor_type,
            "actor_reference": actor_reference,
            "request_id": request_id,
        }

    @staticmethod
    def _require_current_state(
        workflow: MigrationWorkflow,
        expected_version: int,
        state: MigrationWorkflowStatus,
    ) -> None:
        """Reject a current-version command that is unavailable in ``state``."""

        # Stale calls are deferred to repository idempotency/concurrency handling,
        # which preserves exact replays after a successful state transition.
        if workflow.version == expected_version and workflow.status is not state:
            raise WorkflowOperationUnavailableError()

    def _latest_required(
        self,
        workflow_id: UUID,
        artifact_type: WorkflowArtifactType,
    ) -> WorkflowArtifact:
        """Load the latest artifact of a required type or fail the operation."""

        artifact = self.persistence.get_latest_artifact(workflow_id, artifact_type)
        if artifact is None:
            raise WorkflowRequiredArtifactError()
        return artifact

    def _referenced_artifact(
        self,
        workflow_id: UUID,
        artifact_version: int,
        artifact_type: WorkflowArtifactType,
        *,
        require_latest: bool,
    ) -> WorkflowArtifact:
        """Validate an artifact's ownership, type, and optional latest-version rule."""

        artifact = self.persistence.get_artifact(workflow_id, artifact_version)
        if artifact is None:
            raise WorkflowRequiredArtifactError()
        if artifact.artifact_type is not artifact_type:
            raise WorkflowStaleArtifactReferenceError()
        if require_latest:
            # A valid but superseded artifact must not cross an approval or
            # execution boundary for a current-version command.
            latest = self.persistence.get_latest_artifact(workflow_id, artifact_type)
            if latest is None or latest.artifact_version != artifact_version:
                raise WorkflowStaleArtifactReferenceError()
        return artifact

    def _discover(
        self,
        workflow_id: UUID,
        *,
        expected_version: int,
        source: bool,
        idempotency_key: str,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
    ) -> WorkflowPlanningResult:
        """Discover one workflow relation and durably record canonical metadata."""

        workflow = self.persistence.get_workflow(workflow_id)
        self._require_current_state(workflow, expected_version, MigrationWorkflowStatus.DRAFT)
        relation = workflow.source_relation if source else workflow.target_relation
        profile_id = workflow.source_profile_id if source else workflow.target_profile_id
        try:
            connector = self.discovery_resolver(profile_id)
            metadata = connector.get_table_metadata(
                database=relation.catalog_name,
                schema=relation.schema_name,
                table=relation.object_name,
            )
        except Exception:
            raise WorkflowConnectorOperationError() from None
        if metadata is None:
            raise WorkflowConnectorOperationError()
        counterpart_type = (
            WorkflowArtifactType.TARGET_DISCOVERY
            if source
            else WorkflowArtifactType.SOURCE_DISCOVERY
        )
        # Discovery can occur in either order.  The workflow advances only when
        # both independently persisted snapshots are present.
        advance = self.persistence.get_latest_artifact(workflow_id, counterpart_type) is not None
        digest = request_hash(
            "WORKFLOW_DISCOVER_SOURCE" if source else "WORKFLOW_DISCOVER_TARGET",
            {
                "workflow_id": workflow_id,
                "expected_version": expected_version,
                "relation": relation,
            },
        )
        context = self._context(
            idempotency_key=idempotency_key,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
        )
        if source:
            updated, artifact = self.persistence.record_source_discovery(
                workflow_id,
                expected_version,
                metadata,
                advance=advance,
                command_hash=digest,
                **context,
            )
        else:
            updated, artifact = self.persistence.record_target_discovery(
                workflow_id,
                expected_version,
                metadata,
                advance=advance,
                command_hash=digest,
                **context,
            )
        return WorkflowPlanningResult(
            updated, artifact, table_metadata_from_artifact(artifact)
        )

    def discover_source(self, workflow_id: UUID, **command) -> WorkflowPlanningResult:
        """Discover and persist the source relation configured on the workflow."""

        return self._discover(workflow_id, source=True, **command)

    def discover_target(self, workflow_id: UUID, **command) -> WorkflowPlanningResult:
        """Discover and persist the target relation configured on the workflow."""

        return self._discover(workflow_id, source=False, **command)

    def generate_mapping(
        self,
        workflow_id: UUID,
        *,
        expected_version: int,
        idempotency_key: str,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
    ) -> WorkflowPlanningResult:
        """Generate a proposal from the latest two discovery artifacts."""

        workflow = self.persistence.get_workflow(workflow_id)
        self._require_current_state(
            workflow, expected_version, MigrationWorkflowStatus.DISCOVERED
        )
        source_artifact = self._latest_required(
            workflow_id, WorkflowArtifactType.SOURCE_DISCOVERY
        )
        target_artifact = self._latest_required(
            workflow_id, WorkflowArtifactType.TARGET_DISCOVERY
        )
        source = table_metadata_from_artifact(source_artifact)
        target = table_metadata_from_artifact(target_artifact)
        try:
            plan = self.mapping_service.suggest(source, target)
        except (TypeError, ValueError):
            raise WorkflowArtifactValidationError() from None
        digest = request_hash(
            "WORKFLOW_GENERATE_MAPPING",
            {
                "workflow_id": workflow_id,
                "expected_version": expected_version,
                "source_artifact_version": source_artifact.artifact_version,
                "target_artifact_version": target_artifact.artifact_version,
            },
        )
        updated, artifact = self.persistence.record_mapping_proposal(
            workflow_id,
            expected_version,
            plan,
            command_hash=digest,
            **self._context(
                idempotency_key=idempotency_key,
                actor_type=actor_type,
                actor_reference=actor_reference,
                request_id=request_id,
            ),
        )
        return WorkflowPlanningResult(
            updated, artifact, mapping_plan_from_artifact(artifact)
        )

    def approve_mapping(
        self,
        workflow_id: UUID,
        *,
        expected_version: int,
        mapping_artifact_version: int,
        decisions: tuple[MappingReviewDecision, ...],
        idempotency_key: str,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
    ) -> WorkflowPlanningResult:
        """Apply human decisions to the referenced latest mapping proposal."""

        workflow = self.persistence.get_workflow(workflow_id)
        self._require_current_state(
            workflow, expected_version, MigrationWorkflowStatus.MAPPING_PROPOSED
        )
        mapping_artifact = self._referenced_artifact(
            workflow_id,
            mapping_artifact_version,
            WorkflowArtifactType.MAPPING_PLAN,
            require_latest=workflow.version == expected_version,
        )
        source = table_metadata_from_artifact(
            self._latest_required(workflow_id, WorkflowArtifactType.SOURCE_DISCOVERY)
        )
        target = table_metadata_from_artifact(
            self._latest_required(workflow_id, WorkflowArtifactType.TARGET_DISCOVERY)
        )
        plan: TableMappingPlan = mapping_plan_from_artifact(mapping_artifact)
        try:
            approved: ApprovedTableMappingPlan = self.approval_service.apply(
                plan,
                source=source,
                target=target,
                decisions=decisions,
            )
        except (TypeError, ValueError):
            raise WorkflowMappingApprovalRequiredError() from None
        # Preview and execution require a complete human decision boundary;
        # pending-only or empty approval artifacts are not sufficient.
        if (
            any(item.status is MappingApprovalStatus.PENDING for item in approved.approvals)
            or not approved.approved_mappings
            or any(
                item.transformation is None for item in approved.approved_mappings
            )
        ):
            raise WorkflowMappingApprovalRequiredError()
        digest = request_hash(
            "WORKFLOW_APPROVE_MAPPING",
            {
                "workflow_id": workflow_id,
                "expected_version": expected_version,
                "mapping_artifact_version": mapping_artifact_version,
                "decisions": decisions,
            },
        )
        updated, artifact = self.persistence.record_approved_mapping(
            workflow_id,
            expected_version,
            approved,
            command_hash=digest,
            **self._context(
                idempotency_key=idempotency_key,
                actor_type=actor_type,
                actor_reference=actor_reference,
                request_id=request_id,
            ),
        )
        return WorkflowPlanningResult(
            updated, artifact, approved_mapping_plan_from_artifact(artifact)
        )

    def preview_transformation(
        self,
        workflow_id: UUID,
        *,
        expected_version: int,
        approved_mapping_artifact_version: int,
        staging_database: str | None,
        staging_schema: str | None,
        staging_table: str | None,
        statement_type: TransformationStatementType,
        idempotency_key: str,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
    ) -> WorkflowPlanningResult:
        """Compile and persist SQL from a referenced approved mapping artifact."""

        workflow = self.persistence.get_workflow(workflow_id)
        current_command = workflow.version == expected_version
        if current_command and workflow.status in {
            MigrationWorkflowStatus.DRAFT,
            MigrationWorkflowStatus.DISCOVERED,
            MigrationWorkflowStatus.MAPPING_PROPOSED,
        }:
            raise WorkflowMappingApprovalRequiredError()
        if current_command and workflow.status not in {
            MigrationWorkflowStatus.MAPPING_APPROVED,
            MigrationWorkflowStatus.STAGED,
        }:
            raise WorkflowOperationUnavailableError()
        # Bind the preview to immutable approved evidence rather than accepting
        # mappings or SQL directly from the client.
        approved_artifact = self._referenced_artifact(
            workflow_id,
            approved_mapping_artifact_version,
            WorkflowArtifactType.APPROVED_MAPPING_PLAN,
            require_latest=current_command,
        )
        approved = approved_mapping_plan_from_artifact(approved_artifact)
        if not approved.approved_mappings or any(
            item.status is MappingApprovalStatus.PENDING for item in approved.approvals
        ):
            raise WorkflowMappingApprovalRequiredError()
        if workflow.status is MigrationWorkflowStatus.STAGED:
            staging_artifact = self.persistence.get_latest_artifact(
                workflow_id, WorkflowArtifactType.STAGING_LOAD_EVIDENCE
            )
            if staging_artifact is None:
                raise WorkflowRequiredArtifactError()
            staging_evidence = workflow_transport_evidence_from_artifact(staging_artifact)
            if (
                staging_evidence.approved_mapping_artifact_version
                != approved_mapping_artifact_version
            ):
                raise WorkflowStaleArtifactReferenceError()
            managed = staging_evidence.staging_relation
            supplied = (staging_database, staging_schema, staging_table)
            expected = (
                managed.catalog_name,
                managed.schema_name,
                managed.object_name,
            )
            if any(value is not None for value in supplied) and supplied != expected:
                raise WorkflowStaleArtifactReferenceError()
            staging_database, staging_schema, staging_table = expected
        elif None in (staging_database, staging_schema, staging_table):
            raise WorkflowPreviewCompilationError()
        try:
            adapter = self.target_registry.resolve(workflow.target_relation.system)
            staging_relation = TransportRelation(
                catalog_name=staging_database,
                schema_name=staging_schema,
                object_name=staging_table,
            )
            kwargs = {"staging_relation": staging_relation}
            if not adapter.capabilities.supports_preview(statement_type):
                raise WorkflowPreviewCompilationError()
            if statement_type is TransformationStatementType.SELECT:
                preview = adapter.compiler.compile_select(approved, **kwargs)
            else:
                preview = adapter.compiler.compile_insert_select(
                    approved, **kwargs
                )
        except Exception:
            raise WorkflowPreviewCompilationError() from None
        digest = request_hash(
            "WORKFLOW_PREVIEW_TRANSFORMATION",
            {
                "workflow_id": workflow_id,
                "expected_version": expected_version,
                "approved_mapping_artifact_version": approved_mapping_artifact_version,
                "staging_database": staging_database,
                "staging_schema": staging_schema,
                "staging_table": staging_table,
                "statement_type": statement_type,
            },
        )
        updated, artifact = self.persistence.record_transformation_preview(
            workflow_id,
            expected_version,
            preview,
            command_hash=digest,
            **self._context(
                idempotency_key=idempotency_key,
                actor_type=actor_type,
                actor_reference=actor_reference,
                request_id=request_id,
            ),
        )
        return WorkflowPlanningResult(
            updated, artifact, transformation_sql_from_artifact(artifact)
        )


__all__ = ["WorkflowPlanningOrchestrator", "WorkflowPlanningResult"]
