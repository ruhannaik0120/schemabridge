"""Strict transport contracts for durable migration workflow persistence."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, JsonValue, StringConstraints

from schemabridge.models.workflow import (
    AuditActorType,
    MigrationAuditEventType,
    MigrationWorkflowStatus,
    WorkflowArtifactType,
)
from schemabridge.models.mapping import TransformationStatementType
from schemabridge.models.execution import (
    MigrationExecutionAttemptStatus,
    MigrationTransactionOutcome,
)
from schemabridge.models.workflow_validation import WorkflowValidationRunStatus
from schemabridge.models.workflow_transport import WorkflowTransportAttemptStatus

from .common import ApiSchema
from .migrations import (
    ApprovedTableMappingPlanSchema,
    GeneratedTransformationSqlSchema,
    GeneratedValidationSqlSchema,
    Identifier,
    NonNegativeInt,
    PositiveInt,
    SafeCode,
    TableMappingPlanSchema,
    TableMetadataSchema,
    MappingReviewDecisionSchema,
    ValidationExecutionResponse,
)


DisplayName = Annotated[str, StringConstraints(min_length=1, max_length=200)]
ProfileId = Annotated[str, StringConstraints(min_length=1, max_length=256)]
ActorReference = Annotated[str, StringConstraints(min_length=1, max_length=256)]


class WorkflowRelationSchema(ApiSchema):
    catalog_name: Identifier | None
    schema_name: Identifier
    object_name: Identifier
    system: Annotated[str, StringConstraints(min_length=1, max_length=64)]


class WorkflowCommandContext(ApiSchema):
    actor_type: AuditActorType = AuditActorType.USER
    actor_reference: ActorReference | None = None


class WorkflowCreateRequest(WorkflowCommandContext):
    display_name: DisplayName
    source_profile_id: ProfileId
    target_profile_id: ProfileId
    source_relation: WorkflowRelationSchema
    target_relation: WorkflowRelationSchema


class WorkflowStatusTransitionRequest(WorkflowCommandContext):
    expected_version: PositiveInt
    new_status: MigrationWorkflowStatus
    reason_code: SafeCode | None = None


class WorkflowPlanningCommand(WorkflowCommandContext):
    expected_version: PositiveInt


class WorkflowMappingApprovalCommand(WorkflowPlanningCommand):
    mapping_artifact_version: PositiveInt
    decisions: tuple[MappingReviewDecisionSchema, ...]


class WorkflowTransformationPreviewCommand(WorkflowPlanningCommand):
    approved_mapping_artifact_version: PositiveInt
    staging_database: Identifier | None = None
    staging_schema: Identifier | None = None
    staging_table: Identifier | None = None
    statement_type: TransformationStatementType


class WorkflowTransportCommand(WorkflowPlanningCommand):
    source_discovery_artifact_version: PositiveInt
    approved_mapping_artifact_version: PositiveInt
    source_profile_id: ProfileId
    target_profile_id: ProfileId
    batch_size: PositiveInt | None = None
    timeout_seconds: PositiveInt | None = None


class WorkflowExecutionCommand(WorkflowPlanningCommand):
    approved_mapping_artifact_version: PositiveInt
    transformation_preview_artifact_version: PositiveInt
    target_profile_id: ProfileId
    timeout_seconds: PositiveInt | None = None


class WorkflowValidationCommand(WorkflowPlanningCommand):
    execution_evidence_artifact_version: PositiveInt
    approved_mapping_artifact_version: PositiveInt
    source_profile_id: ProfileId
    target_profile_id: ProfileId
    timeout_seconds: PositiveInt | None = None


class MigrationWorkflowSchema(ApiSchema):
    workflow_id: UUID
    display_name: DisplayName
    source_profile_id: ProfileId
    target_profile_id: ProfileId
    source_relation: WorkflowRelationSchema
    target_relation: WorkflowRelationSchema
    status: MigrationWorkflowStatus
    version: PositiveInt
    created_at: datetime
    updated_at: datetime
    latest_artifact_version: NonNegativeInt
    last_error_code: SafeCode | None
    warnings: tuple[SafeCode, ...]


class ArtifactCommandBase(WorkflowCommandContext):
    expected_version: PositiveInt


class SourceDiscoveryArtifactRequest(ArtifactCommandBase):
    artifact_type: Literal[WorkflowArtifactType.SOURCE_DISCOVERY]
    payload: TableMetadataSchema


class TargetDiscoveryArtifactRequest(ArtifactCommandBase):
    artifact_type: Literal[WorkflowArtifactType.TARGET_DISCOVERY]
    payload: TableMetadataSchema


class MappingPlanArtifactRequest(ArtifactCommandBase):
    artifact_type: Literal[WorkflowArtifactType.MAPPING_PLAN]
    payload: TableMappingPlanSchema


class ApprovedMappingPlanArtifactRequest(ArtifactCommandBase):
    artifact_type: Literal[WorkflowArtifactType.APPROVED_MAPPING_PLAN]
    payload: ApprovedTableMappingPlanSchema


class TransformationPreviewArtifactRequest(ArtifactCommandBase):
    artifact_type: Literal[WorkflowArtifactType.TRANSFORMATION_PREVIEW]
    payload: GeneratedTransformationSqlSchema


class ValidationPreviewArtifactPayload(ApiSchema):
    source: GeneratedValidationSqlSchema
    target: GeneratedValidationSqlSchema


class ValidationExecutionArtifactPayload(ValidationExecutionResponse):
    source_profile_id: ProfileId
    target_profile_id: ProfileId


WorkflowArtifactAppendRequest = Annotated[
    SourceDiscoveryArtifactRequest
    | TargetDiscoveryArtifactRequest
    | MappingPlanArtifactRequest
    | ApprovedMappingPlanArtifactRequest
    | TransformationPreviewArtifactRequest,
    Field(discriminator="artifact_type"),
]


class WorkflowArtifactSchema(ApiSchema):
    artifact_id: UUID
    workflow_id: UUID
    artifact_type: WorkflowArtifactType
    artifact_version: PositiveInt
    schema_version: PositiveInt
    payload: JsonValue
    payload_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    created_at: datetime


class WorkflowArtifactAppendResponse(ApiSchema):
    workflow: MigrationWorkflowSchema
    artifact: WorkflowArtifactSchema


class WorkflowDiscoveryOperationResponse(WorkflowArtifactAppendResponse):
    result: TableMetadataSchema


class WorkflowMappingOperationResponse(WorkflowArtifactAppendResponse):
    result: TableMappingPlanSchema


class WorkflowApprovalOperationResponse(WorkflowArtifactAppendResponse):
    result: ApprovedTableMappingPlanSchema


class WorkflowTransformationPreviewOperationResponse(WorkflowArtifactAppendResponse):
    result: GeneratedTransformationSqlSchema


class TransportRelationSchema(ApiSchema):
    catalog_name: Identifier | None
    schema_name: Identifier
    object_name: Identifier


class WorkflowTransportAttemptSchema(ApiSchema):
    attempt_id: UUID
    workflow_id: UUID
    source_discovery_artifact_version: PositiveInt
    approved_mapping_artifact_version: PositiveInt
    source_profile_id: ProfileId
    target_profile_id: ProfileId
    staging_relation: TransportRelationSchema
    batch_size: PositiveInt
    timeout_seconds: PositiveInt
    transport_fingerprint: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    status: WorkflowTransportAttemptStatus
    claimed_at: datetime
    actor_type: AuditActorType
    idempotency_key: Annotated[str, StringConstraints(min_length=1, max_length=128)]
    actor_reference: ActorReference | None
    running_at: datetime | None
    completed_at: datetime | None
    evidence_artifact_id: UUID | None
    failure_category: SafeCode | None


class WorkflowTransportEvidenceSchema(ApiSchema):
    attempt_id: UUID
    workflow_id: UUID
    source_relation: TransportRelationSchema
    staging_relation: TransportRelationSchema
    source_profile_id: ProfileId
    target_profile_id: ProfileId
    batch_size: PositiveInt
    batch_count: NonNegativeInt
    column_count: PositiveInt
    rows_read: NonNegativeInt
    rows_written: NonNegativeInt
    started_at: datetime
    completed_at: datetime
    duration_ms: NonNegativeInt
    source_discovery_artifact_version: PositiveInt
    approved_mapping_artifact_version: PositiveInt
    transport_fingerprint: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class WorkflowTransportOperationResponse(WorkflowArtifactAppendResponse):
    attempt: WorkflowTransportAttemptSchema
    result: WorkflowTransportEvidenceSchema


class MigrationExecutionAttemptSchema(ApiSchema):
    attempt_id: UUID
    workflow_id: UUID
    approved_mapping_artifact_version: PositiveInt
    transformation_preview_artifact_version: PositiveInt
    target_profile_id: ProfileId
    execution_fingerprint: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    status: MigrationExecutionAttemptStatus
    timeout_seconds: PositiveInt
    claimed_at: datetime
    actor_type: AuditActorType
    idempotency_key: Annotated[str, StringConstraints(min_length=1, max_length=128)]
    actor_reference: ActorReference | None
    running_at: datetime | None
    completed_at: datetime | None
    evidence_artifact_id: UUID | None
    failure_category: SafeCode | None


class MigrationExecutionEvidenceSchema(ApiSchema):
    attempt_id: UUID
    workflow_id: UUID
    status: MigrationExecutionAttemptStatus
    statement_count: PositiveInt
    affected_rows: NonNegativeInt | None
    target_relation: tuple[Identifier | None, Identifier, Identifier]
    target_profile_id: ProfileId
    connector_type: Annotated[str, StringConstraints(min_length=1, max_length=64)]
    started_at: datetime
    completed_at: datetime
    duration_ms: NonNegativeInt
    transaction_outcome: MigrationTransactionOutcome
    approved_mapping_artifact_version: PositiveInt
    transformation_preview_artifact_version: PositiveInt
    execution_fingerprint: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    failure_category: SafeCode | None


class WorkflowExecutionOperationResponse(WorkflowArtifactAppendResponse):
    attempt: MigrationExecutionAttemptSchema
    result: MigrationExecutionEvidenceSchema


class WorkflowValidationRunSchema(ApiSchema):
    run_id: UUID
    workflow_id: UUID
    execution_attempt_id: UUID
    execution_evidence_artifact_version: PositiveInt
    approved_mapping_artifact_version: PositiveInt
    validation_preview_artifact_version: PositiveInt
    source_profile_id: ProfileId
    target_profile_id: ProfileId
    validation_fingerprint: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    status: WorkflowValidationRunStatus
    timeout_seconds: PositiveInt
    claimed_at: datetime
    actor_type: AuditActorType
    idempotency_key: Annotated[str, StringConstraints(min_length=1, max_length=128)]
    actor_reference: ActorReference | None
    running_at: datetime | None
    completed_at: datetime | None
    duration_ms: NonNegativeInt | None
    evidence_artifact_id: UUID | None
    failure_category: SafeCode | None


class WorkflowValidationOperationResponse(ApiSchema):
    workflow: MigrationWorkflowSchema
    run: WorkflowValidationRunSchema
    plan_artifact: WorkflowArtifactSchema
    evidence_artifact: WorkflowArtifactSchema
    result: ValidationExecutionArtifactPayload


class AuditMetadataSchema(ApiSchema):
    reason_code: SafeCode | None = None


class MigrationAuditEventSchema(ApiSchema):
    sequence_number: PositiveInt
    event_id: UUID
    workflow_id: UUID
    event_type: MigrationAuditEventType
    previous_status: MigrationWorkflowStatus | None
    new_status: MigrationWorkflowStatus | None
    workflow_version: PositiveInt
    artifact_id: UUID | None
    artifact_type: WorkflowArtifactType | None
    actor_type: AuditActorType
    actor_reference: ActorReference | None
    request_id: Annotated[str, StringConstraints(min_length=1, max_length=64)] | None
    idempotency_key: Annotated[str, StringConstraints(min_length=1, max_length=128)]
    occurred_at: datetime
    metadata: AuditMetadataSchema


class WorkflowArtifactListResponse(ApiSchema):
    items: tuple[WorkflowArtifactSchema, ...]
    offset: NonNegativeInt
    limit: PositiveInt


class MigrationAuditEventListResponse(ApiSchema):
    items: tuple[MigrationAuditEventSchema, ...]
    offset: NonNegativeInt
    limit: PositiveInt
