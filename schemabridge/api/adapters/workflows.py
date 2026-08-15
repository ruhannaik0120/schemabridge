"""Conversions between durable workflow API schemas and domain models."""

from __future__ import annotations

import json

from schemabridge.models.execution import MigrationExecutionAttempt, MigrationExecutionEvidence
from schemabridge.models.workflow_validation import WorkflowValidationRun
from schemabridge.models.workflow_transport import (
    WorkflowTransportAttempt,
    WorkflowTransportEvidence,
)
from schemabridge.models.mapping import GeneratedTransformationSql
from schemabridge.models.validation import (
    GeneratedValidationSql,
    MigrationValidationExecutionReport,
    MigrationValidationReport,
    ValidationCheckDefinition,
    ValidationCheckResult,
)
from schemabridge.models.workflow import (
    MigrationAuditEvent,
    MigrationWorkflow,
    WorkflowArtifact,
    WorkflowRelation,
)

from ..schemas.migrations import (
    GeneratedTransformationSqlSchema,
    GeneratedValidationSqlSchema,
    MigrationValidationReportSchema,
    ValidationCheckDefinitionSchema,
    ValidationCheckResultSchema,
)
from ..schemas.workflows import (
    AuditMetadataSchema,
    MigrationAuditEventSchema,
    MigrationWorkflowSchema,
    MigrationExecutionAttemptSchema,
    MigrationExecutionEvidenceSchema,
    ValidationExecutionArtifactPayload,
    WorkflowValidationRunSchema,
    WorkflowArtifactSchema,
    WorkflowRelationSchema,
    TransportRelationSchema,
    WorkflowTransportAttemptSchema,
    WorkflowTransportEvidenceSchema,
)


def workflow_relation_to_domain(value: WorkflowRelationSchema) -> WorkflowRelation:
    return WorkflowRelation(**value.model_dump(mode="python"))


def workflow_relation_to_api(value: WorkflowRelation) -> WorkflowRelationSchema:
    return WorkflowRelationSchema(
        catalog_name=value.catalog_name,
        schema_name=value.schema_name,
        object_name=value.object_name,
        system=value.system,
    )


def workflow_to_api(value: MigrationWorkflow) -> MigrationWorkflowSchema:
    return MigrationWorkflowSchema(
        workflow_id=value.workflow_id,
        display_name=value.display_name,
        source_profile_id=value.source_profile_id,
        target_profile_id=value.target_profile_id,
        source_relation=workflow_relation_to_api(value.source_relation),
        target_relation=workflow_relation_to_api(value.target_relation),
        status=value.status,
        version=value.version,
        created_at=value.created_at,
        updated_at=value.updated_at,
        latest_artifact_version=value.latest_artifact_version,
        last_error_code=value.last_error_code,
        warnings=value.warnings,
    )


def transformation_sql_to_domain(
    value: GeneratedTransformationSqlSchema,
) -> GeneratedTransformationSql:
    return GeneratedTransformationSql(
        **value.model_dump(mode="python", exclude={"preview_only"})
    )


def validation_check_definition_to_domain(
    value: ValidationCheckDefinitionSchema,
) -> ValidationCheckDefinition:
    return ValidationCheckDefinition(**value.model_dump(mode="python"))


def validation_sql_to_domain(value: GeneratedValidationSqlSchema) -> GeneratedValidationSql:
    return GeneratedValidationSql(
        **value.model_dump(mode="python", exclude={"checks"}),
        checks=tuple(validation_check_definition_to_domain(item) for item in value.checks),
    )


def validation_check_result_to_domain(
    value: ValidationCheckResultSchema,
) -> ValidationCheckResult:
    return ValidationCheckResult(**value.model_dump(mode="python"))


def validation_report_to_domain(
    value: MigrationValidationReportSchema,
) -> MigrationValidationReport:
    return MigrationValidationReport(
        **value.model_dump(mode="python", exclude={"check_results"}),
        check_results=tuple(validation_check_result_to_domain(item) for item in value.check_results),
    )


def validation_execution_report_to_domain(
    value: ValidationExecutionArtifactPayload,
) -> MigrationValidationExecutionReport:
    return MigrationValidationExecutionReport(
        source_profile_id=value.source_profile_id,
        target_profile_id=value.target_profile_id,
        source_sql_summary=validation_sql_to_domain(value.source_sql_summary),
        target_sql_summary=validation_sql_to_domain(value.target_sql_summary),
        validation_report=validation_report_to_domain(value.validation_report),
        source_execution_status=value.source_execution_status,
        target_execution_status=value.target_execution_status,
        warnings=tuple(value.warnings),
    )


def artifact_to_api(value: WorkflowArtifact) -> WorkflowArtifactSchema:
    return WorkflowArtifactSchema(
        artifact_id=value.artifact_id,
        workflow_id=value.workflow_id,
        artifact_type=value.artifact_type,
        artifact_version=value.artifact_version,
        schema_version=value.schema_version,
        payload=json.loads(value.payload.decode("utf-8")),
        payload_sha256=value.payload_sha256,
        created_at=value.created_at,
    )


def execution_attempt_to_api(
    value: MigrationExecutionAttempt,
) -> MigrationExecutionAttemptSchema:
    return MigrationExecutionAttemptSchema(
        **{name: getattr(value, name) for name in value.__dataclass_fields__}
    )


def execution_evidence_to_api(
    value: MigrationExecutionEvidence,
) -> MigrationExecutionEvidenceSchema:
    return MigrationExecutionEvidenceSchema(
        **{name: getattr(value, name) for name in value.__dataclass_fields__}
    )


def _transport_relation_to_api(value) -> TransportRelationSchema:
    return TransportRelationSchema(
        catalog_name=value.catalog_name,
        schema_name=value.schema_name,
        object_name=value.object_name,
    )


def transport_attempt_to_api(
    value: WorkflowTransportAttempt,
) -> WorkflowTransportAttemptSchema:
    data = {name: getattr(value, name) for name in value.__dataclass_fields__}
    data["staging_relation"] = _transport_relation_to_api(value.staging_relation)
    return WorkflowTransportAttemptSchema(**data)


def transport_evidence_to_api(
    value: WorkflowTransportEvidence,
) -> WorkflowTransportEvidenceSchema:
    data = {name: getattr(value, name) for name in value.__dataclass_fields__}
    data["source_relation"] = _transport_relation_to_api(value.source_relation)
    data["staging_relation"] = _transport_relation_to_api(value.staging_relation)
    return WorkflowTransportEvidenceSchema(**data)


def validation_run_to_api(value: WorkflowValidationRun) -> WorkflowValidationRunSchema:
    return WorkflowValidationRunSchema(
        **{name: getattr(value, name) for name in value.__dataclass_fields__}
    )


def audit_event_to_api(value: MigrationAuditEvent) -> MigrationAuditEventSchema:
    return MigrationAuditEventSchema(
        sequence_number=value.sequence_number,
        event_id=value.event_id,
        workflow_id=value.workflow_id,
        event_type=value.event_type,
        previous_status=value.previous_status,
        new_status=value.new_status,
        workflow_version=value.workflow_version,
        artifact_id=value.artifact_id,
        artifact_type=value.artifact_type,
        actor_type=value.actor_type,
        actor_reference=value.actor_reference,
        request_id=value.request_id,
        idempotency_key=value.idempotency_key,
        occurred_at=value.occurred_at,
        metadata=AuditMetadataSchema(reason_code=value.metadata.reason_code),
    )


__all__ = [name for name in globals() if name.endswith(("_to_api", "_to_domain"))]
