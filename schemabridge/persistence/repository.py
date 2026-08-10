"""Define the durable control-plane contract used by workflow services.

The protocol separates orchestration from PostgreSQL/psycopg details and makes
transactional, concurrency, idempotency, and recovery behavior replaceable in
tests.  Implementations must preserve atomicity across each multi-record
operation described by a single method.
"""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from schemabridge.models.execution import (
    MigrationExecutionAttempt,
    MigrationExecutionEvidence,
)
from schemabridge.models.validation import MigrationValidationExecutionReport
from schemabridge.models.workflow import (
    MigrationAuditEvent,
    MigrationWorkflow,
    MigrationWorkflowStatus,
    WorkflowArtifact,
)
from schemabridge.models.workflow_validation import WorkflowValidationRun


class WorkflowRepository(Protocol):
    """Persist workflows, immutable evidence, operation claims, and audit events."""

    def create_workflow(
        self,
        workflow: MigrationWorkflow,
        event: MigrationAuditEvent,
        *,
        idempotency_key: str,
        request_hash: str,
    ) -> MigrationWorkflow:
        """Create a workflow and its first event, or replay the same command."""

        ...

    def get_workflow(self, workflow_id: UUID) -> MigrationWorkflow:
        """Load the current workflow or raise the repository not-found error."""

        ...

    def transition_status(
        self,
        workflow_id: UUID,
        expected_version: int,
        new_status: MigrationWorkflowStatus,
        event: MigrationAuditEvent,
        *,
        last_error_code: str | None,
        idempotency_key: str,
        request_hash: str,
    ) -> MigrationWorkflow:
        """Apply one optimistic state transition and append its audit event."""

        ...

    def append_artifact(
        self,
        workflow_id: UUID,
        expected_version: int,
        artifact: WorkflowArtifact,
        event: MigrationAuditEvent,
        *,
        idempotency_key: str,
        request_hash: str,
    ) -> tuple[MigrationWorkflow, WorkflowArtifact]:
        """Append immutable evidence and advance workflow/artifact versions."""

        ...

    def append_artifact_operation(
        self,
        workflow_id: UUID,
        expected_version: int,
        artifact: WorkflowArtifact,
        artifact_event: MigrationAuditEvent,
        *,
        new_status: MigrationWorkflowStatus | None,
        transition_event: MigrationAuditEvent | None,
        idempotency_key: str,
        request_hash: str,
    ) -> tuple[MigrationWorkflow, WorkflowArtifact]:
        """Atomically append evidence and optionally transition workflow state."""

        ...

    def mark_failed(self, *args, **kwargs) -> MigrationWorkflow:
        """Persist an allowed terminal failure transition."""

        ...

    def cancel_workflow(self, *args, **kwargs) -> MigrationWorkflow:
        """Persist an allowed terminal cancellation transition."""

        ...

    def list_artifacts(
        self,
        workflow_id: UUID,
        *,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[WorkflowArtifact, ...]:
        """Return a bounded ordered page of immutable artifacts."""

        ...

    def get_artifact(
        self, workflow_id: UUID, artifact_version: int
    ) -> WorkflowArtifact | None:
        """Load one workflow-owned artifact by its monotonic version."""

        ...

    def get_artifact_by_id(
        self, workflow_id: UUID, artifact_id: UUID
    ) -> WorkflowArtifact | None:
        """Load one artifact by ID while enforcing workflow ownership."""

        ...

    def get_latest_artifact(
        self, workflow_id: UUID, artifact_type
    ) -> WorkflowArtifact | None:
        """Load the highest-version artifact of a given type."""

        ...

    def get_execution_attempt_by_command(
        self,
        workflow_id: UUID,
        idempotency_key: str,
        request_hash: str,
    ) -> MigrationExecutionAttempt | None:
        """Resolve an exact execution replay or detect a conflicting key."""

        ...

    def claim_execution_attempt(
        self,
        workflow_id: UUID,
        expected_version: int,
        attempt: MigrationExecutionAttempt,
        event: MigrationAuditEvent,
        *,
        idempotency_key: str,
        request_hash: str,
    ) -> tuple[MigrationWorkflow, MigrationExecutionAttempt, bool]:
        """Atomically claim target execution before the remote write begins."""

        ...

    def mark_execution_running(
        self, attempt_id: UUID, running_at
    ) -> tuple[MigrationExecutionAttempt, bool]:
        """Acquire the single-runner transition from claimed to running."""

        ...

    def complete_execution_attempt(
        self,
        workflow_id: UUID,
        expected_version: int,
        attempt_id: UUID,
        evidence: MigrationExecutionEvidence,
        artifact: WorkflowArtifact,
        artifact_event: MigrationAuditEvent,
        new_status: MigrationWorkflowStatus,
        transition_event: MigrationAuditEvent,
    ) -> tuple[MigrationWorkflow, MigrationExecutionAttempt, WorkflowArtifact]:
        """Atomically store terminal execution evidence and workflow state."""

        ...

    def get_execution_attempt(
        self, attempt_id: UUID
    ) -> MigrationExecutionAttempt:
        """Load one durable execution attempt."""

        ...

    def get_validation_run_by_command(
        self,
        workflow_id: UUID,
        idempotency_key: str,
        request_hash: str,
    ) -> WorkflowValidationRun | None:
        """Resolve an exact validation replay or detect a conflicting key."""

        ...

    def claim_validation_run(
        self,
        workflow_id: UUID,
        expected_version: int,
        run: WorkflowValidationRun,
        artifact: WorkflowArtifact,
        artifact_event: MigrationAuditEvent,
        transition_event: MigrationAuditEvent,
        *,
        idempotency_key: str,
        request_hash: str,
    ) -> tuple[MigrationWorkflow, WorkflowValidationRun, WorkflowArtifact, bool]:
        """Atomically persist the validation plan and claim one runner."""

        ...

    def mark_validation_running(
        self, run_id: UUID, running_at
    ) -> tuple[WorkflowValidationRun, bool]:
        """Acquire the single-runner transition from claimed to running."""

        ...

    def complete_validation_run(
        self,
        workflow_id: UUID,
        expected_version: int,
        run_id: UUID,
        report: MigrationValidationExecutionReport | None,
        artifact: WorkflowArtifact | None,
        artifact_event: MigrationAuditEvent | None,
        new_status: MigrationWorkflowStatus,
        transition_event: MigrationAuditEvent,
        *,
        completed_at,
        failure_category: str | None,
    ) -> tuple[MigrationWorkflow, WorkflowValidationRun, WorkflowArtifact | None]:
        """Persist terminal validation evidence or an uncertain recovery state."""

        ...

    def get_validation_run(self, run_id: UUID) -> WorkflowValidationRun:
        """Load one durable validation run."""

        ...

    def list_audit_events(
        self,
        workflow_id: UUID,
        *,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[MigrationAuditEvent, ...]:
        """Return a bounded ordered page of append-only audit events."""

        ...

    def close(self) -> None:
        """Release repository-owned resources; repeated calls must be safe."""

        ...
