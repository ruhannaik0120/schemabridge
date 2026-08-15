"""Verify durable orchestration around source-to-managed-staging transport."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from schemabridge.models.transport import BatchTransportResult, TransportRelation
from schemabridge.models.workflow import MigrationWorkflowStatus
from schemabridge.persistence.errors import (
    WorkflowStaleArtifactReferenceError,
    WorkflowTransportConfirmedFailureError,
    WorkflowTransportOutcomeUncertainError,
)
from schemabridge.services.batch_transport import (
    BatchTransportDisposition,
    ProfileBoundBatchTransportResult,
)
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from schemabridge.services.workflow_transport import WorkflowTransportOrchestrator
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_workflow_orchestration_api import (
    _application,
    _approve,
    _create,
    _discover_pair,
    _mapping,
)


ATTEMPT_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


class FakeTransport:
    def __init__(self, disposition: BatchTransportDisposition, *, raises: bool = False):
        self.disposition = disposition
        self.raises = raises
        self.prepare_calls = []
        self.run_calls = []

    def prepare(self, **kwargs):
        self.prepare_calls.append(kwargs)
        return SimpleNamespace(batch_size=2, timeout_seconds=10)

    def run(self, _prepared, **kwargs):
        self.run_calls.append(kwargs)
        if self.raises:
            raise RuntimeError("private driver failure")
        if self.disposition is not BatchTransportDisposition.SUCCEEDED:
            return ProfileBoundBatchTransportResult(
                disposition=self.disposition,
                failure_category=(
                    "STAGING_LOAD_FAILED"
                    if self.disposition
                    is BatchTransportDisposition.CONFIRMED_FAILED_CLEANED_UP
                    else "STAGING_OUTCOME_UNCERTAIN"
                ),
            )
        source = kwargs["source_table"]
        return ProfileBoundBatchTransportResult(
            disposition=BatchTransportDisposition.SUCCEEDED,
            result=BatchTransportResult(
                transport_id=kwargs["transport_id"],
                source_relation=TransportRelation(
                    catalog_name=source.catalog_name,
                    schema_name=source.schema_name,
                    object_name=source.object_name,
                ),
                staging_relation=TransportRelation(
                    catalog_name=kwargs["target_database"],
                    schema_name=kwargs["target_schema"],
                    object_name=f"SB_STAGE_{kwargs['transport_id'].hex.upper()}",
                ),
                batch_size=2,
                batch_count=2,
                column_count=len(source.columns),
                rows_read=3,
                rows_written=3,
            ),
        )


def _approved(repository: InMemoryWorkflowRepository):
    with TestClient(_application(repository)) as client:
        created = _create(client)
        _, target = _discover_pair(client, created)
        proposed = _mapping(
            client,
            created["workflow_id"],
            target["workflow"]["version"],
        )
        approved = _approve(
            client,
            created["workflow_id"],
            proposed["workflow"]["version"],
            proposed["artifact"]["artifact_version"],
        )
    return created, approved


def _orchestrator(repository, transport):
    start = max(
        workflow.updated_at for workflow in repository._workflows.values()
    ) + timedelta(seconds=1)
    times = iter(
        start + timedelta(seconds=index)
        for index in range(10)
    )
    return WorkflowTransportOrchestrator(
        WorkflowPersistenceService(repository),
        transport_service=transport,
        clock=lambda: next(times),
        uuid_factory=lambda: ATTEMPT_ID,
    )


def _run(orchestrator, created, approved, *, key="load-staging"):
    return orchestrator.run(
        UUID(created["workflow_id"]),
        expected_version=approved["workflow"]["version"],
        source_discovery_artifact_version=1,
        approved_mapping_artifact_version=approved["artifact"]["artifact_version"],
        source_profile_id="pg-source",
        target_profile_id="sf-target",
        batch_size=100,
        timeout_seconds=20,
        idempotency_key=key,
    )


def test_success_is_claimed_once_persists_sanitized_evidence_and_replays() -> None:
    repository = InMemoryWorkflowRepository()
    created, approved = _approved(repository)
    transport = FakeTransport(BatchTransportDisposition.SUCCEEDED)
    first = _run(_orchestrator(repository, transport), created, approved)

    replay_transport = FakeTransport(BatchTransportDisposition.SUCCEEDED)
    replay = _run(_orchestrator(repository, replay_transport), created, approved)

    assert first.workflow.status is MigrationWorkflowStatus.STAGED
    assert first.workflow.version == approved["workflow"]["version"] + 2
    assert first.evidence.rows_read == first.evidence.rows_written == 3
    assert first.evidence.batch_count == 2
    assert first.evidence.staging_relation.object_name == (
        "SB_STAGE_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    )
    assert first.artifact.artifact_type.value == "STAGING_LOAD_EVIDENCE"
    assert replay.evidence == first.evidence
    assert len(transport.run_calls) == 1
    assert replay_transport.prepare_calls == replay_transport.run_calls == []


def test_confirmed_failure_returns_to_approved_only_after_cleanup() -> None:
    repository = InMemoryWorkflowRepository()
    created, approved = _approved(repository)
    transport = FakeTransport(
        BatchTransportDisposition.CONFIRMED_FAILED_CLEANED_UP
    )

    with pytest.raises(WorkflowTransportConfirmedFailureError):
        _run(_orchestrator(repository, transport), created, approved)

    workflow = repository.get_workflow(UUID(created["workflow_id"]))
    attempt = repository.get_transport_attempt(ATTEMPT_ID)
    assert workflow.status is MigrationWorkflowStatus.MAPPING_APPROVED
    assert attempt.status.value == "FAILED_CLEANED_UP"
    assert attempt.evidence_artifact_id is None


@pytest.mark.parametrize("raises", [False, True])
def test_uncertain_or_unexpected_remote_outcome_blocks_retry(raises: bool) -> None:
    repository = InMemoryWorkflowRepository()
    created, approved = _approved(repository)
    transport = FakeTransport(
        BatchTransportDisposition.OUTCOME_UNCERTAIN,
        raises=raises,
    )

    with pytest.raises(WorkflowTransportOutcomeUncertainError):
        _run(_orchestrator(repository, transport), created, approved)

    workflow = repository.get_workflow(UUID(created["workflow_id"]))
    attempt = repository.get_transport_attempt(ATTEMPT_ID)
    assert workflow.status is MigrationWorkflowStatus.STAGING_RECOVERY_REQUIRED
    assert attempt.status.value == "OUTCOME_UNCERTAIN"


def test_stale_source_discovery_is_rejected_before_transport_preparation() -> None:
    repository = InMemoryWorkflowRepository()
    created, approved = _approved(repository)
    transport = FakeTransport(BatchTransportDisposition.SUCCEEDED)
    orchestrator = _orchestrator(repository, transport)

    with pytest.raises(WorkflowStaleArtifactReferenceError):
        orchestrator.run(
            UUID(created["workflow_id"]),
            expected_version=approved["workflow"]["version"],
            source_discovery_artifact_version=2,
            approved_mapping_artifact_version=approved["artifact"]["artifact_version"],
            source_profile_id="pg-source",
            target_profile_id="sf-target",
            batch_size=100,
            timeout_seconds=20,
            idempotency_key="stale-source",
        )

    assert transport.prepare_calls == transport.run_calls == []
