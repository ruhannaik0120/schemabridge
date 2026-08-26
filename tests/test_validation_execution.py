from __future__ import annotations
from dataclasses import replace
import pytest
from schemabridge.models.validation import (
    MigrationValidationStatus,
)
from schemabridge.models.discovery import ConstraintType, KeyConstraintMetadata
from schemabridge.models.mapping import (
    MappingApprovalStatus,
    MappingReviewDecision,
    TransformationExpression,
    TransformationExpressionType,
)
from schemabridge.services.database_service import DatabaseExecutionResult
from schemabridge.validation.execution import (
    MigrationValidationExecutionService,
    ValidationApprovalRequiredError,
    MalformedValidationExecutionResultError,
)
from tests.support.builders import approved_mapping_plan as _approved
from tests.support.builders import mapping_column, mapping_table
from tests.support.builders import validation_execution_request as _request


class FakeService:
    def __init__(self, name, metrics, events):
        self.name = name
        self.metrics = metrics
        self.events = events
        self.calls = []

    def validation_execution_context(self, timeout):
        return {
            "profile_id": self.name,
            "db_type": "postgresql" if self.name == "pg" else "snowflake",
            "validation_dialect": "POSTGRESQL" if self.name == "pg" else "SNOWFLAKE",
            "timeout_seconds": timeout,
        }

    def execute_validation_query(self, **kwargs):
        self.calls.append(kwargs)
        self.events.append(self.name)
        return DatabaseExecutionResult(
            tuple(self.metrics), (tuple(self.metrics.values()),), None
        )


def test_approval_gate_prevents_resolution(monkeypatch):
    monkeypatch.setattr(
        "schemabridge.validation.execution.get_database_service",
        lambda _: (_ for _ in ()).throw(AssertionError()),
    )
    with pytest.raises(ValidationApprovalRequiredError):
        MigrationValidationExecutionService().run(_request(False))


def test_profile_isolation_parameter_order_and_reconciliation(monkeypatch):
    events = []
    metrics = {
        "row_count": 1,
        "m000_null_count": 0,
        "m000_distinct_count": 1,
        "m001_null_count": 0,
        "m001_distinct_count": 1,
    }
    pg = FakeService("pg", metrics, events)
    sf = FakeService("sf", metrics, events)
    monkeypatch.setattr(
        "schemabridge.validation.execution.get_database_service",
        lambda key: pg if key == "pg" else sf,
    )
    report = MigrationValidationExecutionService().run(_request())
    assert (
        events == ["pg", "sf"]
        and report.validation_report.status is MigrationValidationStatus.PASSED
    )
    assert report.validation_report.approved_plan_version == 1
    assert pg.calls[0]["parameters"] == (" ", " ") and sf.calls[0].get(
        "parameters"
    ) in (None, ())
    assert pg.calls[0]["timeout_seconds"] == sf.calls[0]["timeout_seconds"] == 9


def test_non_default_approved_plan_version_is_preserved(monkeypatch):
    events = []
    metrics = {
        "row_count": 1,
        "m000_null_count": 0,
        "m000_distinct_count": 1,
        "m001_null_count": 0,
        "m001_distinct_count": 1,
    }
    pg = FakeService("pg", metrics, events)
    sf = FakeService("sf", metrics, events)
    monkeypatch.setattr(
        "schemabridge.validation.execution.get_database_service",
        lambda key: pg if key == "pg" else sf,
    )
    report = MigrationValidationExecutionService().run(
        _request(plan=replace(_approved(), version=7))
    )
    assert report.validation_report.approved_plan_version == 7


def test_malformed_multiple_rows_is_not_a_validation_mismatch(monkeypatch):
    class Bad(FakeService):
        def execute_validation_query(self, **kwargs):
            return DatabaseExecutionResult(("row_count",), ((1,), (1,)), None)

    monkeypatch.setattr(
        "schemabridge.validation.execution.get_database_service",
        lambda key: Bad(key, {}, []),
    )
    with pytest.raises(MalformedValidationExecutionResultError):
        MigrationValidationExecutionService().run(_request())


def _strict_plan_and_tables():
    from schemabridge.mapping.approval import MappingApprovalService
    from schemabridge.mapping.suggestions import SchemaMappingService
    from dataclasses import replace

    key = lambda: KeyConstraintMetadata(
        name="pk_test",
        constraint_type=ConstraintType.PRIMARY_KEY,
        columns=("id",),
        vendor_metadata={},
    )
    source = replace(mapping_table("source", mapping_column("id")), primary_key=key())
    target = replace(mapping_table("target", mapping_column("id")), primary_key=key())
    proposed = SchemaMappingService().suggest(source, target)
    plan = MappingApprovalService().apply(
        proposed,
        source=source,
        target=target,
        decisions=(
            MappingReviewDecision(
                source_column="id",
                target_column="id",
                status=MappingApprovalStatus.APPROVED,
                transformation=TransformationExpression(
                    expression_type=TransformationExpressionType.DIRECT_COPY,
                    source_columns=("id",),
                ),
            ),
        ),
    )
    return plan, source, target


def test_strict_primary_key_reconciliation_fails_on_extra_target_key(monkeypatch):
    from dataclasses import replace

    class StrictFake(FakeService):
        def __init__(self, name, metrics, events, batches):
            super().__init__(name, metrics, events)
            self.batches = iter(batches)

        def execute_validation_query(self, **kwargs):
            self.calls.append(kwargs)
            self.events.append(self.name)
            if '"key_000"' in kwargs["sql"]:
                return DatabaseExecutionResult(("key_000",), next(self.batches), None)
            return DatabaseExecutionResult(
                tuple(self.metrics), (tuple(self.metrics.values()),), None
            )

    metrics = {"row_count": 2, "m000_null_count": 0, "m000_distinct_count": 2}
    events = []
    source = StrictFake("pg", metrics, events, (((1,), (2,)), ()))
    target = StrictFake("sf", metrics, events, (((1,), (2,)), ((3,),), ()))
    monkeypatch.setattr(
        "schemabridge.validation.execution.get_database_service",
        lambda key: source if key == "pg" else target,
    )
    plan, source_table, target_table = _strict_plan_and_tables()

    report = MigrationValidationExecutionService().run(
        replace(
            _request(plan=plan),
            strict_primary_key=True,
            source_table_metadata=source_table,
            target_table_metadata=target_table,
            primary_key_batch_size=2,
        )
    )

    assert report.validation_report.status is MigrationValidationStatus.FAILED
    assert report.primary_key_reconciliation is not None
    assert report.primary_key_reconciliation.extra_key_count == 1
    assert report.validation_report.warnings == ("STRICT_PRIMARY_KEY_MISMATCH",)
    assert all(call.get("max_rows") == 2 for call in source.calls[1:] + target.calls[1:])


def test_strict_primary_key_validation_is_incomplete_when_keys_are_not_eligible(monkeypatch):
    events = []
    metrics = {
        "row_count": 1,
        "m000_null_count": 0,
        "m000_distinct_count": 1,
        "m001_null_count": 0,
        "m001_distinct_count": 1,
    }
    pg = FakeService("pg", metrics, events)
    sf = FakeService("sf", metrics, events)
    monkeypatch.setattr(
        "schemabridge.validation.execution.get_database_service",
        lambda key: pg if key == "pg" else sf,
    )
    from dataclasses import replace
    from tests.support.builders import mapping_table

    report = MigrationValidationExecutionService().run(
        replace(
            _request(),
            strict_primary_key=True,
            source_table_metadata=mapping_table("source"),
            target_table_metadata=mapping_table("target"),
        )
    )

    assert report.validation_report.status is MigrationValidationStatus.INCOMPLETE
    assert report.primary_key_reconciliation is None
    assert report.validation_report.warnings == ("PRIMARY_KEY_UNAVAILABLE",)
