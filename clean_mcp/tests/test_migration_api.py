"""HTTP contract and workflow tests for the production migration API."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from api.adapters.migrations import table_to_api, table_to_domain
from api.app import create_app
from api.config import ApiSettings
from api.dependencies import (
    get_schema_discovery_service,
    get_schema_mapping_service,
    get_validation_execution_service_factory,
)
from models.discovery import (
    CheckConstraintMetadata,
    ConstraintType,
    CoverageStatus,
    DatabaseObjectType,
    DiscoveryCoverage,
    ForeignKeyMetadata,
    KeyConstraintMetadata,
    ObjectPersistence,
    TableMetadata,
)
from models.metadata import CanonicalType, ColumnMetadata

BASE = "/api/v1/migrations"


def _coverage(status: CoverageStatus = CoverageStatus.COMPLETE, warnings=()) -> DiscoveryCoverage:
    return DiscoveryCoverage(
        columns=status,
        primary_key=status,
        unique_constraints=status,
        foreign_keys=status,
        check_constraints=status,
        comments=status,
        estimated_row_count=status,
        view_definition=CoverageStatus.NOT_APPLICABLE,
        partitioning=CoverageStatus.NOT_APPLICABLE,
        clustering=CoverageStatus.NOT_APPLICABLE,
        warnings=tuple(warnings),
    )


def _column(name: str, ordinal: int, kind: CanonicalType = CanonicalType.STRING, table: str = "Source.Table") -> ColumnMetadata:
    return ColumnMetadata(
        catalog_name='Data.B"ase',
        schema_name="München Schema",
        table_name=table,
        column_name=name,
        ordinal_position=ordinal,
        native_type="VARCHAR" if kind is CanonicalType.STRING else "INTEGER",
        canonical_type=kind,
        nullable=False,
        character_length=200 if kind is CanonicalType.STRING else None,
        numeric_precision=38 if kind is CanonicalType.INTEGER else None,
        numeric_scale=0 if kind is CanonicalType.INTEGER else None,
        datetime_precision=None,
        comment="exact punctuation — preserved",
        vendor_metadata={},
    )


def _table(system: str, name: str, columns: tuple[ColumnMetadata, ...], *, partial: bool = False) -> TableMetadata:
    status = CoverageStatus.PARTIAL if partial else CoverageStatus.COMPLETE
    warning = ("CONSTRAINT_MEMBERSHIP_PARTIAL",) if partial else ()
    primary = KeyConstraintMetadata(
        name='PK "Main"', constraint_type=ConstraintType.PRIMARY_KEY, columns=(columns[0].column_name,), vendor_metadata={}
    )
    unique = KeyConstraintMetadata(
        name="UQ.name", constraint_type=ConstraintType.UNIQUE, columns=(columns[-1].column_name,), vendor_metadata={}
    )
    return TableMetadata(
        catalog_name='Data.B"ase',
        schema_name="München Schema",
        object_name=name,
        system=system,
        object_type=DatabaseObjectType.TABLE,
        persistence=ObjectPersistence.PERMANENT,
        comment="table comment",
        columns=columns,
        primary_key=primary,
        unique_constraints=(unique,),
        foreign_keys=(ForeignKeyMetadata(
            name="FK.parent", local_columns=(columns[0].column_name,), referenced_catalog=None,
            referenced_schema="public", referenced_table="Parent.Table", referenced_columns=("id",), vendor_metadata={},
        ),),
        check_constraints=(CheckConstraintMetadata(name="CK.age", expression='"age" >= 0', vendor_metadata={}),),
        coverage=_coverage(status, warning),
        vendor_metadata={},
    )


def _workflow_tables() -> tuple[TableMetadata, TableMetadata]:
    source = _table("postgresql", "Source.Table", (
        _column("first_name", 1), _column("last_name", 2), _column("age", 3, CanonicalType.INTEGER),
    ))
    target = _table("snowflake", 'Target "People"', (
        _column("full_name", 1, table='Target "People"'),
        _column("age", 2, CanonicalType.INTEGER, table='Target "People"'),
    ), partial=True)
    return source, target


def _json_table(value: TableMetadata) -> dict:
    return table_to_api(value).model_dump(mode="json")


def _discover_app(tables: dict[str, TableMetadata | None], calls: list):
    app = create_app()

    class Connector:
        def __init__(self, profile: str):
            self.profile = profile

        def get_table_metadata(self, **kwargs):
            calls.append((self.profile, dict(kwargs)))
            return tables.get(self.profile)

    def resolver(profile_id: str):
        if profile_id == "missing":
            class UnknownProfileError(ValueError):
                pass
            raise UnknownProfileError("sensitive profile and driver details")
        if profile_id == "broken":
            class SchemaDiscoveryError(RuntimeError):
                pass
            raise SchemaDiscoveryError("SELECT secret FROM driver")
        return Connector(profile_id)

    app.dependency_overrides[get_schema_discovery_service] = lambda: resolver
    return app


def test_canonical_metadata_round_trip_preserves_order_identity_and_partial_coverage() -> None:
    source, target = _workflow_tables()
    for table in (source, target):
        transported = table_to_api(table)
        restored = table_to_domain(transported)
        assert restored == table
        assert [item.column_name for item in restored.columns] == [item.column_name for item in table.columns]
        assert restored.primary_key.columns == table.primary_key.columns
        assert restored.unique_constraints[0].columns == table.unique_constraints[0].columns
    assert table_to_api(target).coverage.primary_key is CoverageStatus.PARTIAL
    assert table_to_api(target).coverage.warnings == ("CONSTRAINT_MEMBERSHIP_PARTIAL",)


def test_discovery_success_profile_isolation_headers_and_exact_identifiers() -> None:
    source, target = _workflow_tables()
    calls: list = []
    with TestClient(_discover_app({"pg": source, "sf": target}, calls)) as client:
        pg = client.post(f"{BASE}/discover", json={"profile_id": "pg", "schema_name": source.schema_name, "table_name": source.object_name})
        sf = client.post(f"{BASE}/discover", json={"profile_id": "sf", "database_name": target.catalog_name, "schema_name": target.schema_name, "table_name": target.object_name})
    assert pg.status_code == sf.status_code == 200
    assert pg.json()["object_name"] == "Source.Table"
    assert sf.json()["object_name"] == 'Target "People"'
    assert sf.json()["coverage"]["primary_key"] == "PARTIAL"
    assert calls == [
        ("pg", {"database": None, "schema": "München Schema", "table": "Source.Table"}),
        ("sf", {"database": 'Data.B"ase', "schema": "München Schema", "table": 'Target "People"'}),
    ]
    for response in (pg, sf):
        assert response.headers["Cache-Control"] == "no-store"
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert response.headers["X-Request-ID"]


@pytest.mark.parametrize(
    ("profile", "status", "code"),
    [("missing", 404, "PROFILE_NOT_FOUND"), ("empty", 404, "TABLE_NOT_FOUND"), ("broken", 502, "DISCOVERY_FAILED")],
)
def test_discovery_errors_are_fixed_and_redacted(profile, status, code) -> None:
    app = _discover_app({"empty": None}, [])
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post(f"{BASE}/discover", json={"profile_id": profile, "schema_name": "s", "table_name": "t"})
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert all(text not in response.text.casefold() for text in ("select", "driver", profile.casefold()))


@pytest.mark.parametrize("field", ["password", "secret", "credentials", "connection_string", "connector_options", "raw_sql", "query_text"])
def test_request_boundary_rejects_credentials_connection_data_and_arbitrary_sql(field) -> None:
    app = _discover_app({}, [])
    payload = {"profile_id": "p", "schema_name": "s", "table_name": "t", field: "forbidden"}
    with TestClient(app) as client:
        response = client.post(f"{BASE}/discover", json=payload)
    assert response.status_code == 422
    assert field not in response.text


@pytest.mark.parametrize("identifier", ["   ", "bad\x00name"])
def test_malformed_identifiers_are_rejected_before_discovery(identifier) -> None:
    calls = []
    app = _discover_app({}, calls)
    with TestClient(app) as client:
        response = client.post(f"{BASE}/discover", json={"profile_id": "p", "schema_name": identifier, "table_name": "t"})
    assert response.status_code == 422
    assert calls == []


def test_mapping_suggestion_uses_real_engine_and_resolves_no_database_dependency() -> None:
    source, target = _workflow_tables()
    app = create_app()
    calls = []
    real_provider = get_schema_mapping_service
    service = real_provider()
    app.dependency_overrides[get_schema_mapping_service] = lambda: (calls.append("mapping") or service)
    with TestClient(app) as client:
        response = client.post(f"{BASE}/mappings/suggest", json={"source": _json_table(source), "target": _json_table(target)})
    assert response.status_code == 200
    body = response.json()
    assert calls == ["mapping"]
    assert all(item["decision"] != "APPROVED" for item in body["suggestions"])
    age = next(item for item in body["suggestions"] if item["source_column"] == "age")
    assert age["target_column"] == "age" and age["decision"] == "SUGGESTED"
    assert [item["code"] for item in age["evidence"]][:2] == ["EXACT_NATIVE_NAME", "EXACT_CANONICAL_TYPE"]


def _approval_payload(source: TableMetadata, target: TableMetadata, plan: dict) -> dict:
    return {
        "plan": plan,
        "source": _json_table(source),
        "target": _json_table(target),
        "decisions": [
            {
                "source_column": "first_name", "target_column": "full_name", "status": "APPROVED",
                "reviewer_note": "must never enter SQL",
                "transformation": {"expression_type": "CONCAT", "source_columns": ["first_name", "last_name"], "separator": " "},
            },
            {
                "source_column": "age", "status": "APPROVED",
                "transformation": {"expression_type": "DIRECT_COPY", "source_columns": ["age"]},
            },
            {"source_column": "last_name", "status": "REJECTED"},
        ],
    }


def _get_approved(client: TestClient, source: TableMetadata, target: TableMetadata) -> tuple[dict, dict]:
    suggested = client.post(f"{BASE}/mappings/suggest", json={"source": _json_table(source), "target": _json_table(target)})
    assert suggested.status_code == 200
    approval_payload = _approval_payload(source, target, suggested.json())
    approved = client.post(f"{BASE}/mappings/approve", json=approval_payload)
    assert approved.status_code == 200, approved.text
    return approved.json(), approval_payload


def test_approval_supports_explicit_states_and_preserves_plan_and_evidence() -> None:
    source, target = _workflow_tables()
    with TestClient(create_app()) as client:
        approved, payload = _get_approved(client, source, target)
    assert [item["status"] for item in approved["approvals"]] == ["APPROVED", "REJECTED", "APPROVED"]
    assert [item["source_column"] for item in approved["approved_mappings"]] == ["first_name", "age"]
    assert approved["approvals"][0]["original_evidence"] == payload["plan"]["suggestions"][0]["evidence"]
    assert payload["plan"] == _approval_payload(source, target, payload["plan"])["plan"]


@pytest.mark.parametrize(
    "decision_patch",
    [
        {"source_column": "first_name", "target_column": "full_name", "status": "OVERRIDDEN", "override_reason": ""},
        {"source_column": "first_name", "target_column": "age", "status": "APPROVED"},
        {"source_column": "unknown", "target_column": "age", "status": "APPROVED"},
    ],
)
def test_approval_conflicts_are_safe(decision_patch) -> None:
    source, target = _workflow_tables()
    with TestClient(create_app()) as client:
        plan = client.post(f"{BASE}/mappings/suggest", json={"source": _json_table(source), "target": _json_table(target)}).json()
        payload = {"plan": plan, "source": _json_table(source), "target": _json_table(target), "decisions": [decision_patch]}
        response = client.post(f"{BASE}/mappings/approve", json=payload)
    assert response.status_code in {409, 422}
    assert "unknown" not in response.text.casefold()


def test_override_and_all_transformation_expression_types_cross_http_boundary() -> None:
    source, target = _workflow_tables()
    expression_cases = (
        {"expression_type": "SOURCE_COLUMN", "source_columns": ["age"]},
        {"expression_type": "CAST", "source_columns": ["age"], "target_canonical_type": "STRING"},
        {"expression_type": "LITERAL", "literal_value": "fixed"},
        {"expression_type": "COALESCE", "source_columns": ["age"], "arguments": [{"expression_type": "LITERAL", "literal_value": 0}]},
    )
    with TestClient(create_app()) as client:
        plan = client.post(f"{BASE}/mappings/suggest", json={"source": _json_table(source), "target": _json_table(target)}).json()
        for expression in expression_cases:
            response = client.post(f"{BASE}/mappings/approve", json={
                "plan": plan, "source": _json_table(source), "target": _json_table(target),
                "decisions": [{"source_column": "age", "target_column": "age", "status": "APPROVED", "transformation": expression}],
            })
            assert response.status_code == 200, response.text
            assert response.json()["approved_mappings"][0]["transformation"]["expression_type"] == expression["expression_type"]
        overridden = client.post(f"{BASE}/mappings/approve", json={
            "plan": plan, "source": _json_table(source), "target": _json_table(target),
            "decisions": [{
                "source_column": "age", "target_column": "full_name", "status": "OVERRIDDEN",
                "override_reason": "reviewed incompatible conversion",
                "transformation": {"expression_type": "CAST", "source_columns": ["age"], "target_canonical_type": "STRING"},
            }],
        })
    assert overridden.status_code == 200
    assert overridden.json()["approved_mappings"][0]["status"] == "OVERRIDDEN"


def test_transformation_and_validation_previews_are_deterministic_and_non_executing() -> None:
    source, target = _workflow_tables()
    with TestClient(create_app()) as client:
        approved, _ = _get_approved(client, source, target)
        request = {"approved_plan": approved, "staging_database": 'Stage.DB', "staging_schema": 'S"chema', "staging_table": 'x"; DROP TABLE y;--', "statement_type": "SELECT"}
        select = client.post(f"{BASE}/transformations/preview", json=request)
        request["statement_type"] = "INSERT_SELECT"
        insert = client.post(f"{BASE}/transformations/preview", json=request)
        validation = client.post(f"{BASE}/validations/preview", json={
            "approved_plan": approved, "source_schema": "public", "source_table": "Source.Table",
            "target_database": 'Data.B"ase', "target_schema": "München Schema", "target_table": 'Target "People"',
        })
    assert select.status_code == insert.status_code == validation.status_code == 200
    assert select.json()["preview_only"] is True and insert.json()["preview_only"] is True
    assert select.json()["parameters"] == [" "] and "%s" in select.json()["sql"]
    assert "must never enter SQL" not in select.json()["sql"]
    assert insert.json()["statement_type"] == "INSERT_SELECT"
    pair = validation.json()
    assert pair["preview_only"] is True
    assert [pair["source"]["dialect"], pair["target"]["dialect"]] == ["POSTGRESQL", "SNOWFLAKE"]
    assert pair["source"]["parameters"] == [" "] and pair["target"]["parameters"] == []
    assert [item["check_id"] for item in pair["source"]["checks"]] == pair["source"]["metric_aliases"]


def test_validation_approval_false_blocks_before_service_factory() -> None:
    source, target = _workflow_tables()
    app = create_app()
    calls = []
    app.dependency_overrides[get_validation_execution_service_factory] = lambda: (lambda: calls.append("resolved"))
    with TestClient(app) as client:
        approved, _ = _get_approved(client, source, target)
        response = client.post(f"{BASE}/validations/execute", json={
            "approved_plan": approved, "source_profile_id": "pg", "target_profile_id": "sf",
            "source_schema": "public", "source_table": "source", "target_database": "db",
            "target_schema": "s", "target_table": "target", "timeout_seconds": 9, "explicitly_approved": False,
        })
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "VALIDATION_APPROVAL_REQUIRED"
    assert calls == []


@pytest.mark.parametrize("timeout", [True, 0, -1])
def test_validation_timeout_is_positive_and_non_boolean(timeout) -> None:
    source, target = _workflow_tables()
    with TestClient(create_app()) as client:
        approved, _ = _get_approved(client, source, target)
        response = client.post(f"{BASE}/validations/execute", json={
            "approved_plan": approved, "source_profile_id": "pg", "target_profile_id": "sf",
            "source_schema": "s", "source_table": "t", "target_database": "d", "target_schema": "s",
            "target_table": "t", "timeout_seconds": timeout, "explicitly_approved": True,
        })
    assert response.status_code == 422


def test_complete_http_workflow_executes_ordered_profile_bound_validation(monkeypatch) -> None:
    source, target = _workflow_tables()
    calls: list = []
    results = {
        "row_count": 2, "m000_null_count": 0, "m000_distinct_count": 2,
        "m001_null_count": 0, "m001_distinct_count": 2,
    }

    class QueryService:
        def __init__(self, profile):
            self.profile = profile

        def execute_query(self, **kwargs):
            calls.append((self.profile, deepcopy(kwargs)))
            return SimpleNamespace(success=True, data={"columns": list(results), "rows": [tuple(results.values())]})

    import services.validation_execution as execution_module
    monkeypatch.setattr(execution_module, "get_query_service", lambda profile: QueryService(profile))
    app = _discover_app({"pg-source": source, "sf-target": target}, [])
    with TestClient(app) as client:
        discovered_source = client.post(f"{BASE}/discover", json={"profile_id": "pg-source", "schema_name": source.schema_name, "table_name": source.object_name}).json()
        discovered_target = client.post(f"{BASE}/discover", json={"profile_id": "sf-target", "database_name": target.catalog_name, "schema_name": target.schema_name, "table_name": target.object_name}).json()
        suggested = client.post(f"{BASE}/mappings/suggest", json={"source": discovered_source, "target": discovered_target})
        approval_payload = _approval_payload(source, target, suggested.json())
        approval_snapshot = deepcopy(approval_payload)
        approved = client.post(f"{BASE}/mappings/approve", json=approval_payload).json()
        preview = client.post(f"{BASE}/transformations/preview", json={"approved_plan": approved, "staging_database": "stage", "staging_schema": "s", "staging_table": "t", "statement_type": "SELECT"})
        validation_preview = client.post(f"{BASE}/validations/preview", json={"approved_plan": approved, "source_schema": "public", "source_table": "source", "target_database": "db", "target_schema": "s", "target_table": "target"})
        executed = client.post(f"{BASE}/validations/execute", json={
            "approved_plan": approved, "source_profile_id": "pg-source", "target_profile_id": "sf-target",
            "source_schema": "public", "source_table": "source", "target_database": "db", "target_schema": "s",
            "target_table": "target", "timeout_seconds": 11, "explicitly_approved": True,
        })
    assert suggested.status_code == preview.status_code == validation_preview.status_code == executed.status_code == 200
    assert approval_payload == approval_snapshot
    assert preview.json()["parameters"] == [" "]
    assert calls[0][0] == "pg-source" and calls[1][0] == "sf-target"
    assert calls[0][1]["parameters"] == (" ",) and calls[1][1]["parameters"] == ()
    assert calls[0][1]["timeout_seconds"] == calls[1][1]["timeout_seconds"] == 11
    assert executed.json()["validation_report"]["status"] == "PASSED"
    assert executed.json()["validation_report"]["mismatched_count"] == 0


@pytest.mark.parametrize(
    ("target_change", "expected"),
    [(None, "PASSED"), ({"row_count": 3}, "FAILED"), ({"m001_distinct_count": None}, "INCOMPLETE")],
)
def test_execution_returns_validation_outcomes_as_success(monkeypatch, target_change, expected) -> None:
    source, target = _workflow_tables()
    base_metrics = {
        "row_count": 2, "m000_null_count": 0, "m000_distinct_count": 2,
        "m001_null_count": 0, "m001_distinct_count": 2,
    }
    source_metrics = dict(base_metrics)
    target_metrics = dict(base_metrics)
    if target_change:
        for key, value in target_change.items():
            if value is None:
                target_metrics.pop(key)
            else:
                target_metrics[key] = value

    class QueryService:
        def __init__(self, metrics):
            self.metrics = metrics

        def execute_query(self, **_kwargs):
            return SimpleNamespace(success=True, data={"columns": list(self.metrics), "rows": [tuple(self.metrics.values())]})

    import services.validation_execution as execution_module
    monkeypatch.setattr(execution_module, "get_query_service", lambda profile: QueryService(source_metrics if profile == "pg" else target_metrics))
    with TestClient(create_app()) as client:
        approved, _ = _get_approved(client, source, target)
        response = client.post(f"{BASE}/validations/execute", json={
            "approved_plan": approved, "source_profile_id": "pg", "target_profile_id": "sf",
            "source_schema": "public", "source_table": "source", "target_database": "db", "target_schema": "s",
            "target_table": "target", "timeout_seconds": 4, "explicitly_approved": True,
        })
    assert response.status_code == 200
    assert response.json()["validation_report"]["status"] == expected


def test_malformed_execution_result_is_redacted_502(monkeypatch) -> None:
    source, target = _workflow_tables()

    class QueryService:
        def execute_query(self, **_kwargs):
            return SimpleNamespace(success=True, data={"columns": ["row_count"], "rows": [(1,), (2,)]})

    import services.validation_execution as execution_module
    monkeypatch.setattr(execution_module, "get_query_service", lambda _profile: QueryService())
    with TestClient(create_app()) as client:
        approved, _ = _get_approved(client, source, target)
        response = client.post(f"{BASE}/validations/execute", json={
            "approved_plan": approved, "source_profile_id": "pg-secret", "target_profile_id": "sf-secret",
            "source_schema": "private", "source_table": "hidden", "target_database": "db", "target_schema": "s",
            "target_table": "target", "timeout_seconds": 4, "explicitly_approved": True,
        })
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "VALIDATION_RESULT_INVALID"
    assert all(value not in response.text for value in ("pg-secret", "sf-secret", "private", "hidden"))


def test_execution_failures_are_redacted_and_not_validation_mismatches() -> None:
    source, target = _workflow_tables()
    app = create_app()

    class ValidationExecutionError(ValueError):
        pass

    class Service:
        def run(self, _request):
            raise ValidationExecutionError("SELECT secret params profile pg driver query-id")

    app.dependency_overrides[get_validation_execution_service_factory] = lambda: Service
    with TestClient(app) as client:
        approved, _ = _get_approved(client, source, target)
        response = client.post(f"{BASE}/validations/execute", json={
            "approved_plan": approved, "source_profile_id": "pg", "target_profile_id": "sf",
            "source_schema": "s", "source_table": "t", "target_database": "d", "target_schema": "s",
            "target_table": "t", "timeout_seconds": 2, "explicitly_approved": True,
        })
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "VALIDATION_EXECUTION_FAILED"
    assert all(value not in response.text.casefold() for value in ("select", "params", "driver", "query-id", "profile pg"))


def test_openapi_workflow_contract_is_deterministic_unique_and_request_safe() -> None:
    app = create_app()
    first = app.openapi()
    assert first == app.openapi()
    expected = {
        f"{BASE}/discover", f"{BASE}/mappings/suggest", f"{BASE}/mappings/approve",
        f"{BASE}/transformations/preview", f"{BASE}/validations/preview", f"{BASE}/validations/execute",
    }
    assert expected.issubset(first["paths"])
    operation_ids = [operation["operationId"] for path in first["paths"].values() for method, operation in path.items() if method in {"get", "post"}]
    assert len(operation_ids) == len(set(operation_ids))
    schemas = first["components"]["schemas"]
    forbidden = {"password", "secret", "credential", "connection_string", "private_key", "access_token", "refresh_token", "connector_options", "raw_sql", "sql_text", "query_text"}
    visited = set()

    def scan(schema):
        if "$ref" in schema:
            name = schema["$ref"].rsplit("/", 1)[-1]
            if name in visited:
                return
            visited.add(name)
            scan(schemas[name])
        for name, child in schema.get("properties", {}).items():
            assert name.casefold() not in forbidden
            scan(child)
        for key in ("items",):
            if isinstance(schema.get(key), dict):
                scan(schema[key])
        for key in ("anyOf", "oneOf", "allOf"):
            for child in schema.get(key, []):
                scan(child)

    for path in expected:
        body = first["paths"][path]["post"]["requestBody"]["content"]["application/json"]["schema"]
        scan(body)


def test_workflow_body_limit_and_independent_apps() -> None:
    first = create_app(ApiSettings(max_request_body_bytes=32))
    second = create_app()
    first.dependency_overrides[get_schema_discovery_service] = lambda: pytest.fail
    assert get_schema_discovery_service not in second.dependency_overrides
    with TestClient(first) as client:
        response = client.post(f"{BASE}/discover", content=b"{" + b"x" * 100 + b"}", headers={"Content-Type": "application/json"})
    assert response.status_code == 413
