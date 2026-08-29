"""Optional Docker-friendly proof of strict validation against PostgreSQL."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest

from schemabridge.connectors.postgresql.connector import PostgreSQLConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.mapping import (
    MappingApprovalStatus,
    MappingReviewDecision,
    TransformationExpression,
    TransformationExpressionType,
)
from schemabridge.services.database_service import DatabaseService
from schemabridge.validation.execution import MigrationValidationExecutionService
def _profile(profile_id: str) -> ConnectionProfile:
    host = os.getenv("SCHEMABRIDGE_POSTGRES_HOST")
    database = os.getenv("SCHEMABRIDGE_POSTGRES_DATABASE")
    if not host or not database:
        pytest.skip("PostgreSQL strict-validation integration is not configured.")
    return ConnectionProfile(
        profile_id=profile_id,
        db_type="postgresql",
        host=host,
        database=database,
        username=os.getenv("SCHEMABRIDGE_POSTGRES_USERNAME", ""),
        password=os.getenv("SCHEMABRIDGE_POSTGRES_PASSWORD", ""),
        connection_options={"port": int(os.getenv("SCHEMABRIDGE_POSTGRES_PORT", "5432"))},
        max_rows=2,
    )


def _service(profile: ConnectionProfile) -> DatabaseService:
    return DatabaseService(profile, PostgreSQLConnector(profile=profile))


def test_strict_validation_detects_missing_and_extra_primary_keys():
    """Prove exact key reconciliation with disposable local PostgreSQL tables."""

    if os.getenv("SCHEMABRIDGE_STRICT_VALIDATION_POSTGRES_INTEGRATION") != "1":
        pytest.skip("PostgreSQL strict-validation integration is not enabled.")
    source_profile, target_profile = _profile("strict-source"), _profile("strict-target")
    source, target = _service(source_profile), _service(target_profile)
    suffix = uuid4().hex[:12]
    source_table, target_table = f"strict_source_{suffix}", f"strict_target_{suffix}"
    driver = PostgreSQLConnector(profile=source_profile)._driver()
    connection = driver.connect(
        host=source_profile.host,
        port=source_profile.connection_options_copy()["port"],
        dbname=source_profile.database,
        user=source_profile.username or None,
        password=source_profile.password or None,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE TABLE public."{source_table}" (id integer PRIMARY KEY, label text NOT NULL)')
            cursor.execute(f'CREATE TABLE public."{target_table}" (id integer PRIMARY KEY, label text NOT NULL)')
            cursor.execute(f'INSERT INTO public."{source_table}" (id, label) VALUES (1, \'one\'), (2, \'two\'), (3, \'three\')')
            cursor.execute(f'INSERT INTO public."{target_table}" (id, label) VALUES (1, \'one\'), (2, \'two\'), (4, \'four\')')
        connection.commit()

        source_metadata = source.get_table_metadata(
            database=source_profile.database, schema="public", table=source_table
        )
        target_metadata = target.get_table_metadata(
            database=target_profile.database, schema="public", table=target_table
        )
        from schemabridge.mapping.approval import MappingApprovalService
        from schemabridge.mapping.suggestions import SchemaMappingService
        from schemabridge.models.validation import MigrationValidationExecutionRequest, MigrationValidationStatus

        proposed = SchemaMappingService().suggest(source_metadata, target_metadata)
        approved = MappingApprovalService().apply(
            proposed,
            source=source_metadata,
            target=target_metadata,
            decisions=tuple(
                MappingReviewDecision(
                    source_column=column,
                    target_column=column,
                    status=MappingApprovalStatus.APPROVED,
                    transformation=TransformationExpression(
                        expression_type=TransformationExpressionType.DIRECT_COPY,
                        source_columns=(column,),
                    ),
                )
                for column in ("id", "label")
            ),
        )
        report = MigrationValidationExecutionService(
            database_service_factory=lambda profile_id: (
                source if profile_id == source_profile.profile_id else target
            )
        ).run(
            MigrationValidationExecutionRequest(
                source_profile_id=source_profile.profile_id,
                target_profile_id=target_profile.profile_id,
                approved_mapping_plan=approved,
                source_schema="public",
                source_table=source_table,
                target_database=target_profile.database,
                target_schema="public",
                target_table=target_table,
                explicitly_approved=True,
                strict_primary_key=True,
                source_table_metadata=source_metadata,
                target_table_metadata=target_metadata,
                primary_key_batch_size=2,
            )
        )

        assert report.validation_report.status is MigrationValidationStatus.FAILED
        assert report.primary_key_reconciliation is not None
        assert report.primary_key_reconciliation.missing_key_count == 1
        assert report.primary_key_reconciliation.extra_key_count == 1
        # PostgreSQL enforces a declared primary key, so a live duplicate-key
        # proof is impossible without corrupting database internals. Duplicate
        # reconciliation is covered in the pure unit tests and applies to
        # systems with informational/non-enforced keys.
        assert report.primary_key_reconciliation.target_duplicate_key_count == 0
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'DROP TABLE IF EXISTS public."{source_table}"')
                cursor.execute(f'DROP TABLE IF EXISTS public."{target_table}"')
            connection.commit()
        finally:
            connection.close()
