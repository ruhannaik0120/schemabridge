# SchemaBridge architecture

## System context

SchemaBridge is a synchronous FastAPI backend. The HTTP layer validates commands, orchestration services enforce workflow policy, domain services generate deterministic artifacts, and a repository stores durable state in a control-plane PostgreSQL database.

The database-operation path is:

```text
API client
    |
    v
FastAPI application and routes
    |
    v
Workflow planning / execution / validation orchestrators
    |
    v
DatabaseService
    |
    v
ProfileRegistry
    |
    v
ConnectorFactory
    |
    +--> PostgreSQL connector (source discovery and validation)
    |
    +--> Snowflake connector (target discovery, execution, validation)
```

Durable state follows a separate path:

```text
Workflow orchestrators
    |
    v
WorkflowPersistenceService
    |
    v
WorkflowRepository contract
    |
    v
PostgreSQLWorkflowRepository
    |
    v
Control-plane PostgreSQL
```

These paths meet in the orchestrators. Remote database work is performed outside the control-plane transaction, while durable claims and final evidence are stored before and after that remote boundary.

## Repository layers

| Layer | Main modules | Responsibility |
|---|---|---|
| Application | `schemabridge/api/app.py`, `api/dependencies.py` | Build FastAPI, own application lifecycle, and wire services. |
| Transport | `api/routes/`, `api/schemas/`, `api/adapters/` | Validate HTTP requests, map transport values to domain values, and translate stable errors. |
| Orchestration | `services/workflow_orchestration.py`, `workflow_execution.py`, `workflow_validation.py` | Enforce workflow state, artifact, idempotency, and remote-operation rules. |
| Domain services | `services/schema_mapping.py`, `mapping_approval.py`, `transformation_sql.py`, `validation_sql.py`, `reconciliation.py` | Perform deterministic mapping, approval, SQL compilation, and result comparison. |
| Database boundary | `services/database_service.py`, `profile_registry.py`, `connectors/` | Resolve named profiles, apply profile limits, and call vendor drivers. |
| Domain models | `models/` | Define immutable workflow, mapping, execution, validation, and metadata contracts. |
| Persistence | `persistence/` | Serialize artifacts and implement transactional workflow storage. |
| SQL guard | `validation/sql_guard.py` | Reject unsupported, multi-statement, commented, or structurally unsafe generated SQL. |

## Application startup

`schemabridge.api.app:create_app` is the ASGI factory used by Uvicorn and Docker.

1. `create_app` builds the FastAPI object, middleware, exception handlers, and routers.
2. Dependency functions remain lazy so importing the app or generating OpenAPI does not load database drivers or connect to a database.
3. During the lifespan startup, `ApiSettings` is validated.
4. If `SCHEMABRIDGE_CONTROL_PLANE_DSN` is configured, `build_workflow_repository` creates the app-owned `PostgreSQLWorkflowRepository`. Construction does not open a connection.
5. Readiness becomes true only after startup checks succeed.
6. Shutdown closes the repository and any cached profile-bound database services.

Application startup deliberately does not apply migrations. Migrations are an explicit operational command.

## HTTP surfaces

### Durable workflow endpoints

The primary stateful API is under `/api/v1/migrations/workflows`:

| Method and path | Purpose |
|---|---|
| `POST /api/v1/migrations/workflows` | Create a `DRAFT` workflow. |
| `GET /api/v1/migrations/workflows/{workflow_id}` | Read current durable state. |
| `POST .../{workflow_id}/discover-source` | Discover and persist source metadata. |
| `POST .../{workflow_id}/discover-target` | Discover and persist target metadata. |
| `POST .../{workflow_id}/mapping-proposals` | Generate and persist mapping suggestions. |
| `POST .../{workflow_id}/mapping-approvals` | Apply reviewer decisions and persist approval. |
| `POST .../{workflow_id}/transformation-previews` | Compile and persist transformation SQL. |
| `POST .../{workflow_id}/execute` | Claim and execute an approved target operation. |
| `POST .../{workflow_id}/validate` | Claim, execute, and persist paired validation. |
| `POST .../{workflow_id}/transitions` | Perform allowed administrative transitions. |
| `POST .../{workflow_id}/artifacts` | Append a typed artifact through the persistence policy. |
| `GET .../{workflow_id}/artifacts` | List immutable artifacts. |
| `GET .../{workflow_id}/audit-events` | List ordered audit history. |

Every durable mutation uses an `Idempotency-Key`. Commands after creation also carry an expected workflow version.

### Lower-level operation endpoints

`schemabridge/api/routes/migrations.py` exposes direct discovery, mapping, approval, transformation-preview, validation-preview, and validation-execution endpoints under `/api/v1/migrations`. They reuse domain services but do not represent the complete durable workflow. The workflow endpoints are the path that provides persistence, optimistic concurrency, artifact history, execution claims, and replay semantics.

### Health endpoints

- `GET /health/live` confirms that the process can answer.
- `GET /health/ready` confirms that the application lifespan completed.

Readiness does not prove that every external profile is reachable.

## Database roles

### Control-plane PostgreSQL

The control plane stores product state. It contains workflows, artifact bytes and hashes, audit events, idempotency records, execution attempts, and validation runs. Its DSN is supplied through `SCHEMABRIDGE_CONTROL_PLANE_DSN`.

It is not the migration source database. Keeping it separate lets SchemaBridge apply transactions, row locks, optimistic versions, and constraints to workflow state without mixing that state with customer data.

### Source PostgreSQL

The source profile is selected by the workflow's `source_profile_id`. It is used for source schema discovery and the PostgreSQL half of validation. SchemaBridge does not persist its credentials and does not extract its rows into Snowflake.

### Target Snowflake

The target profile is selected by `target_profile_id`. It is used for target discovery, approved write execution, and the Snowflake half of validation. The durable write path checks that the profile database exactly matches the workflow target and that `write_enabled=true`.

The generated `INSERT ... SELECT` reads a Snowflake staging relation supplied during transformation preview. Provisioning that staging relation is outside the current repository.

## Workflow path details

### 1. Workflow creation

`api/routes/workflows.py:create_workflow` converts the request to immutable relation models and calls `WorkflowPersistenceService.create_workflow`. The service:

- creates a version-1 `DRAFT` workflow;
- hashes the canonical command;
- creates the initial `WORKFLOW_CREATED` event;
- asks the repository to store workflow, event, and idempotency record atomically.

### 2. Source discovery

`WorkflowPlanningOrchestrator.discover_source` verifies the current state and resolves the workflow's source profile through `DatabaseService`. The PostgreSQL connector runs fixed catalog queries, normalizers build canonical `TableMetadata`, and the persistence service appends `SOURCE_DISCOVERY`.

### 3. Target discovery

`discover_target` follows the same boundary using the workflow's Snowflake target profile. Once both discovery artifacts exist, the workflow can enter `DISCOVERED`.

### 4. Mapping proposal

`generate_mapping` rehydrates the latest source and target discovery artifacts. `SchemaMappingService` produces deterministic suggestions and evidence. The result is persisted as `MAPPING_PLAN`, and the workflow enters `MAPPING_PROPOSED`.

### 5. Mapping approval

`approve_mapping` requires an explicit mapping artifact version and reviewer decisions. `MappingApprovalService` verifies source and target identities, rejects unknown columns and target reuse, and records pending, approved, rejected, or overridden decisions in `APPROVED_MAPPING_PLAN`.

### 6. Transformation preview

`preview_transformation` rehydrates the approved plan and calls `SnowflakeTransformationSqlCompiler`. A `SELECT` can be produced for review, but durable execution requires an `INSERT_SELECT` preview. The preview records source and target relations, parameters, columns, and approved-plan version.

### 7. Execution

`WorkflowExecutionOrchestrator.execute` performs the following checks before a target call:

1. Calculate the command hash and return a completed exact replay when available.
2. Require an execution-ready workflow and reject in-progress or recovery-required state.
3. Load the referenced approved-mapping and transformation-preview artifacts.
4. Require current artifact versions for a current command.
5. Require no pending mapping decisions.
6. Require an `INSERT_SELECT` preview tied to the approved-plan version.
7. Recompile from the persisted approved mapping and require exact equality with the preview.
8. Validate the SQL structure.
9. Resolve the exact Snowflake target profile and require `write_enabled=true`.
10. Persist a unique execution fingerprint and durable `CLAIMED` attempt.
11. Atomically acquire the `RUNNING` state before calling Snowflake.
12. Persist sanitized evidence and the resulting workflow state.

This ordering prevents a stale preview, altered SQL, duplicate caller, or disabled profile from bypassing approval.

### 8. Validation

`WorkflowValidationOrchestrator.validate` requires successful committed execution evidence and the approved mapping. It recompiles a safe validation plan, claims a validation run, marks it running, and delegates to `MigrationValidationExecutionService`.

The execution service resolves the source and target profiles independently, requires PostgreSQL on the source side and Snowflake on the target side, executes one read-only aggregate query per side, and rejects malformed multi-row results.

### 9. Reconciliation

`reconcile_validation_results` compares metric aliases defined in the generated plan. It does not compare arbitrary driver columns. The final report distinguishes matches, mismatches, and unavailable counts.

### 10. Artifact and audit persistence

`WorkflowPersistenceService` validates artifact ownership and type before serialization. `artifact_codec.py` performs the inverse operation when an orchestrator needs a typed domain value.

`PostgreSQLWorkflowRepository` stores the workflow update, artifact, audit event, and idempotency result inside one control-plane transaction. Artifacts are append-only; workflow and artifact versions are not inferred from client input.

## Important design decisions

### Why clients cannot submit migration SQL

The durable execute request contains artifact versions, a target profile ID, and a timeout. It contains no SQL field. SQL is generated from modeled transformations, so approval applies to domain intent rather than an arbitrary string.

### Why approved mappings are persisted

Persistence creates a reviewable, immutable boundary. Execution and validation can prove which source columns, target columns, transformations, and override reasons were authorized, even after the API process restarts.

### Why SQL is recompiled before execution

A persisted preview is evidence, not authority by itself. Recompiling from the approved mapping and comparing the full generated value ensures that a stale or altered preview cannot bypass the approval boundary.

### Why `write_enabled` exists

Selecting a Snowflake profile is not sufficient authorization for writes. The profile must opt in explicitly, and the exact configured database must match the workflow target. This provides a configuration-level kill switch in addition to workflow approval.

### Why idempotency exists

Clients retry when responses are lost. An idempotency key plus canonical request hash distinguishes an exact retry from a different command using the same key. Durable remote-operation claims prevent a reconstructed process from repeating completed work.

### Why optimistic concurrency exists

Two reviewers or clients may act on the same workflow version. `expected_version` allows only one current mutation to win while preserving exact replay behavior for the other caller.

### Why uncertain outcomes use recovery states

The target database and control plane cannot commit atomically. If the remote response is lost, retrying may duplicate a write. Recovery states stop automatic progress until an operator determines what happened.

### Why validation is separate from execution

A committed write and a data comparison answer different questions. Execution evidence records the target operation outcome; validation evidence records source/target metric agreement. A mismatch therefore leads to review rather than rewriting execution history.

## Transaction boundaries

Control-plane writes use PostgreSQL transactions and locks. Remote discovery, Snowflake execution, and source/target validation occur outside those transactions.

The durable claim pattern is:

```text
control-plane transaction: create claim
remote operation: execute once
control-plane transaction: store classified result and evidence
```

This cannot provide a distributed transaction. It provides an explicit record of what is known and quarantines what is not known.

## Extension boundaries

New generic connectors implement `DatabaseConnector`, export `Connector`, and register a module path in `ConnectorFactory`. Extending the durable migration workflow requires additional policy work beyond registering a connector, because execution currently enforces a PostgreSQL-source/Snowflake-target contract.
