# SchemaBridge code guide

If you are new to SchemaBridge, read the files in this order.

1. `README.md` — the project boundary, quick start, and honest limitations.
2. `docs/PRD.md` — the product behavior and vocabulary.
3. `schemabridge/api/app.py` — how the FastAPI application is assembled.
4. `schemabridge/api/routes/workflows.py` — the durable HTTP workflow.
5. `schemabridge/models/workflow.py` — workflow states, artifacts, and audit events.
6. `schemabridge/services/workflow_orchestration.py` — discovery, mapping, approval, and preview coordination.
7. `schemabridge/services/workflow_execution.py` — approval-gated execution coordination.
8. `schemabridge/services/workflow_validation.py` — validation and reconciliation coordination.
9. `schemabridge/services/database_service.py` — profile-bound database access.
10. `schemabridge/services/schema_mapping.py`, `mapping_approval.py`, and `transformation_sql.py` — the deterministic migration logic.
11. `schemabridge/persistence/repository.py` and `postgresql.py` — the durable control plane.
12. `schemabridge/connectors/` — concrete PostgreSQL and Snowflake boundaries.
13. `Dockerfile`, `compose.yaml`, and `scripts/` — packaging and operation.

The durable migration path currently plans a PostgreSQL-to-Snowflake migration, but execution compiles a Snowflake `INSERT ... SELECT` from a caller-specified relation that must already exist in Snowflake. There is no PostgreSQL row extractor or Snowflake staging loader in this repository. Keep that distinction in mind while reading.

## A useful mental model

SchemaBridge has four layers:

```text
HTTP request
    -> FastAPI route and API schema
    -> workflow orchestrator and domain service
    -> profile-bound connector (remote data plane)
       and workflow repository (local control plane)
```

The data plane contains the source PostgreSQL and target Snowflake databases. The control plane is a separate PostgreSQL database that records decisions and history; it does not carry migrated business rows.

## Where does the application start?

### `schemabridge/api/app.py`

- **What is this file for?** It is the application factory. It creates FastAPI, installs middleware and error handlers, mounts routers, and owns process-lifetime cleanup.
- **Who calls it?** Uvicorn calls `create_app` through `schemabridge.api.app:create_app --factory`; tests also call the factory directly.
- **What does it call?** API settings, middleware, error-handler installers, and the health, migration, and workflow routers.
- **Important objects:** `create_app`, `_lifespan`, and cleanup helpers.
- **Before reading:** Understand that route inclusion must not open database connections. Connections are created lazily when an operation needs them.
- **Interview question:** Why use an application factory? It gives tests an isolated app, keeps configuration explicit, and gives resources a clear lifespan.

### `schemabridge/api/config.py`

- **Purpose:** Loads API-only settings such as service metadata and request-size limits from environment variables.
- **Called by:** `create_app`.
- **Calls:** Plain environment parsing; it does not resolve database profiles.
- **Important object:** `ApiSettings`.
- **Interview question:** Why keep API settings separate from connection profiles? The web platform and database credentials have different responsibilities and lifetimes.

## Where are API endpoints?

### `schemabridge/api/routes/workflows.py`

- **Purpose:** Exposes the durable migration workflow under `/api/v1/migrations/workflows`.
- **Called by:** FastAPI after the app includes its router.
- **Calls:** Planning, execution, validation, and persistence services supplied by dependency injection.
- **Important endpoints:** create/get workflow; discover source/target; generate/approve mappings; preview/execute a transformation; validate; transition; list artifacts and audit events.
- **Before reading:** Every mutation uses an `Idempotency-Key`. After creation, commands also carry the expected workflow version for optimistic concurrency.
- **Interview question:** Why do routes pass artifact versions rather than SQL? An artifact reference binds execution to reviewed, immutable control-plane evidence.

### `schemabridge/api/routes/migrations.py`

- **Purpose:** Exposes lower-level discovery, mapping, approval, transformation-preview, and validation helpers under `/api/v1/migrations`.
- **Called by:** FastAPI clients that want individual operations without a durable workflow.
- **Calls:** Domain services and API/domain adapters.
- **Important endpoints:** `discover`, `suggest-mappings`, `approve-mappings`, `preview-transformation`, `preview-validation`, and `execute-validation`.
- **Before reading:** These helpers do not provide the workflow repository's state machine, artifacts, audit events, idempotency records, or execution claim.
- **Interview question:** Why retain both route groups? Stateless operations make the domain functions independently usable, while the workflow API adds governance and durability.

### Supporting API files

- `schemabridge/api/dependencies.py` builds services lazily and obtains the app-owned workflow repository.
- `schemabridge/api/schemas/` defines strict Pydantic request and response contracts.
- `schemabridge/api/adapters/` translates between API schemas and immutable domain models.
- `schemabridge/api/middleware.py` applies request IDs, body-size limits, and security headers.
- `schemabridge/api/errors.py` converts known failures into stable, sanitized HTTP errors.
- `schemabridge/api/routes/health.py` provides liveness and readiness without pretending that optional remote profiles are healthy.

An interview may ask why adapters exist. They prevent HTTP serialization concerns from leaking into domain models and allow either layer to change independently.

## Where is workflow state defined?

### `schemabridge/models/workflow.py`

- **Purpose:** Defines the control-plane vocabulary.
- **Called by:** Routes, orchestrators, repository implementations, artifact codecs, and tests.
- **Important objects:** `MigrationWorkflowStatus`, `WorkflowArtifactType`, `MigrationAuditEventType`, `MigrationWorkflow`, `WorkflowArtifact`, and `MigrationAuditEvent`.
- **Before reading:** A workflow row is current mutable state; artifacts and audit events are immutable history.
- **Interview question:** Why store both a current status and an audit trail? Current state supports efficient decisions; append-only events explain how that state was reached.

The legal transition graph is enforced in `schemabridge/services/workflow_persistence.py`, not in the enum itself. Execution attempt types live in `schemabridge/models/execution.py`; validation run types live in `schemabridge/models/workflow_validation.py`.

## Where does workflow planning happen?

### `schemabridge/services/workflow_orchestration.py`

- **Purpose:** Coordinates durable source discovery, target discovery, mapping proposal, mapping approval, and transformation preview.
- **Called by:** Durable workflow routes.
- **Calls:** `WorkflowPersistenceService`, `DatabaseService`, mapping/approval services, the transformation compiler, and artifact codecs.
- **Important object:** `WorkflowPlanningOrchestrator`.
- **Before reading:** The orchestrator does not implement matching or SQL grammar. It checks workflow/artifact preconditions and delegates domain logic.
- **Interview question:** What is the difference between orchestration and domain logic? Orchestration sequences stateful operations; domain services calculate deterministic results from inputs.

## Where does schema discovery happen?

Discovery crosses several files by design:

- `schemabridge/services/database_service.py` resolves a profile and provides the service boundary.
- `schemabridge/connectors/postgresql/connector.py` reads PostgreSQL catalog metadata.
- `schemabridge/connectors/snowflake/connector.py` reads Snowflake metadata.
- Connector-specific query constants live beside those connectors.
- `schemabridge/normalizers/` converts driver rows into canonical metadata.
- `schemabridge/models/discovery.py` and `metadata.py` define the canonical result.

The workflow orchestrator persists source and target results as separate immutable artifacts. It advances to `DISCOVERED` only when the required discovery evidence exists.

- **Interview question:** Why normalize metadata? Mapping can compare one canonical model rather than carrying vendor-specific result shapes through the application.

## Where is mapping performed?

### `schemabridge/services/schema_mapping.py`

- **Purpose:** Produces deterministic, explainable column suggestions from canonical source and target metadata.
- **Called by:** Lower-level migration routes and the planning orchestrator.
- **Calls:** Mapping domain models; it does not call an AI service or a database.
- **Important object:** `SchemaMappingService` and its candidate-scoring helpers.
- **Before reading:** Matching considers normalized names, token similarity, ordinal proximity, and type/dimension compatibility. A target column is not assigned twice.
- **Interview question:** How does mapping work without AI? It is a rule-based ranking algorithm whose evidence codes and confidence values are reproducible and testable.

### `schemabridge/services/mapping_approval.py`

- **Purpose:** Applies human review decisions and produces an approved plan.
- **Called by:** Both route layers during approval.
- **Calls:** Immutable mapping models.
- **Important object:** `MappingApprovalService`.
- **Before reading:** Low-confidence or incompatible suggestions require explicit treatment; accepted targets remain unique.
- **Interview question:** Why is approval a new artifact instead of a flag? It preserves exactly what was reviewed and keeps the proposal unchanged for auditability.

The shapes involved are defined in `schemabridge/models/mapping.py`: suggestions, evidence, decisions, expressions, approved plans, and generated SQL.

## Where is SQL generated?

### `schemabridge/services/transformation_sql.py`

- **Purpose:** Compiles approved mapping expressions into Snowflake `SELECT` or `INSERT ... SELECT` statements.
- **Called by:** Transformation-preview routes and execution orchestration.
- **Calls:** Mapping models only; it performs no I/O.
- **Important object:** `SnowflakeTransformationSqlCompiler` plus convenience compile functions.
- **Before reading:** Identifiers are quoted, literal values become bound parameters, expression nesting is bounded, and only approved mapped columns are available.
- **Interview question:** Why recompile during execution? The orchestrator proves that the executable statement still matches the approved mapping rather than trusting stored or client-provided SQL.

The source relation for the compiled Snowflake statement is a Snowflake staging relation supplied during preview. It is not the remote PostgreSQL relation.

## Where does database execution happen?

### `schemabridge/services/workflow_execution.py`

- **Purpose:** Coordinates approval-gated, durable migration execution.
- **Called by:** The workflow execution route.
- **Calls:** Repository/persistence services, artifact codecs, the SQL compiler, and `ProfileBoundMigrationExecutionService`.
- **Important object:** `WorkflowExecutionOrchestrator`.
- **Before reading:** It checks current state and latest artifact versions, recompiles SQL, validates equivalence, claims the attempt in the control plane, then crosses the remote database boundary.
- **Interview question:** Why claim before executing? A durable claim prevents concurrent requests from both starting the same remote write.

### `schemabridge/services/migration_execution.py`

- **Purpose:** Enforces the target-profile boundary and sends the generated statement to Snowflake.
- **Called by:** `WorkflowExecutionOrchestrator`.
- **Calls:** `DatabaseService` and the SQL guard.
- **Important objects:** `ProfileBoundMigrationExecutionService`, `PreparedMigrationTarget`, and `TargetExecutionResult`.
- **Before reading:** The target must resolve to Snowflake and have `write_enabled=true`. The client cannot use this service to submit arbitrary SQL through the workflow API.
- **Interview question:** Why treat some failures as uncertain? A network timeout can occur after the database committed, so an automatic retry could duplicate work.

## Where is validation performed?

### `schemabridge/services/validation_sql.py`

- **Purpose:** Generates paired read-only aggregate checks from an approved mapping.
- **Called by:** Validation preview routes and workflow validation.
- **Calls:** Validation and mapping models.
- **Before reading:** The checks compare total row count, per-mapping null counts, and per-mapping distinct counts when compatibility is known; this is not a full row-by-row comparison.
- **Interview question:** Why generate validation SQL rather than accept it from clients? Generated checks keep the validation boundary read-only and tied to the approved plan.

### `schemabridge/services/validation_execution.py`

- **Purpose:** Executes generated source and target checks through separately resolved services.
- **Called by:** Lower-level validation execution and workflow validation.
- **Calls:** Two `DatabaseService` instances.
- **Before reading:** Every validation statement is independently guarded and must return exactly one aggregate row.
- **Interview question:** Why use separate services? Source and target have different vendors, profiles, permissions, and failure domains.

### `schemabridge/services/workflow_validation.py`

- **Purpose:** Adds workflow preconditions, durable claims, artifacts, audit events, and recovery classification around validation.
- **Called by:** The workflow validation route.
- **Calls:** Validation compiler/executor, reconciliation, persistence, and artifact codecs.
- **Important object:** `WorkflowValidationOrchestrator`.
- **Before reading:** Validation begins only after committed execution evidence exists, and a claim prevents concurrent validation runs.
- **Interview question:** Why is validation a separate workflow phase? Execution success says a statement completed; validation asks whether source and target aggregates agree.

## Where is reconciliation performed?

### `schemabridge/services/reconciliation.py`

- **Purpose:** Compares the source and target aggregate metrics and constructs the final migration validation report.
- **Called by:** Validation execution.
- **Calls:** Validation domain models only.
- **Before reading:** It compares generated check IDs and normalized scalar metrics; it never fetches or compares full business rows.
- **Interview question:** What happens on a mismatch? The durable orchestrator records the report and enters `VALIDATION_REVIEW_REQUIRED` rather than declaring the migration valid.

## Where is workflow data persisted?

### `schemabridge/persistence/repository.py`

- **Purpose:** Defines the `WorkflowRepository` protocol used by services.
- **Called by:** Persistence and workflow orchestrators.
- **Implemented by:** `PostgreSQLWorkflowRepository` and test fakes.
- **Before reading:** The protocol keeps orchestration independent of psycopg and makes failure/concurrency scenarios testable.
- **Interview question:** Why use a protocol? It describes the required behavior without coupling callers to a concrete database implementation.

### `schemabridge/persistence/postgresql.py`

- **Purpose:** Implements transactional control-plane storage in PostgreSQL.
- **Called by:** The app-owned persistence service.
- **Calls:** psycopg, canonical serialization, and immutable domain models.
- **Important object:** `PostgreSQLWorkflowRepository`.
- **Before reading:** Transactions combine version checks, state changes, artifact/event insertion, and idempotency results. Row locks serialize competing mutations.
- **Interview question:** How is optimistic concurrency enforced? The caller supplies an expected version and the repository rejects a stale update instead of silently overwriting newer state.

### `schemabridge/services/workflow_persistence.py`

- **Purpose:** Applies legal transition rules and turns repository primitives into workflow operations.
- **Called by:** Planning, execution, validation, and direct persistence routes.
- **Calls:** `WorkflowRepository`.
- **Before reading:** This is where required artifacts and operation-specific state rules are enforced before persistence.

## Where are artifacts stored?

Artifacts are rows in control-plane PostgreSQL, created by the repository using the tables from `schemabridge/persistence/migrations/*.sql`. Payloads are canonical JSON plus a content hash; business rows are not stored there.

`schemabridge/persistence/artifact_codec.py` reconstructs typed domain objects from persisted payloads. `serialization.py` produces stable JSON, content hashes, and request hashes. The codec validates type and shape so an artifact cannot be silently interpreted as the wrong kind of evidence.

## Where are connections created?

### `schemabridge/services/database_service.py`

- **Purpose:** Gives application code a bounded, profile-specific discovery/query/write interface.
- **Called by:** Discovery, execution, and validation services.
- **Calls:** `ProfileRegistry`, `ConnectorFactory`, connector methods, SQL guardrails, and result sanitization.
- **Important objects:** `DatabaseService`, `DatabaseExecutionResult`, `get_database_service`, and cache reset.
- **Before reading:** Caller limits are clamped to profile limits; write operations fail unless enabled; connector cleanup happens even on driver failure. Cached services are keyed by profile and can be reset for tests or shutdown.
- **Interview question:** What value does this layer add over calling a driver directly? It centralizes profile resolution, safe limits, connector lifecycle, redaction, guardrails, and a stable result contract.

### `schemabridge/connectors/factory.py`

- **Purpose:** Creates the configured connector without importing every optional driver eagerly.
- **Called by:** `DatabaseService` and tests.
- **Calls:** Demo, MySQL, PostgreSQL, Snowflake, or SQL Server connector constructors according to the profile.
- **Interview question:** Why lazy imports? A PostgreSQL-only process should not fail because an unused optional database driver is unavailable.

Concrete connectors own driver-specific connection, discovery, execution, rollback, and cleanup behavior. `schemabridge/connectors/base.py` defines their common boundary. The durable migration execution path specifically requires a Snowflake target even though the generic factory contains additional connectors.

## Where are credentials and profile settings loaded?

### `schemabridge/services/profile_registry.py`

- **Purpose:** Parses `DB_PROFILES_JSON` into immutable `ConnectionProfile` objects and resolves profile IDs.
- **Called by:** `get_database_service` and dependency factories.
- **Calls:** `schemabridge/models/connection_profile.py` validation.
- **Before reading:** Workflows persist profile identifiers, not secrets. Unknown profiles fail closed. `write_enabled` defaults to false.
- **Interview question:** Why persist profile IDs? Credentials can rotate outside workflow history and are not leaked into artifacts or audit events.

`schemabridge/config.py` supports the legacy single-profile connector configuration used by generic connector tests and smoke tooling. Durable workflow operations use named profiles.

## Where is SQL safety enforced?

### `schemabridge/validation/sql_guard.py`

- **Purpose:** Rejects empty, multi-statement, comment-obscured, or disallowed SQL before connector execution.
- **Called by:** `DatabaseService`, migration execution, and validation execution.
- **Important functions:** `normalize_query` and `validate_query`.
- **Before reading:** This is a defense-in-depth lexical guard, not a full SQL parser. The stronger workflow boundary is that migration and validation SQL are generated from approved domain models.
- **Interview question:** Is lexical validation sufficient by itself? No. SchemaBridge combines generated SQL, bound parameters, profile permissions, write authorization, artifact checks, and the guard.

## Where are migrations and Docker configuration?

- `schemabridge/persistence/migrations/0001_workflow_audit.sql` creates workflows, artifacts, audit events, and idempotency storage.
- `0002_workflow_execution.sql` adds execution attempts and constraints.
- `0003_workflow_validation.sql` adds validation runs and validation-related state support.
- `schemabridge/persistence/migrations.py` discovers files, verifies checksums, obtains an advisory lock, and applies pending migrations.
- `scripts/migrate_control_plane.py` is the command-line entry point.
- `Dockerfile` packages the FastAPI process using the pinned API dependency lock.
- `compose.yaml` runs the control-plane PostgreSQL, one-shot migration job, and API service.
- `scripts/setup.ps1` prepares a Windows environment; `scripts/verify.ps1` runs the credential-free verification set.

An interview may ask why application startup does not run migrations. Explicit migrations make schema changes observable, checksum-verified, and independently retryable; Compose orders the one-shot migration job before the API.

## How to study the behavior efficiently

1. Run `python -m scripts.demo_workflow` and inspect the created workflow.
2. Read `tests/test_workflow_end_to_end.py` as the executable happy path.
3. Read one failure suite at a time: orchestration, execution, validation, persistence.
4. Trace one request from its route to an orchestrator, then to the repository and connector boundary.
5. Be able to explain which guarantees are local and which depend on a remote database outcome.

The most defensible interview summary is: SchemaBridge is a deterministic, approval-gated migration control plane. It makes planning, generated SQL, remote execution attempts, aggregate validation, and audit history explicit. It currently does not implement the data-transfer step that loads PostgreSQL rows into Snowflake staging.
