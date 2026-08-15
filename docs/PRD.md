# SchemaBridge product requirements

## 1. Product overview

SchemaBridge is a governed backend for planning, approving, executing, and validating PostgreSQL/MySQL-to-Snowflake migration workflows. It exposes FastAPI endpoints, stores durable workflow evidence in a separate PostgreSQL control plane, resolves database credentials through named runtime profiles, and delegates database operations to connector implementations.

The current product is deterministic Python code. Mapping, SQL generation, execution checks, and validation do not use AI, an MCP server, or an external ticketing system.

## 2. Problem being solved

Ad hoc migration scripts often combine credentials, metadata discovery, mapping decisions, SQL construction, execution, and validation in one process. That makes it difficult to review what was approved, prevent duplicate writes, distinguish a safe retry from an uncertain result, or explain how a result was produced.

SchemaBridge separates those responsibilities into durable steps. Each important decision is represented by a typed artifact, each mutation carries an idempotency key, and post-creation commands use an expected workflow version.

## 3. Goals

- Discover PostgreSQL or MySQL source metadata and Snowflake target metadata through named profiles.
- Convert vendor metadata into canonical immutable models.
- Produce deterministic and explainable one-to-one mapping suggestions.
- Require explicit human approval before write-capable execution.
- Compile parameterized Snowflake SQL from approved mappings.
- Persist workflow state, artifacts, execution attempts, validation runs, and audit events.
- Prevent stale commands, unsafe generated SQL, duplicate remote work, and silent retries after uncertain outcomes.
- Generate and reconcile paired PostgreSQL and Snowflake validation queries.
- Keep credentials and raw driver errors out of workflow artifacts and public API errors.

## 4. Non-goals

The current repository does not provide:

- a frontend, authentication system, background worker, or hosted production deployment;
- file ingestion, change-data capture, or streaming transport;
- Jira, MCP, Excel, HTML-reporting, AWS, or PySpark integration;
- AI-based mapping, fuzzy profiling, or automatic resolution of ambiguous mappings;
- full row-by-row data comparison;
- automatic recovery from an uncertain remote database outcome.

## 5. Current supported scope

The durable workflow is designed around:

- **source system:** PostgreSQL or MySQL, for schema discovery, bounded extraction, and source-side validation;
- **target system:** Snowflake, for target discovery, approved transformation execution, and target-side validation;
- **control plane:** a separate PostgreSQL database that stores workflow records.

The connector factory also contains demo and SQL Server implementations. SQL Server remains a reusable lower-level connector, while MySQL now implements the discovery, batch-reader, and validation capabilities required on the durable source side. The durable execution path still explicitly requires a Snowflake target.

After approval, SchemaBridge creates a managed transient Snowflake staging table and copies PostgreSQL or MySQL rows into it in bounded batches. Transformation execution produces a Snowflake `INSERT ... SELECT` whose source is derived from the persisted staging-load evidence rather than supplied by the client.

## 6. Source and target systems

Workflows persist profile identifiers, not credentials. At runtime, `ProfileRegistry` parses `DB_PROFILES_JSON`, and `DatabaseService` resolves the selected immutable profile through `ConnectorFactory`.

The PostgreSQL or MySQL source profile may be read-only. The Snowflake target profile must match the workflow target database and must set `write_enabled=true` before migration execution. Validation remains read-only and selects generated SQL from connector-advertised dialect capabilities.

## 7. End-to-end workflow

1. Create a durable workflow in `DRAFT`.
2. Discover the source PostgreSQL or MySQL relation.
3. Discover the target Snowflake relation.
4. Persist both discovery artifacts and enter `DISCOVERED`.
5. Generate a deterministic mapping proposal.
6. Record explicit reviewer decisions and persist the approved mapping.
7. Claim a durable transport attempt, create managed Snowflake staging, and copy source rows in bounded batches.
8. Persist row-free staging-load evidence and enter `STAGED`.
9. Compile and persist a Snowflake transformation preview from that staging evidence.
10. Execute an approved `INSERT ... SELECT` through the exact target profile.
11. Persist execution evidence and classify the remote outcome.
12. After a confirmed commit, remove the exact managed staging table and persist cleanup evidence.
13. Compile paired PostgreSQL and Snowflake validation queries.
14. Execute both read-only queries and reconcile their aggregate metrics.
15. Persist the validation report and finish as validated, review-required, or recovery-required.

## 8. Workflow states

The implemented states are:

- planning: `DRAFT`, `DISCOVERED`, `MAPPING_PROPOSED`, `MAPPING_APPROVED`;
- transport: `STAGING`, `STAGED`, `STAGING_RECOVERY_REQUIRED`;
- execution: `EXECUTION_READY`, `EXECUTING`, `EXECUTED`, `EXECUTION_RECOVERY_REQUIRED`;
- validation: `VALIDATION_READY`, `VALIDATING`, `VALIDATED`, `VALIDATION_REVIEW_REQUIRED`, `VALIDATION_RECOVERY_REQUIRED`;
- administrative terminal states: `FAILED`, `CANCELLED`.

Transition rules are defined by `ALLOWED_TRANSITIONS` in `schemabridge/models/workflow.py`. Execution and validation states are controlled by their orchestrators rather than the generic transition endpoint.

`VALIDATION_READY` is present in the transition graph, but the current durable validation endpoint claims a run directly from `EXECUTED` and transitions to `VALIDATING`.

## 9. Artifact types

The workflow recognizes ten artifact types:

1. `SOURCE_DISCOVERY`
2. `TARGET_DISCOVERY`
3. `MAPPING_PLAN`
4. `APPROVED_MAPPING_PLAN`
5. `STAGING_LOAD_EVIDENCE`
6. `STAGING_CLEANUP_EVIDENCE`
7. `TRANSFORMATION_PREVIEW`
8. `EXECUTION_EVIDENCE`
9. `VALIDATION_PREVIEW`
10. `VALIDATION_EXECUTION_REPORT`

Artifacts use canonical JSON bytes, a schema version, a monotonically increasing artifact version, and a SHA-256 digest. Domain objects are rehydrated through `schemabridge/persistence/artifact_codec.py`.

## 10. Schema discovery

`DatabaseService.get_table_metadata` calls the connector bound to a named profile. PostgreSQL and Snowflake discovery connectors run fixed catalog queries and normalize the results into canonical schemas, objects, columns, constraints, and coverage information.

Discovery errors are translated into stable application errors. Driver-controlled text, credentials, and connection details are not returned through the public workflow API.

## 11. Mapping generation

`SchemaMappingService` compares canonical source and target columns. It uses normalized name tokens, ordinal position, canonical type compatibility, nullability, and size/precision information to produce deterministic evidence and confidence values.

Suggestions are one-to-one and can be `SUGGESTED`, `AMBIGUOUS`, or `UNMATCHED`. The service does not execute SQL and does not automatically approve a suggestion.

## 12. Human approval

`MappingApprovalService` converts reviewer decisions into an immutable approved plan. Low-confidence or incompatible selections require an explicit override and reason. Approved mappings cannot reuse a target column, and transformation expressions may reference only known source columns.

Pending decisions prevent workflow execution. The approval artifact is the durable boundary between a generated suggestion and an authorized transformation.

## 13. Transformation compilation

`SnowflakeTransformationSqlCompiler` renders either a read-only `SELECT` preview or an `INSERT_SELECT` statement. Identifiers are quoted, literal values become bound parameters, recursion depth is limited, and only modeled transformation expression types are supported.

The execution path accepts no client-provided SQL. It rehydrates the approved mapping, recompiles the expected statement, and requires exact equality with the persisted preview before any target call.

## 14. Execution

`WorkflowExecutionOrchestrator` verifies workflow state, current artifact versions, mapping approval, target profile identity, and the generated statement. `ProfileBoundMigrationExecutionService` then requires:

- a Snowflake workflow target;
- the exact configured target database;
- a resolved target profile with `write_enabled=true`;
- a compiler-produced Snowflake `INSERT_SELECT`;
- SQL accepted by the structural SQL guard.

Before the remote call, the orchestrator creates a durable execution claim. Concurrent callers cannot independently acquire the same attempt. Completion stores sanitized evidence rather than raw connector output.

After a confirmed commit, the orchestrator removes only the exact managed `SB_STAGE_<UUID>` relation from staging-load evidence and appends `STAGING_CLEANUP_EVIDENCE`. A failed cleanup can be retried through an exact execution replay without repeating the committed insert. Rolled-back or uncertain execution never triggers automatic cleanup.

## 15. Validation

Validation is a separate post-execution operation. `compile_validation_sql` generates one PostgreSQL query and one Snowflake query containing:

- total row count;
- per-mapping null counts;
- per-mapping distinct counts when compatibility is known.

The queries are generated from the approved mapping, parameterized where needed, and must remain read-only. `MigrationValidationExecutionService` executes each side through its named profile and requires exactly one aggregate result row from each query.

## 16. Reconciliation

`reconcile_validation_results` converts database metric values to non-negative integer counts and compares matching aliases. Each check becomes `MATCH`, `MISMATCH`, or `UNAVAILABLE`.

The report records the exact version from the persisted approved mapping. The durable orchestrator rejects a validation result whose reported plan version does not match that approved artifact, so mismatched lineage is quarantined rather than persisted as validation evidence.

The overall report is:

- `PASSED` when every available metric matches;
- `FAILED` when at least one metric differs;
- `INCOMPLETE` when a metric cannot be compared and no mismatch exists.

A mismatch leads to `VALIDATION_REVIEW_REQUIRED`; it is not treated as proof that execution itself failed.

## 17. Control-plane persistence

`PostgreSQLWorkflowRepository` implements the `WorkflowRepository` contract with PostgreSQL transactions. The control plane stores:

- workflows and optimistic versions;
- immutable artifacts and hashes;
- idempotency keys and request hashes;
- execution attempts;
- validation runs;
- ordered audit events.

Migrations `0001` through `0005` create and extend this model. Migration `0004` adds durable source-to-staging transport claims, and migration `0005` adds cleanup evidence support. Migration files are checksum-verified and applied explicitly by `scripts.migrate_control_plane`; application startup does not run migrations.

## 18. Idempotency

Every workflow mutation carries an `Idempotency-Key`. SchemaBridge hashes the operation name and canonical request inputs. Repeating the same key and request returns the recorded result; reusing the key for different inputs raises a conflict.

Transport, execution, and validation additionally persist command hashes and remote-operation claims so a reconstructed service can return completed evidence without calling the remote database again.

## 19. Optimistic concurrency

After creation, mutating commands include `expected_version`. The repository compares that value with the durable workflow version inside the transaction. This prevents a stale client from overwriting a newer decision while still allowing an exact idempotent replay.

## 20. Audit history

Audit events are append-only and sequence-numbered per workflow. Events record the event type, previous and new status, workflow version, actor classification, request ID, idempotency key, optional artifact reference, timestamp, and a constrained reason code.

Audit metadata deliberately excludes credentials, SQL text, query parameters, and raw driver errors.

## 21. Recovery and uncertain outcomes

Remote database execution cannot share an atomic transaction with the control plane. A process can lose the response after the target has committed. SchemaBridge therefore distinguishes:

- successful execution;
- a confirmed failure with rollback, which the orchestration contract can return to `EXECUTION_READY` (the current production execution adapter conservatively classifies caught target exceptions as uncertain rather than proving rollback);
- an uncertain outcome, which enters `EXECUTION_RECOVERY_REQUIRED`.

Validation uses the same principle and enters `VALIDATION_RECOVERY_REQUIRED` when its remote result cannot be classified safely. Recovery-required workflows are not automatically retried.

## 22. Security and safety decisions

- Credentials remain in runtime configuration and are not persisted in workflow artifacts.
- Public errors replace driver messages with stable sanitized errors.
- Profile-controlled database, timeout, row-limit, and credential fields cannot be overridden by a request.
- Requested timeouts and row limits are clamped to the selected profile.
- Target writes require both an approved artifact and `write_enabled=true`.
- SQL is generated internally, structurally checked, and limited to one supported statement.
- Validation queries must be `SELECT` statements.
- Connector resources are operation-scoped or explicitly closed.
- Docker runs the API as a non-root user.

## 23. Known limitations

- Live verification has covered one five-row PostgreSQL-to-Snowflake workflow with automatic managed staging, committed execution, aggregate validation, exact replay, and post-commit staging cleanup; production scale and failure recovery have not been live-tested.
- Batch transport is synchronous and is not optimized as a bulk-file or streaming engine.
- Validation compares aggregates rather than every row.
- Production execution currently supports only Snowflake as the target.
- There is no authentication or authorization layer around the HTTP API.
- Operations are synchronous; there is no worker queue.
- Uncertain outcomes require manual investigation.
- Docker Compose is statically validated, but the latest verification does not claim a successful image build or running container health check.

## 24. Future directions

The following are possible future additions, not current functionality:

- authenticated users and role-based approval;
- a background execution worker and operational monitoring;
- bulk-file transport and asynchronous workers for larger datasets;
- documented manual recovery workflows and operator tooling;
- additional durable source/target connector support;
- stronger deployment hardening and live environment evidence;
- optional richer validation strategies for selected datasets.
