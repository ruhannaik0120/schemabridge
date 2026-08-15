# SchemaBridge interview demo

## Prepare beforehand

```powershell
PowerShell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
Copy-Item .\.env.example .\.env
docker compose up --build -d
docker compose ps
.\.venv\Scripts\python.exe -m pytest tests\test_workflow_end_to_end.py -q
```

Keep <http://localhost:8000/docs> open. Do not configure production credentials for the credential-free interview path.

## Five-to-seven minute sequence

### Minute 0–1: problem and architecture

Explain that migration scripts commonly combine credentials, mapping judgment, SQL, execution, and verification. Show the README architecture diagram: FastAPI and orchestration, a separate PostgreSQL control plane, and profile-bound source/target services.

### Minute 1–2: create and inspect durable state

Run:

```powershell
.\.venv\Scripts\python.exe -m scripts.demo_workflow
```

Show the returned workflow ID, `DRAFT` state, version, empty artifact history, and append-only creation audit event. State clearly that this is a control-plane demonstration, not a real migration.

### Minute 2–4: governed workflow contract

In Swagger, show these operations in order:

1. source and target discovery;
2. mapping proposal and human approval;
3. transformation preview;
4. approval-gated execution;
5. validation and reconciliation;
6. artifact and audit queries.

Point out that execution accepts artifact versions and a profile ID—not SQL or credentials. The target profile must independently opt into writes.

### Minute 4–5: evidence and replay

Open `tests/test_workflow_end_to_end.py`. Explain the eight ordered artifacts, 17 ordered audit events, service reconstruction, and the assertions that exact replay leaves execution and validation invocation counts at one.

### Minute 5–6: failures and recovery

Describe three outcomes:

- the orchestration contract can return a proven rollback to `EXECUTION_READY`, while the current production adapter conservatively treats caught target exceptions as uncertain;
- data mismatch becomes `VALIDATION_REVIEW_REQUIRED` without calling it an execution failure;
- uncertain execution or validation becomes a recovery-required state and is never automatically rerun.

### Minute 6–7: limitations

State the evidence precisely: a live five-row PostgreSQL-to-Snowflake workflow passed using automatic managed staging. SchemaBridge created the transient table, loaded three batches, committed five target inserts, passed all 13 aggregate checks, and exact replays left the target at five rows. This proves the small controlled path, not production scale. Transport is synchronous and batch-based, not a bulk-file or streaming engine. Validation is aggregate-based. Authentication, frontend, background workers, AWS/PySpark, advanced profiling, and automatic uncertain-outcome resolution are not implemented.

## Architecture talking points

- Domain models and compilers are deterministic and immutable.
- FastAPI routes are transport adapters; orchestration remains in services.
- Remote database work cannot share a transaction with the control plane, so durable claims model that boundary honestly.
- PostgreSQL transactions atomically store artifacts, workflow versions, idempotency outcomes, and audit events.
- Profile IDs are persisted; secrets and raw driver errors are not.

## Likely questions

**Why PostgreSQL for the control plane?**  
It provides transactions, row locking, advisory locking, JSONB artifacts, constraints, and reliable optimistic concurrency for a small operational data model.

**How do you prevent duplicate migrations?**  
An idempotency key plus request hash identifies exact replays, a durable execution claim is written before the remote call, and concurrent active attempts are rejected.

**Why not retry every failure?**  
A lost response can leave the target outcome unknown. Automatic retry could duplicate a migration, so uncertain outcomes are quarantined.

**Can a client execute arbitrary SQL?**  
Not through the durable workflow. SchemaBridge recompiles and verifies the persisted approved plan, and the execution request contains artifact references rather than SQL.

**How is validation performed?**  
SchemaBridge generates paired, parameterized PostgreSQL and Snowflake aggregate queries and reconciles row, null, and distinct counts using the persisted approved mapping.

**Is it production-ready?**  
No production-readiness claim is made. The core safety model and local packaging are implemented, but real-environment security, operations, authentication, monitoring, and Snowflake smoke evidence remain deployment work.
