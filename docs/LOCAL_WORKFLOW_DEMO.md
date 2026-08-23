# Local workflow demo

This guide separates a safe local control-plane demonstration from a real data-plane migration. The local demo never claims that data moved.

## Disposable Snowflake-to-PostgreSQL transport proof

The following script proves the connector-neutral batch boundary rather than
the durable workflow policy. It creates a three-row Snowflake source table,
loads it into a PostgreSQL staging table in two batches, verifies the row
count, and removes both disposable tables in `finally` cleanup.

Use a write-enabled PostgreSQL target profile and explicitly confirm remote
writes:

```powershell
.\.venv\Scripts\python.exe -m scripts.live_snowflake_to_postgresql_transport `
  --confirm-live-write `
  --snowflake-profile <snowflake-profile-id> `
  --postgresql-profile <write-enabled-postgresql-profile-id>
```

If a local PostgreSQL lab is exposed on a port different from the profile,
pass `--postgresql-port <port>` for this one proof. The override is in memory
only; it never changes `.env` or the stored profile document.

## Disposable PostgreSQL-to-MySQL transport proof

This equivalent proof creates three PostgreSQL source rows, loads them into a
MySQL staging table in two batches, checks the target row count, and removes
both disposable tables in `finally` cleanup:

```powershell
.\.venv\Scripts\python.exe -m scripts.live_postgresql_to_mysql_transport `
  --confirm-live-write `
  --postgresql-profile <postgresql-profile-id> `
  --mysql-profile <mysql-profile-id>
```

Use `--postgresql-port` and `--mysql-port` only when a local lab uses ports
different from its configured profiles. The overrides are in-memory only.

## Disposable FastAPI PostgreSQL-to-MySQL workflow proof

With a configured control-plane DSN, this runs discovery, mapping approval,
staging, target execution, validation, and staging cleanup through the FastAPI
application. It removes its uniquely named data-plane source and target tables
afterward; the control-plane workflow evidence remains for audit inspection.

```powershell
.\.venv\Scripts\python.exe -m scripts.live_postgresql_to_mysql_workflow `
  --confirm-live-write `
  --postgresql-profile <postgresql-profile-id> `
  --mysql-profile <write-enabled-mysql-profile-id>
```

## 1. Local API and control plane only

Start the stack and open Swagger:

```powershell
Copy-Item .\.env.example .\.env
docker compose up --build -d
Start-Process http://localhost:8000/docs
```

Run the repeatable inspection script:

```powershell
.\.venv\Scripts\python.exe -m scripts.demo_workflow
```

It calls the health endpoint, creates a durable `DRAFT` workflow, retrieves its version and state, and lists artifacts and audit history. The fixed idempotency key makes an exact rerun return the same workflow. It does not discover a real schema or execute a migration.

Equivalent first request:

```bash
curl -X POST http://localhost:8000/api/v1/migrations/workflows \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-create" \
  -d '{
    "display_name":"Placement demo workflow",
    "source_profile_id":"postgres-source",
    "target_profile_id":"snowflake-target",
    "source_relation":{"catalog_name":null,"schema_name":"public","object_name":"source_people","system":"postgresql"},
    "target_relation":{"catalog_name":"ANALYTICS","schema_name":"PUBLIC","object_name":"PEOPLE","system":"snowflake"},
    "actor_type":"USER","actor_reference":"local-demo"
  }'
```

Use the returned `workflow_id`:

```bash
curl http://localhost:8000/api/v1/migrations/workflows/<workflow_id>
curl http://localhost:8000/api/v1/migrations/workflows/<workflow_id>/artifacts
curl http://localhost:8000/api/v1/migrations/workflows/<workflow_id>/audit-events
```

## 2. Real-profile discovery and planning

Configure a read-only PostgreSQL source and a Snowflake target in the ignored `.env`. Leave Snowflake `write_enabled=false` while demonstrating discovery and planning.

```powershell
.\.venv\Scripts\python.exe -m scripts.demo_workflow --discover
```

This calls `discover-source`, `discover-target`, and `mapping-proposals`, then stops for human review. Inspect the `MAPPING_PLAN` artifact in Swagger.

## 3. Approval, staging load, preview, execution, and validation

Continue in Swagger so every version and artifact reference comes from the preceding persisted response:

After approval, call `load-staging`. SchemaBridge creates a uniquely named transient Snowflake table, reads PostgreSQL in bounded batches, loads that table, and returns row-free evidence containing its relation and counts.

| Step | Endpoint | Required references |
|---|---|---|
| Approve mapping | `POST /api/v1/migrations/workflows/{id}/mapping-approvals` | current version, mapping artifact version, per-column review decisions |
| Load staging | `POST /api/v1/migrations/workflows/{id}/load-staging` | current version, source discovery and approved mapping artifact versions, exact source and target profiles |
| Compile preview | `POST /api/v1/migrations/workflows/{id}/transformation-previews` | current version, approved mapping artifact version, `INSERT_SELECT`; staging is derived automatically |
| Execute | `POST /api/v1/migrations/workflows/{id}/execute` | current version, approved mapping and transformation artifact versions, exact Snowflake profile |
| Validate | `POST /api/v1/migrations/workflows/{id}/validate` | current version, execution evidence and approved mapping artifact versions, exact source and target profiles |

Every mutation needs a unique `Idempotency-Key`. Repeating the same body with the same key is an exact replay; changing the body while reusing the key returns a conflict.

Before the execution step, a reviewer must verify the generated SQL and intentionally set `write_enabled=true` for the least-privilege Snowflake target profile. Restart the API after changing injected profile configuration.

After completion, show:

```bash
curl http://localhost:8000/api/v1/migrations/workflows/<workflow_id>
curl http://localhost:8000/api/v1/migrations/workflows/<workflow_id>/artifacts?limit=50
curl http://localhost:8000/api/v1/migrations/workflows/<workflow_id>/audit-events?limit=50
```

A successful real run should end at `VALIDATED`. A data mismatch ends at `VALIDATION_REVIEW_REQUIRED`; an uncertain connector outcome is quarantined and must not be retried automatically.

## Automated credential-free equivalent

The complete orchestration sequence is exercised without production credentials by:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_workflow_end_to_end.py -q
```

The fakes exist only at dependency boundaries in the test application. Production routes contain no demo execution behavior.
