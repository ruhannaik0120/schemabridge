# Local workflow demo

This guide separates a safe local control-plane demonstration from a real PostgreSQL-to-Snowflake migration. The local demo never claims that data moved.

## 1. Local API and control plane only

Start the stack and open Swagger:

```powershell
Copy-Item .\.env.example .\.env
docker compose up --build -d
Start-Process http://localhost:8000/docs
```

Run the repeatable inspection script:

```powershell
.\.venv\Scripts\python.exe .\clean_mcp\scripts\demo_workflow.py
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
.\.venv\Scripts\python.exe .\clean_mcp\scripts\demo_workflow.py --discover
```

This calls `discover-source`, `discover-target`, and `mapping-proposals`, then stops for human review. Inspect the `MAPPING_PLAN` artifact in Swagger.

## 3. Approval, preview, execution, and validation

Continue in Swagger so every version and artifact reference comes from the preceding persisted response:

| Step | Endpoint | Required references |
|---|---|---|
| Approve mapping | `POST /api/v1/migrations/workflows/{id}/mapping-approvals` | current version, mapping artifact version, per-column review decisions |
| Compile preview | `POST /api/v1/migrations/workflows/{id}/transformation-previews` | current version, approved mapping artifact version, staging relation, `INSERT_SELECT` |
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
cd clean_mcp
..\.venv\Scripts\python.exe -m pytest tests\test_workflow_end_to_end.py -q
```

The fakes exist only at dependency boundaries in the test application. Production routes contain no demo execution behavior.
