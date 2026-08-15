# SchemaBridge setup

This guide starts from a clean checkout. Commands are shown from the repository root.

## What can run without credentials?

| Operation | Live database credentials required? |
|---|---|
| Full unit/API/workflow test suite | No |
| Migration discovery with `--check` | No |
| FastAPI import and OpenAPI generation | No |
| Demo CLI `--help` | No |
| Local workflow demo against a running control plane | No source or Snowflake credentials; a control-plane PostgreSQL instance is required |
| PostgreSQL/Snowflake schema discovery | Yes, for the selected profiles |
| Real Snowflake execution and validation | Yes; target also requires `write_enabled=true` |
| Control-plane migration application | Yes, through `SCHEMABRIDGE_CONTROL_PLANE_DSN` |

## Prerequisites

- Git
- Python 3.12 or newer
- PowerShell on Windows
- Docker Desktop or Docker Engine with Compose v2 only if using the containerized control plane/API

The Docker image uses Python 3.12.11. The repository's existing verification environment may use any supported Python 3.12+ interpreter.

## Clone

```powershell
git clone <repository-url>
cd schemabridge
```

Replace `<repository-url>` with the actual Git remote.

## Windows setup

List installed Python interpreters and confirm that at least Python 3.12 is available:

```powershell
py -0p
```

The repository setup script creates or reuses `.venv`, upgrades pip to the configured minimum, and installs `requirements-dev.txt`:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
```

Manual equivalent:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install "pip>=26.1.2"
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

If only a newer supported interpreter such as 3.13 is installed, replace `-3.12` with that version.

## POSIX setup

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install "pip>=26.1.2"
python -m pip install -r requirements-dev.txt
```

`requirements.txt` contains application runtime dependencies. `requirements-dev.txt` includes it and adds test dependencies. `requirements-api.lock` is the pinned runtime set copied into the Docker image.

## Create local configuration

Never edit or commit the example with real credentials. Copy it to the ignored `.env` file:

```powershell
Copy-Item .\.env.example .\.env
```

POSIX:

```bash
cp .env.example .env
```

## Environment variables

### Control-plane and Compose

| Variable | Purpose |
|---|---|
| `SCHEMABRIDGE_CONTROL_PLANE_DSN` | PostgreSQL DSN used by workflow persistence and the migration command. |
| `SCHEMABRIDGE_CONTROL_PLANE_DB` | Compose database name. |
| `SCHEMABRIDGE_CONTROL_PLANE_USER` | Compose database user. |
| `SCHEMABRIDGE_CONTROL_PLANE_PASSWORD` | Compose-only local password. Change it outside demonstration use. |
| `SCHEMABRIDGE_CONTROL_PLANE_PORT` | Host port mapped to PostgreSQL, default `55432`. |
| `SCHEMABRIDGE_API_PORT` | Host port mapped to the API, default `8000`. |

### Connector defaults

`DB_TYPE`, `DB_HOST`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`, and `DB_CONNECTION_OPTIONS` configure the generic/default connector path used by connector tooling and smoke tests. The named `demo-local` profile is configured separately inside `DB_PROFILES_JSON`.

`DB_TIMEOUT_SECONDS` and `DB_MAX_ROWS` define limits for that generic/default path. Each named profile has its own timeout and row limit, and a request cannot exceed the selected profile's values.

`LOG_LEVEL` selects the Python logging level.

### Named profiles

`DB_PROFILES_JSON` is a JSON object keyed by profile ID. Durable workflows persist these IDs and resolve the full profile only at runtime.

The checked-in example defines three shapes:

- `demo-local`: no network or credentials;
- `postgres-source`: PostgreSQL account, database, port/TLS options, timeout, and row limit;
- `snowflake-target`: Snowflake account identifier, database, warehouse, schema, role, credentials, timeout, row limit, and `write_enabled`.

Replace every angle-bracket placeholder before using a live profile. Leave the target's `write_enabled` false until a reviewer explicitly authorizes a non-production target for writes.

Profile-controlled host, database, credentials, timeout, row limit, and write authorization cannot be overridden through request-supplied connection options.

### Optional integration-test variables

The normal suite does not use live databases. The following variables enable explicitly opted-in checks:

| Variable | Purpose |
|---|---|
| `SCHEMABRIDGE_RUN_CONTROL_PLANE_INTEGRATION` | Set to `1` to enable the disposable live control-plane contract. |
| `SCHEMABRIDGE_CONTROL_PLANE_TEST_DSN` | DSN for that contract; its database name must clearly contain `test`. |
| `SCHEMABRIDGE_POSTGRES_INTEGRATION` | Set to `1` to enable live PostgreSQL discovery coverage. |
| `SCHEMABRIDGE_POSTGRES_HOST`, `SCHEMABRIDGE_POSTGRES_PORT`, `SCHEMABRIDGE_POSTGRES_DATABASE`, `SCHEMABRIDGE_POSTGRES_USERNAME`, `SCHEMABRIDGE_POSTGRES_PASSWORD` | Connection values for the opt-in PostgreSQL discovery check. |
| `DB_SMOKE_TEST_CONNECT` | Set to `true` only when the generic connector smoke test should make a live connection. |

Keep all flags disabled for credential-free verification.

## Run credential-free tests

Windows:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Focused durable workflow:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_workflow_end_to_end.py -q
```

Complete scripted quality gate:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

POSIX:

```bash
.venv/bin/python -m pytest -q
```

The normal suite uses fakes at remote boundaries. Two optional integration contracts are skipped unless explicitly enabled.

## Inspect control-plane migrations without a database

```powershell
.\.venv\Scripts\python.exe -m scripts.migrate_control_plane --check
```

Expected discovered versions are `1, 2, 3, 4, 5`, corresponding to:

- `0001_workflow_audit.sql`
- `0002_workflow_execution.sql`
- `0003_workflow_validation.sql`
- `0004_workflow_transport.sql`
- `0005_staging_cleanup.sql`

## Configure the control-plane PostgreSQL database

### Option A: start only PostgreSQL with Compose

```powershell
docker compose up -d control-plane
```

The default host DSN in `.env.example` uses port `55432`. Apply migrations explicitly:

```powershell
.\.venv\Scripts\python.exe -m scripts.migrate_control_plane
```

This command requires a reachable `SCHEMABRIDGE_CONTROL_PLANE_DSN`. Applied migration checksums are recorded, and modifying an already-applied migration causes a failure.

### Option B: use an existing PostgreSQL database

Create a dedicated database and least-privilege application account, set `SCHEMABRIDGE_CONTROL_PLANE_DSN`, and run the same migration command. Do not point the control plane at the source PostgreSQL database merely because both use PostgreSQL; they have different responsibilities and permissions.

## Start FastAPI on the host

After the control plane is reachable and migrated:

```powershell
.\.venv\Scripts\python.exe -m uvicorn schemabridge.api.app:create_app --factory --env-file .env --host 127.0.0.1 --port 8000
```

POSIX:

```bash
.venv/bin/python -m uvicorn schemabridge.api.app:create_app --factory --env-file .env --host 127.0.0.1 --port 8000
```

Open:

- Swagger UI: <http://localhost:8000/docs>
- OpenAPI JSON: <http://localhost:8000/openapi.json>
- Liveness: <http://localhost:8000/health/live>
- Readiness: <http://localhost:8000/health/ready>

The process can be imported and can construct OpenAPI without source or target credentials. Durable workflow endpoints require the control-plane repository, and database operations require their selected profiles.

## Credential-free local demo

With the API and control plane running:

```powershell
.\.venv\Scripts\python.exe -m scripts.demo_workflow
```

This creates and reads a durable `DRAFT` workflow and lists its artifacts and audit events. It does not discover a live schema or execute a migration.

The following command requires valid PostgreSQL and Snowflake profiles because it performs source discovery, target discovery, and mapping proposal generation:

```powershell
.\.venv\Scripts\python.exe -m scripts.demo_workflow --discover
```

It still stops before approval and execution.

## Live PostgreSQL and Snowflake profiles

For source discovery and validation:

1. Fill the `postgres-source` profile placeholders in `.env`.
2. Use a read-only PostgreSQL role with access only to the intended schemas.
3. Restart the API after changing `DB_PROFILES_JSON`.

For target discovery and execution:

1. Fill the `snowflake-target` account, database, warehouse, schema, role, username, and secret.
2. Use a non-production, least-privilege role.
3. Call the durable `load-staging` workflow endpoint after mapping approval; SchemaBridge creates and loads the managed transient Snowflake staging table.
4. Keep `write_enabled=false` during discovery and review.
5. Set `write_enabled=true` only for an intentional execution demonstration, then restart the API.

No live Snowflake migration is claimed by the repository's current verification evidence.

## Docker Compose

Validate configuration without starting containers:

```powershell
docker compose config --quiet
```

Start the complete local stack:

```powershell
docker compose up --build -d
docker compose ps
```

Compose starts:

1. `control-plane`, with a PostgreSQL health check;
2. `migrate`, which applies migrations once;
3. `api`, after migration completion.

The repository statically verifies this configuration. The latest project verification does not claim a successful Docker image build or healthy running stack.

## Shutdown

Stop containers while keeping the PostgreSQL volume:

```powershell
docker compose down
```

Delete the disposable local volume only when you intentionally want to erase its workflows:

```powershell
docker compose down -v
```

## Troubleshooting

### `SCHEMABRIDGE_CONTROL_PLANE_DSN is required`

The migration command was run without a DSN. Copy `.env.example` to `.env`, start the Compose control plane or configure another PostgreSQL instance, and verify the DSN.

### Workflow endpoints return `CONTROL_PLANE_UNAVAILABLE`

The API started without an enabled control-plane configuration. Set the DSN and restart the process. Startup does not apply migrations automatically.

### A profile is not configured

Check that `DB_PROFILES_JSON` is valid JSON and that the workflow profile ID matches a key exactly except for case normalization. Restart the API after changing profiles because the registry is cached.

### Snowflake target is not write capable

The target profile either has `write_enabled=false`, selects the wrong connector, or does not match the workflow target database. Do not bypass the check; correct and review the profile.

### Generated statement is rejected

Execution accepts only a compiler-produced Snowflake `INSERT_SELECT` tied to the approved mapping artifact. Regenerate the preview from the current workflow rather than submitting or editing SQL.

### Execution or validation is recovery-required

The remote result could not be classified safely. Do not retry automatically. Inspect the target/control-plane state and resolve the outcome manually.

### PostgreSQL or Snowflake driver is missing

Install `requirements-dev.txt` for local development. Connector imports are lazy, but using a selected live connector requires its driver.

### SQL Server fails despite Python dependencies

`pyodbc` is installed by the runtime requirements, but Windows also needs a compatible Microsoft ODBC Driver. SQL Server is a retained generic connector, not the durable Snowflake execution target.

### Docker warnings about user configuration

`docker compose config --quiet` can still validate the project file even when a restricted environment cannot read a user-level Docker configuration. Resolve local Docker permissions before attempting a build or runtime claim.
