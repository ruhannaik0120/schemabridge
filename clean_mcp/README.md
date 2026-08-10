# SchemaBridge backend and database connector layer

This directory contains the durable SchemaBridge FastAPI workflow and its lower-level profile, database-service, and connector infrastructure. Start with the [repository overview](../README.md) for the PostgreSQL-to-Snowflake workflow, Docker Compose, migrations, Swagger, tests, and interview demo.

FastAPI factory: `clean_mcp.api.app:create_app`

Swagger after startup: <http://localhost:8000/docs>

Control-plane migration command: `python clean_mcp/scripts/migrate_control_plane.py`

SchemaBridge resolves named profiles directly in Python services. PostgreSQL and Snowflake support the current migration workflow; SQL Server, MySQL, and the offline demo connector remain reusable connector implementations.

## Where To Start

Read the folder in this order:

1. `api/` - FastAPI application, routes, schemas, and dependency wiring.
2. `services/` - workflow orchestration and profile-bound database access.
3. `persistence/` - durable workflow repositories and migrations.
4. `connectors/` - shared connector contract and database implementations.
5. `config.py` - validated connector configuration and credential redaction.
6. `models/` and `validation/` - domain contracts and SQL guardrails.
7. `tests/` - behavior, safety, architecture, and smoke verification.

Files such as `.env`, `.pytest-tmp`, `.test-runtime`, and `__pycache__` are local
or generated and are not part of the delivered source tree.

## Framework Scope

`clean_mcp` is the current source directory for SchemaBridge. The FastAPI
workflow calls profile-bound Python services directly; those services resolve
immutable named profiles and delegate operation-scoped work to connectors.

```text
FastAPI workflow endpoint
        |
        v
workflow orchestration and profile-bound DatabaseService
        |
        v
PostgreSQL | Snowflake | retained connector implementations
```

## Capabilities

- Connector factory and stable `DatabaseConnector` extension contract.
- Immutable named-profile resolution by explicit profile ID.
- Approval-gated Snowflake migration execution and generated read-only validation.
- Dialect-aware one-statement checks, bounded returned rows, and connection plus statement timeouts.
- Credential-safe database errors and deterministic connector cleanup.
- Architecture tests that keep vendor drivers inside `connectors/`.

## Configuration

Copy the example and keep real credentials only in the ignored `.env` file:

```powershell
Copy-Item .\clean_mcp\.env.example .\clean_mcp\.env
```

Profiles are configured through `DB_PROFILES_JSON`. Workflows persist profile IDs such as `postgres-source` and `snowflake-target`; credentials remain only in runtime configuration.

The application treats profile configuration as a cacheable startup snapshot.
After changing the local `.env`, restart the API so the profile registry and
database-service cache are rebuilt. Production environments should inject
credentials through their deployment secret manager.

Profile-controlled target, credential, and timeout fields cannot be overridden through `connection_options`. Use `connection_options` only for backend-specific settings such as an ODBC driver, TLS mode, warehouse, role, or schema.

```env
DB_TYPE=demo
DB_DATABASE=schemabridge_demo
DB_TIMEOUT_SECONDS=30
DB_MAX_ROWS=500
DB_PROFILES_JSON={"demo-local":{"db_type":"demo","database":"schemabridge_demo"},"postgres-local":{"db_type":"postgresql","host":"localhost","database":"schemabridge_demo","username":"demo_user","password":"demo_password","connection_options":{"port":5432}}}
```

## Setup And Verification

From the repository root:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\clean_mcp\scripts\setup.ps1
PowerShell -ExecutionPolicy Bypass -File .\clean_mcp\scripts\verify.ps1
```

For SQL Server, the setup script installs `pyodbc`, but Windows must also have the Microsoft ODBC Driver 18 for SQL Server installed. The offline `demo` profile does not require any database driver, credentials, or network access.

## Safety Model

- Workflow execution requires an approved mapping artifact and a write-enabled target profile.
- Validation SQL is compiler-generated and read-only.
- Database operations are bound to each explicitly referenced profile's configured database.
- Use only sandbox/test databases and least-privilege profile credentials.
- Keep cloud/private databases reachable only through approved company network access.
- Database permissions remain the final authority for allowed commands.
- Returned rows cannot exceed `DB_MAX_ROWS`; request timeouts cannot exceed `DB_TIMEOUT_SECONDS`.
- Credentials, tokens, private keys, and connection strings are excluded from public errors and workflow artifacts.
- Controlled execution accepts one validated generated statement per operation.

## Repository Map

```text
api/              FastAPI application and dependency wiring
config.py         Validated, redacted runtime configuration
connectors/       Common contract, factory, and vendor implementations
services/         Workflow orchestration and profile-bound database access
persistence/      Durable repository, artifacts, and migrations
validation/       Single-command structural validation
models/           Domain and database metadata contracts
tests/            Unit, behavior, and architecture tests
docs/             Integration, extension, and testing guides
scripts/          Setup and verification automation
```

See [ADDING_CONNECTORS.md](docs/ADDING_CONNECTORS.md) for the connector extension contract and required verification rules.
