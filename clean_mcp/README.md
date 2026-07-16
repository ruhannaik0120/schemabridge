# MCP Database Execution Framework

Reusable, AI-client-agnostic MCP server for executing approved SQL commands through named database profiles. It supports SQL Server, PostgreSQL, MySQL, Snowflake, and an offline demo connector through one stable tool and response contract.

## Where To Start

Read the folder in this order:

1. `server.py` - MCP entry point and published tool names.
2. `tools/` - thin agent-facing wrappers.
3. `services/` - profile, runtime, and request orchestration.
4. `connectors/` - shared connector contract and database implementations.
5. `config.py` - validated configuration and credential redaction.
6. `models/` and `validation/` - response contracts and SQL guardrails.
7. `tests/` - behavior, safety, architecture, and smoke verification.

Files such as `.env`, `.pytest-tmp`, `.test-runtime`, and `__pycache__` are local
or generated and are not part of the delivered source tree.

## Framework Scope

`clean_mcp` is a standalone MCP database execution framework. It exposes one
stable tool surface for selecting configured database profiles, inspecting
metadata, executing approved SQL commands, and returning structured results.

```text
MCP-compatible client
        |
        v
clean_mcp: profile selection, validation, execution, structured response
        |
        v
SQL Server | PostgreSQL | MySQL | Snowflake | future connectors
```

## Capabilities

- Standard MCP `stdio` transport for compatible AI clients.
- Connector factory and stable `DatabaseConnector` extension contract.
- Approval-gated named profile switching with connection verification and rollback.
- Runtime serialization so queries cannot overlap a half-finished profile switch.
- Approved SQL command execution, including reads, writes, and DDL permitted by the database account.
- Dialect-aware one-statement checks, bounded returned rows, and connection plus statement timeouts.
- Structured responses, errors, request IDs, duration, profile metadata, and result data.
- Credential redaction in diagnostics and errors.
- Structured technical console logging with request correlation.
- Architecture tests that keep vendor drivers inside `connectors/`.

## MCP Tools

| Tool | Purpose |
|---|---|
| `tool_list_connection_profiles` | Lists profile names and safe metadata without credentials. |
| `tool_switch_connection_profile` | Requires `confirm=true`, verifies the target, and rolls back on failure. |
| `tool_reload_configuration` | Requires `confirm=true`, atomically reloads the local `.env`, and rolls back invalid changes. |
| `tool_config_diagnostics` | Returns redacted effective configuration. |
| `tool_test_connection` | Performs a real connection test and returns safe server metadata. |
| `tool_health` | Checks the active connector's operational status. |
| `tool_list_databases` | Lists visible databases. |
| `tool_list_tables` | Lists tables/views by database and schema. |
| `tool_describe_table` | Returns normalized column metadata. |
| `tool_suggest_columns` | Ranks similar real column names without modifying or executing SQL. |
| `tool_execute_query` | Primary tool for one approved SQL command/query. |
| `tool_execute_select_query` | Deprecated compatibility alias for `tool_execute_query`. |

The deprecated alias uses the same generic execution path and does not impose different SQL behavior.

## Configuration

Copy the example and keep real credentials only in the ignored `.env` file:

```powershell
Copy-Item .\clean_mcp\.env.example .\clean_mcp\.env
```

Profiles are configured through `DB_PROFILES_JSON`. The AI works only with names such as `postgres-local`; profile listing returns credential-presence flags, never credential values.

The server treats configuration as a startup snapshot. After changing the local
`.env`, call `tool_reload_configuration(confirm=true)` or restart the server.
Reload is atomic: invalid settings leave the previous active configuration and
connector available. In production environments that inject configuration from
a secret manager rather than a local `.env`, update the deployment environment
and restart the MCP process instead of using the reload tool.

Profile-controlled target, credential, and timeout fields cannot be overridden through `connection_options`. Use `connection_options` only for backend-specific settings such as an ODBC driver, TLS mode, warehouse, role, or schema.

```env
DB_TYPE=demo
DB_DATABASE=qa_demo
DB_TIMEOUT_SECONDS=30
DB_MAX_ROWS=500
DB_ACTIVE_PROFILE=demo-local
DB_PROFILES_JSON={"demo-local":{"db_type":"demo","database":"qa_demo"},"postgres-local":{"db_type":"postgresql","host":"localhost","database":"qa_demo","username":"qa_user","password":"qa_password","connection_options":{"port":5432}}}
```

## Setup And Verification

From the repository root:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\clean_mcp\scripts\setup.ps1
PowerShell -ExecutionPolicy Bypass -File .\clean_mcp\scripts\verify.ps1
```

For SQL Server, the setup script installs `pyodbc`, but Windows must also have the Microsoft ODBC Driver 18 for SQL Server installed. The offline `demo` profile does not require any database driver, credentials, or network access.

VS Code discovers the server through `.vscode/mcp.json`. Restart the MCP server after changing `.env`.

## Safety Model

- The calling client must obtain human approval before execution and profile changes.
- The profile-switch tool requires the explicit `confirm=true` assertion.
- Query execution is bound to the active profile's configured database; another database requires an approved profile switch.
- Use only sandbox/test databases and least-privilege profile credentials.
- Keep cloud/private databases reachable only through approved company network access.
- Database permissions remain the final authority for allowed commands.
- Returned rows cannot exceed `DB_MAX_ROWS`; request timeouts cannot exceed `DB_TIMEOUT_SECONDS`.
- Credentials, tokens, private keys, and connection strings are redacted from agent-visible diagnostics and errors.
- One tool call accepts one SQL statement; comments and multiple statements are rejected to keep requests unambiguous.

## Repository Map

```text
server.py         MCP registration and stdio entry point
config.py         Validated, redacted runtime configuration
connectors/       Common contract, factory, and vendor implementations
services/         Request orchestration and profile switching
tools/            Thin MCP-facing wrappers
validation/       Single-command structural validation
models/           Stable response and error contracts
tests/            Unit, behavior, and architecture tests
docs/             Integration, extension, and testing guides
scripts/          Setup and verification automation
```

See [ADDING_CONNECTORS.md](docs/ADDING_CONNECTORS.md) for the connector extension contract and required verification rules.
See [PRODUCTION_DEPLOYMENT.md](docs/PRODUCTION_DEPLOYMENT.md) for process isolation, secret injection, and metadata-assisted recovery guidance.
