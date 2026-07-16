# Production Deployment

## Session Isolation

The supported transport is MCP `stdio`. One MCP client launches one server
process, and that process owns one active database profile and connector cache.
Every response includes a non-secret `runtime_id`, startup timestamp, and
`session_isolation=one_client_per_process` metadata so logs can distinguish
independent runtimes.

Do not route multiple users through one shared process. A remote gateway should
start or assign one worker process per authenticated user/session. The process
lock prevents profile-switch/query races inside a worker; the process boundary
prevents credentials and active profiles from crossing sessions.

## Secret Injection

Local development may keep ignored credentials in `clean_mcp/.env`. Production
deployments should omit that file and inject the same recognized variables from
the platform's secret manager before starting the MCP process:

- `DB_TYPE`, `DB_HOST`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`
- `DB_CONNECTION_OPTIONS`, `DB_TIMEOUT_SECONDS`, `DB_MAX_ROWS`
- `DB_ACTIVE_PROFILE`, `DB_PROFILES_JSON`, `LOG_LEVEL`

The framework is vendor-neutral: Azure Key Vault, AWS Secrets Manager,
HashiCorp Vault, Kubernetes Secrets, or another approved provider should render
secrets into the process environment or a deployment-managed environment file.
The agent receives only profile names, presence flags, readiness issues, and
redacted diagnostics.

Rotate production secrets through the deployment platform and restart the
affected worker. `tool_reload_configuration` is intentionally limited to an
approved local `.env` reload and should not replace production secret rotation.

## Metadata-Assisted Recovery

When a database reports an unknown column, the client can call
`tool_suggest_columns` with the approved profile, table, schema, and missing
column name. The tool reads table metadata and ranks similar column names. It
does not rewrite SQL and does not execute a query.

The orchestrator must show any revised SQL to the user and obtain a new approval
before calling `tool_execute_query`. This keeps metadata discovery helpful while
preserving the human authorization boundary.

## Connector Expansion

Add new systems through the `DatabaseConnector` contract and factory registry.
Do not add vendor drivers or credentials to the MCP transport, tool wrappers, or
orchestration service. Follow `ADDING_CONNECTORS.md` and add fake-driver tests
plus a separate opt-in live connection check.
