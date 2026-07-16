# E2E PoC Run Guide

## Purpose

The E2E QA PoC turns the requirements in a Jira ticket into documented QA checks, approved SQL validation queries, saved database evidence, and an optional management-friendly report.

It coordinates two existing capabilities without combining their responsibilities:

- **Atlassian MCP** is used to retrieve Jira ticket context.
- **The database MCP server in `clean_mcp/`** is used only for database profile operations, metadata checks, and execution of SQL that the user has approved.

The agent coordinates the workflow outside both MCP servers. The helper scripts create and export local files only; they do not contact Jira, an MCP server, or a database.

## Workflow At A Glance

```text
User logs in to Atlassian
        |
        v
User provides Jira ticket key
        |
        v
Agent retrieves context through Atlassian MCP
        |
        v
ticket_context.md -> user review and approval
        |
        v
qa_plan.md -> generated_queries.sql -> user SQL approval
        |
        v
Existing clean_mcp DB MCP executes approved queries
        |
        v
expected-vs-actual evaluation -> explicit validation_status
        |
        v
execution_result.json -> JSON only / HTML / Excel / both
```

All generated run evidence is stored under `poc_runs/<ticket_id>/`, outside `clean_mcp/`.

## Before Starting

From the repository root, prepare the shared virtual environment once:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\clean_mcp\scripts\setup.ps1
```

The setup script installs core and E2E dependencies from their separate requirements files. Run `clean_mcp\scripts\verify.ps1` for the complete offline quality gate before the first live ticket.

1. The user logs in to Atlassian separately.
2. Atlassian MCP is made available to the agent.
3. The database profiles needed for the validation are configured through the approved `clean_mcp` configuration process. Credentials are not placed in prompts or run artifacts.
4. The agent reads `00_BASIC_INSTRUCTIONS.md` before taking any workflow action.
5. The user provides the Jira ticket key, for example `ABC-123`.

The E2E run does not begin by changing `clean_mcp/`. `clean_mcp/` is the existing reusable database execution service.

## Step-By-Step Run

### 1. Initialize The Run Folder

From the repository root, create the standard artifact structure:

```powershell
.\.venv\Scripts\python.exe .\scripts\init_poc_run.py ABC-123
```

The script sanitizes the ticket key for safe folder naming and creates only missing files. Running it again does not delete or overwrite existing run files.

It creates:

```text
poc_runs/ABC-123/
|-- ticket_context.md
|-- qa_plan.md
|-- generated_queries.sql
|-- approval_log.md
|-- run_log.md
|-- execution_result.json
`-- output/
```

The script does not retrieve Jira information and does not connect to a database.

### 2. Retrieve Jira Context

The agent uses Atlassian MCP and the supplied ticket key to retrieve the ticket summary, description, acceptance criteria, relevant comments, links, and other available QA context.

The agent organizes the retrieved information in:

```text
poc_runs/<ticket_id>/ticket_context.md
```

Atlassian MCP is used only for Jira context. Jira workflow logic is not added to `clean_mcp/`.

### 3. Review And Approve The Ticket Context

The agent asks the user to review `ticket_context.md`. The workflow stops until the user confirms that the context is complete enough for QA planning.

If information is missing, the user can add it manually or ask the agent to check Jira again through Atlassian MCP. The approval is recorded in `approval_log.md`, and non-secret activity is recorded in `run_log.md`.

### 4. Create The QA Plan

After context approval, the agent converts the confirmed requirements into specific checks and expected outcomes. It identifies which checks need database validation and which database systems or named profiles are required.

The plan is saved to:

```text
poc_runs/<ticket_id>/qa_plan.md
```

### 5. Generate SQL For Review

The agent creates the proposed validation queries and saves them to:

```text
poc_runs/<ticket_id>/generated_queries.sql
```

Each query should have a stable check ID, be associated with a QA check, and have an understandable expected result. Prefer non-mutating validation queries. If DML or DDL is required, explain its impact and request explicit approval for that statement.

The database MCP must not execute the SQL until the user approves it. If an approved query is changed, the changed version requires approval again.

### 6. Execute Through The Existing Database MCP

After SQL approval, the agent uses the database MCP server in `clean_mcp/` to execute only the approved queries. It sends one exact approved SQL statement per MCP call, excluding the organizational comments and separators in `generated_queries.sql`. The DB MCP rejects comments and multi-statement requests.

The helper scripts do not call the DB MCP. The MCP call is made by the agent through the available MCP tools.

Structured execution evidence is saved to:

```text
poc_runs/<ticket_id>/execution_result.json
```

Each execution action and non-secret outcome is recorded in `run_log.md`.

### 7. Evaluate Expected Versus Actual Results

For each stable check ID, the agent compares the returned data with the expected result in `qa_plan.md` and records an explicit `validation_status`.

- `passed` means the actual result satisfies the expected outcome.
- `failed` means the actual result does not satisfy the expected outcome.
- `executed_not_evaluated` means SQL execution succeeded but the QA comparison has not been completed.
- `execution_failed` means the SQL did not execute successfully.

Database MCP `success=true` means only that execution succeeded. It must never be treated as proof that the QA check passed.

The recommended `execution_result.json` shape is:

```json
{
  "schema_version": "1.0",
  "ticket_id": "ABC-123",
  "query_results": [
    {
      "check_id": "CHECK-1",
      "execution_success": true,
      "validation_status": "passed",
      "expected": "No duplicate active records",
      "actual": "0 duplicate records",
      "metadata": {"profile": "approved-test-profile"},
      "row_count": 1,
      "rows": [{"duplicate_count": 0}]
    }
  ],
  "errors": []
}
```

The exporter also accepts one raw MCP response or a top-level list of raw MCP responses. Raw execution success is reported as `executed_not_evaluated` until the agent adds an explicit QA evaluation.

### 8. Handle Multiple Systems Or Profiles

If a QA plan needs more than one database, system, or profile, each execution target must have a named profile. The agent:

1. explains why the next system or profile is required;
2. asks the user for explicit approval before switching;
3. records the decision in `approval_log.md`;
4. switches through the database MCP only after approval; and
5. records the non-secret switch outcome in `run_log.md`.

Approval is required before every profile or environment switch, not only the first one.

### 9. Choose And Create The Final Output

At the end of the run, the agent asks the user to choose and explicitly approve one format:

- `json_only`: keep `execution_result.json` and create no report files.
- `html`: create `output/report.html`.
- `excel`: create `output/report.xlsx`.
- `both`: create both reports.

Run the exporter from the repository root:

```powershell
.\.venv\Scripts\python.exe .\scripts\export_results.py ABC-123 --format json_only
.\.venv\Scripts\python.exe .\scripts\export_results.py ABC-123 --format html
.\.venv\Scripts\python.exe .\scripts\export_results.py ABC-123 --format excel
.\.venv\Scripts\python.exe .\scripts\export_results.py ABC-123 --format both
```

HTML uses the Python standard library. Excel requires the separate E2E dependency:

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\requirements-e2e.txt
```

Do not run HTML or Excel export until the user makes this choice. Record the export approval in `approval_log.md`. The exporter reads the saved `execution_result.json`; it does not execute queries or contact Jira, Atlassian MCP, the database MCP, or a database.

## Approval Checkpoints

The agent must stop and obtain human approval:

1. after retrieving and organizing the Jira ticket context, before QA planning;
2. after generating SQL, before executing it; and
3. before every database system, profile, or environment switch; and
4. before generating the final HTML or Excel report.

Approvals and rejections belong in `approval_log.md`. Activity and outcomes belong in `run_log.md`.

## Safety Notes

- Never hardcode credentials, tokens, connection strings, or private keys.
- Never store credentials in `ticket_context.md`, SQL files, logs, JSON results, or reports.
- Keep Jira retrieval and E2E workflow logic outside `clean_mcp/`.
- Keep every generated run artifact under `poc_runs/<ticket_id>/`, not inside `clean_mcp/`.
- Use Atlassian MCP only for Jira context.
- Use the DB MCP only for database/profile/query operations.
- Execute only user-approved SQL in approved test environments.
- Use least-privilege database profiles and rely on database permissions as the final access boundary.
- Review result previews before sharing reports because business data can itself be sensitive.
- Generated ticket folders under `poc_runs/` are ignored by Git and must not be committed.
