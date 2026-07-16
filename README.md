# AI-Driven QA Automation PoC

This repository separates database execution from the external E2E QA workflow.

## Where To Start

Read [00_BASIC_INSTRUCTIONS.md](00_BASIC_INSTRUCTIONS.md) first. It defines the mandatory Jira-to-database order, human approvals, generated evidence files, reporting choices, and safety boundaries.

Do not start the E2E workflow by editing `clean_mcp/`. It is the existing generic database execution server; the E2E orchestration instructions, helper scripts, and generated run artifacts remain outside it.

Use the [E2E PoC run guide](docs/E2E_POC_RUN_GUIDE.md) for the complete step-by-step operating procedure. The [database MCP documentation](clean_mcp/README.md) separately explains the existing DB execution server and its tools.

The short agent starter prompt is available at `instructions/start_e2e_prompt.txt`. Non-secret workflow policy and artifact defaults are documented in `poc_run_config.json`; credentials never belong there.

## First-Time Setup

From the repository root, create the shared virtual environment and install both the database MCP and separately declared E2E helper dependencies:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\clean_mcp\scripts\setup.ps1
```

This repository uses `.venv\Scripts\python.exe` directly so commands also work on Windows systems where bare `python` is not on `PATH`.

## E2E Helper Scripts

The setup command above installs the optional Excel export dependency from the separate E2E requirements file. To repair or update only that dependency, run:

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\requirements-e2e.txt
```

Initialize the local files for a ticket without overwriting existing artifacts:

```powershell
.\.venv\Scripts\python.exe .\scripts\init_poc_run.py ABC-123
```

Export the saved `execution_result.json` after the approved workflow has completed:

```powershell
.\.venv\Scripts\python.exe .\scripts\export_results.py ABC-123 --format html
.\.venv\Scripts\python.exe .\scripts\export_results.py ABC-123 --format excel
.\.venv\Scripts\python.exe .\scripts\export_results.py ABC-123 --format both
.\.venv\Scripts\python.exe .\scripts\export_results.py ABC-123 --format json_only
```

`scripts/init_poc_run.py` and `scripts/export_results.py` only create and read local run artifacts. They do not call Jira, Atlassian MCP, the database MCP, or any database. Excel export uses `openpyxl` from `requirements-e2e.txt`; HTML and JSON-only behavior use the Python standard library.

## Repository Structure

### Database MCP Server

`clean_mcp/` is the working generic database MCP server. It provides named database profiles, connection and metadata operations, approved query execution, structured results, and database-specific connectors. It stays independent of Jira and the E2E run-artifact workflow.

See the [database MCP documentation](clean_mcp/README.md) for its tools, configuration, supported databases, and verification steps.

### External E2E Workflow Layer

The workflow files outside `clean_mcp/` tell an AI agent how to:

1. retrieve Jira context through Atlassian MCP;
2. stop for user review and approval;
3. create a QA plan and proposed SQL;
4. use the existing database MCP for approved database operations; and
5. save evidence and optional reports under `poc_runs/<ticket_id>/`.

Generated run folders are local artifacts and are excluded from Git. Credentials must never be stored in those folders.

## Verification

Run the complete database MCP, offline smoke, and external-helper quality gate:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\clean_mcp\scripts\verify.ps1
```
