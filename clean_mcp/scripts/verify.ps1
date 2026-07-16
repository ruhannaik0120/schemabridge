<#
Runs the complete local quality gate: compilation, pytest, and an offline smoke
test. Any failed stage stops the script and returns a failing process exit code.
#>

$ErrorActionPreference = "Stop"

# Derive the shared virtual environment and fail early when setup was skipped.
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$WorkspaceRoot = Split-Path -Parent $ProjectRoot
$Python = Join-Path $WorkspaceRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    throw "Virtual environment missing. Run clean_mcp\scripts\setup.ps1 first."
}

Push-Location $ProjectRoot
try {
    # Keep verification bytecode inside an ignored test-only directory.
    $TestTemp = Join-Path $ProjectRoot ".test-runtime"
    New-Item -ItemType Directory -Path $TestTemp -Force | Out-Null
    # Redirect bytecode away from OneDrive-managed source caches, which can be
    # temporarily locked by editors or an already-running MCP process.
    $env:PYTHONPYCACHEPREFIX = Join-Path $TestTemp "pycache"
    & $Python -m compileall -q .
    if ($LASTEXITCODE -ne 0) { throw "Compilation failed." }
    & $Python -m pytest -q
    if ($LASTEXITCODE -ne 0) { throw "Test suite failed." }
    # The deterministic demo connector verifies startup without live credentials.
    $env:DB_TYPE = "demo"
    $env:DB_HOST = "demo-local"
    $env:DB_DATABASE = "qa_demo"
    $env:DB_USERNAME = ""
    $env:DB_PASSWORD = ""
    $env:DB_CONNECTION_OPTIONS = "{}"
    $env:DB_ACTIVE_PROFILE = "demo-local"
    & $Python tests\smoke_test.py
    if ($LASTEXITCODE -ne 0) { throw "Smoke test failed." }
    Write-Host "All verification gates passed."
} finally {
    Pop-Location
}
