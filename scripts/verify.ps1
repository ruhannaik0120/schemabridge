<#
Runs the complete local quality gate: compilation, pytest, and an offline smoke
test. Any failed stage stops the script and returns a failing process exit code.
#>

$ErrorActionPreference = "Stop"

# Derive the shared virtual environment and fail early when setup was skipped.
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    throw "Virtual environment missing. Run scripts\setup.ps1 first."
}

Push-Location $ProjectRoot
try {
    # Keep verification bytecode inside an ignored test-only directory.
    $TestTemp = Join-Path $ProjectRoot ".test-runtime"
    New-Item -ItemType Directory -Path $TestTemp -Force | Out-Null
    # Redirect bytecode away from OneDrive-managed source caches, which can be
    # temporarily locked by editors or an already-running API process.
    $env:PYTHONPYCACHEPREFIX = Join-Path $TestTemp "pycache"
    & $Python -m compileall -q schemabridge scripts tests
    if ($LASTEXITCODE -ne 0) { throw "Compilation failed." }
    & $Python -m pytest -q
    if ($LASTEXITCODE -ne 0) { throw "Test suite failed." }
    & $Python -m pip check
    if ($LASTEXITCODE -ne 0) { throw "Dependency consistency check failed." }
    & $Python -c "from schemabridge.api.app import create_app; schema=create_app().openapi(); assert '/api/v1/migrations/workflows/{workflow_id}/validate' in schema['paths']"
    if ($LASTEXITCODE -ne 0) { throw "FastAPI import/OpenAPI check failed." }
    # The deterministic demo connector verifies startup without live credentials.
    $env:DB_TYPE = "demo"
    $env:DB_HOST = "demo-local"
    $env:DB_DATABASE = "schemabridge_demo"
    $env:DB_USERNAME = ""
    $env:DB_PASSWORD = ""
    $env:DB_CONNECTION_OPTIONS = "{}"
    & $Python -m tests.smoke_test
    if ($LASTEXITCODE -ne 0) { throw "Smoke test failed." }
    Write-Host "All verification gates passed."
} finally {
    Pop-Location
}
