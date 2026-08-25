<#!
.SYNOPSIS
Runs live React-to-FastAPI PostgreSQL migration proofs in a temporary local database.

.DESCRIPTION
Starts temporary API and Vite processes, provisions a randomly named database and
four tiny tables in the local Docker PostgreSQL instance, runs guided and background
worker browser proofs, then removes the entire temporary database and stops the
temporary processes. Docker control-plane data is retained.
#>

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$testDatabase = "schemabridge_ui_e2e_$([guid]::NewGuid().ToString('N').Substring(0, 12))"
$sourceTable = "sb_ui_e2e_source_$([guid]::NewGuid().ToString('N').Substring(0, 12))"
$targetTable = "sb_ui_e2e_target_$([guid]::NewGuid().ToString('N').Substring(0, 12))"
$backgroundSourceTable = "sb_ui_e2e_background_source_$([guid]::NewGuid().ToString('N').Substring(0, 12))"
$backgroundTargetTable = "sb_ui_e2e_background_target_$([guid]::NewGuid().ToString('N').Substring(0, 12))"
$resumeSourceTable = "sb_ui_e2e_resume_source_$([guid]::NewGuid().ToString('N').Substring(0, 12))"
$resumeTargetTable = "sb_ui_e2e_resume_target_$([guid]::NewGuid().ToString('N').Substring(0, 12))"
$profileJson = @{
    'ui-e2e-source' = @{ db_type = 'postgresql'; host = '127.0.0.1'; database = $testDatabase; username = 'schemabridge'; password = 'local_only_change_me'; connection_options = @{ port = 55432 }; timeout_seconds = 30; max_rows = 500; write_enabled = $false }
    'ui-e2e-target' = @{ db_type = 'postgresql'; host = '127.0.0.1'; database = $testDatabase; username = 'schemabridge'; password = 'local_only_change_me'; connection_options = @{ port = 55432 }; timeout_seconds = 30; max_rows = 500; write_enabled = $true }
} | ConvertTo-Json -Compress -Depth 4

$apiProcess = $null
$frontendProcess = $null
$containerId = $null
$testDatabaseCreated = $false
$locationsPushed = 0

function Wait-HttpReady([string]$url) {
    foreach ($attempt in 1..30) {
        try {
            if ((Invoke-WebRequest -UseBasicParsing $url -TimeoutSec 2).StatusCode -eq 200) { return }
        } catch { }
        Start-Sleep -Seconds 1
    }
    throw "Timed out waiting for $url"
}

try {
    Push-Location $root
    $locationsPushed++
    docker compose up -d control-plane
    $containerId = (docker compose ps -q control-plane).Trim()
    if (-not $containerId) { throw 'The local control-plane PostgreSQL container is unavailable.' }

    docker exec $containerId psql -v ON_ERROR_STOP=1 -U schemabridge -d postgres -c "CREATE DATABASE $testDatabase;"
    $testDatabaseCreated = $true
    $env:SCHEMABRIDGE_CONTROL_PLANE_DSN = "postgresql://schemabridge:local_only_change_me@127.0.0.1:55432/$testDatabase"
    & "$root\.venv\Scripts\python.exe" -m scripts.migrate_control_plane
    if ($LASTEXITCODE -ne 0) { throw "Control-plane migration failed with exit code $LASTEXITCODE." }

    docker exec $containerId psql -v ON_ERROR_STOP=1 -U schemabridge -d $testDatabase -c "CREATE TABLE public.$sourceTable (id integer PRIMARY KEY, customer_name text NOT NULL, amount numeric(10,2) NOT NULL); INSERT INTO public.$sourceTable (id, customer_name, amount) VALUES (1, 'Ada', 12.50), (2, 'Grace', 18.75), (3, 'Linus', 24.00); CREATE TABLE public.$targetTable (id integer PRIMARY KEY, customer_name text NOT NULL, amount numeric(10,2) NOT NULL); CREATE TABLE public.$backgroundSourceTable (id integer PRIMARY KEY, customer_name text NOT NULL, amount numeric(10,2) NOT NULL); INSERT INTO public.$backgroundSourceTable (id, customer_name, amount) VALUES (1, 'Ada', 12.50), (2, 'Grace', 18.75), (3, 'Linus', 24.00); CREATE TABLE public.$backgroundTargetTable (id integer PRIMARY KEY, customer_name text NOT NULL, amount numeric(10,2) NOT NULL); CREATE TABLE public.$resumeSourceTable (id integer PRIMARY KEY, customer_name text NOT NULL, amount numeric(10,2) NOT NULL); INSERT INTO public.$resumeSourceTable (id, customer_name, amount) VALUES (1, 'Ada', 12.50), (2, 'Grace', 18.75), (3, 'Linus', 24.00); CREATE TABLE public.$resumeTargetTable (id integer PRIMARY KEY, customer_name text NOT NULL, amount numeric(10,2) NOT NULL);"

    $env:DB_PROFILES_JSON = $profileJson
    $apiProcess = Start-Process -FilePath "$root\.venv\Scripts\python.exe" -ArgumentList @('-m', 'uvicorn', 'schemabridge.api.app:create_app', '--factory', '--env-file', '.env', '--host', '127.0.0.1', '--port', '8000') -WorkingDirectory $root -WindowStyle Hidden -PassThru
    $frontendProcess = Start-Process -FilePath 'npm.cmd' -ArgumentList @('run', 'dev', '--', '--host', '127.0.0.1', '--port', '5173') -WorkingDirectory "$root\frontend" -WindowStyle Hidden -PassThru
    Wait-HttpReady 'http://127.0.0.1:8000/health/ready'
    Wait-HttpReady 'http://127.0.0.1:5173'

    $env:SCHEMABRIDGE_UI_E2E_SOURCE_TABLE = $sourceTable
    $env:SCHEMABRIDGE_UI_E2E_TARGET_TABLE = $targetTable
    $env:SCHEMABRIDGE_UI_E2E_BACKGROUND_SOURCE_TABLE = $backgroundSourceTable
    $env:SCHEMABRIDGE_UI_E2E_BACKGROUND_TARGET_TABLE = $backgroundTargetTable
    $env:SCHEMABRIDGE_UI_E2E_RESUME_SOURCE_TABLE = $resumeSourceTable
    $env:SCHEMABRIDGE_UI_E2E_RESUME_TARGET_TABLE = $resumeTargetTable
    $env:SCHEMABRIDGE_UI_E2E_CATALOG = $testDatabase
    Push-Location "$root\frontend"
    $locationsPushed++
    npm run test:e2e
    if ($LASTEXITCODE -ne 0) { throw "Playwright failed with exit code $LASTEXITCODE." }

    Pop-Location
    $locationsPushed--
    & "$root\.venv\Scripts\python.exe" -m scripts.run_migration_worker
    if ($LASTEXITCODE -ne 0) { throw "Migration worker failed with exit code $LASTEXITCODE." }
    $rowsWritten = (docker exec $containerId psql -v ON_ERROR_STOP=1 -U schemabridge -d $testDatabase -tAc "SELECT count(*) FROM public.$backgroundTargetTable;").Trim()
    if ($rowsWritten -ne '3') { throw "Background worker wrote $rowsWritten rows; expected 3." }

    $env:SCHEMABRIDGE_UI_E2E_VERIFY_WORKER = '1'
    Push-Location "$root\frontend"
    $locationsPushed++
    npm run test:e2e -- --grep "shows the completed background job"
    if ($LASTEXITCODE -ne 0) { throw "Completed-job Playwright check failed with exit code $LASTEXITCODE." }
} finally {
    if ($frontendProcess -and -not $frontendProcess.HasExited) { Stop-Process -Id $frontendProcess.Id -Force }
    if ($apiProcess -and -not $apiProcess.HasExited) { Stop-Process -Id $apiProcess.Id -Force }
    if ($containerId -and $testDatabaseCreated) { docker exec $containerId psql -v ON_ERROR_STOP=1 -U schemabridge -d postgres -c "DROP DATABASE IF EXISTS $testDatabase WITH (FORCE);" }
    while ($locationsPushed -gt 0) {
        Pop-Location
        $locationsPushed--
    }
}
