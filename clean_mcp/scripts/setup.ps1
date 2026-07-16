<#
Creates the workspace virtual environment and installs all Python dependencies.
The script is repeatable: an existing healthy environment is reused, while a
broken interpreter is replaced before installation continues.
#>

$ErrorActionPreference = "Stop"

# Resolve paths from the script location so setup works from any current folder.
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$WorkspaceRoot = Split-Path -Parent $ProjectRoot
$Venv = Join-Path $WorkspaceRoot ".venv"

if (Test-Path $Venv) {
    # Validate the existing interpreter instead of assuming the folder is usable.
    $Python = Join-Path $Venv "Scripts\python.exe"
    $VenvHealthy = $false
    if (Test-Path $Python) {
        & $Python --version *> $null
        $VenvHealthy = $LASTEXITCODE -eq 0
    }
    if (-not $VenvHealthy) {
        Remove-Item -Recurse -Force $Venv
    }
}

if (-not (Test-Path $Venv)) {
    # Prefer the Windows Python launcher, then fall back to a PATH installation.
    $Launcher = Get-Command py -ErrorAction SilentlyContinue
    if ($Launcher) {
        & py -3 -m venv $Venv
        if ($LASTEXITCODE -ne 0) { throw "Failed to create the virtual environment with py." }
    } else {
        $PythonCommand = Get-Command python -ErrorAction Stop
        & $PythonCommand.Source -m venv $Venv
        if ($LASTEXITCODE -ne 0) { throw "Failed to create the virtual environment with python." }
    }
}

$Python = Join-Path $Venv "Scripts\python.exe"
# Install with the virtual-environment interpreter to avoid global packages.
& $Python -m pip install "pip>=26.1.2"
if ($LASTEXITCODE -ne 0) { throw "Failed to install the minimum secure pip version." }
& $Python -m pip install -r (Join-Path $ProjectRoot "requirements.txt")
if ($LASTEXITCODE -ne 0) { throw "Failed to install project dependencies." }
Write-Host "Environment ready: $Python"
