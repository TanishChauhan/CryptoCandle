# Trigger Airflow DAGs without the web UI (scheduler must be running).
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet(
        "crypto_hourly_backfill",
        "crypto_daily_batch",
        "crypto_data_quality",
        "crypto_data_retention"
    )]
    [string]$DagId
)

$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path (Join-Path $PSScriptRoot ".."))
. (Join-Path $PSScriptRoot "lib\docker-compose.ps1")

# Airflow scheduler can briefly start before DAGs are parsed/registered.
# Wait until target DAG appears in `airflow dags list` to avoid DagNotFound.
$maxAttempts = 12
$sleepSeconds = 5
$dagFound = $false
$dagPattern = "(?m)^$([Regex]::Escape($DagId))\s"

for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    $prevErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $dagListOutput = & docker compose --env-file .env exec -T airflow-scheduler airflow dags list 2>&1
    $listExitCode = $LASTEXITCODE
    $ErrorActionPreference = $prevErrorAction

    if ($listExitCode -eq 0 -and ($dagListOutput -join "`n") -match $dagPattern) {
        $dagFound = $true
        break
    }

    if ($attempt -lt $maxAttempts) {
        Write-Host "Waiting for DAG '$DagId' to be registered by scheduler ($attempt/$maxAttempts)..."
        Start-Sleep -Seconds $sleepSeconds
    }
}

if (-not $dagFound) {
    Invoke-Compose -AllowFailure -Command @("exec", "-T", "airflow-scheduler", "airflow", "dags", "list-import-errors") | Out-Null
    throw "DAG '$DagId' is not visible to scheduler after $($maxAttempts * $sleepSeconds)s. Check scheduler logs and DAG import status."
}

# Even after appearing in `dags list`, Airflow CLI can still briefly return DagNotFound.
# Retry the trigger command itself when that specific race happens.
$triggerSucceeded = $false
for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    $prevErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $triggerOutput = & docker compose --env-file .env exec -T airflow-scheduler airflow dags trigger $DagId 2>&1
    $triggerExitCode = $LASTEXITCODE
    $ErrorActionPreference = $prevErrorAction

    $triggerText = ($triggerOutput -join "`n")
    if ($triggerExitCode -eq 0) {
        $triggerSucceeded = $true
        break
    }

    if ($triggerText -match "Dag id .* not found" -and $attempt -lt $maxAttempts) {
        Write-Host "Trigger race for DAG '$DagId' ($attempt/$maxAttempts), retrying in ${sleepSeconds}s..."
        Start-Sleep -Seconds $sleepSeconds
        continue
    }

    throw "docker compose failed (exit ${triggerExitCode}): docker compose exec -T airflow-scheduler airflow dags trigger $DagId`n$triggerText"
}

if (-not $triggerSucceeded) {
    throw "Failed to trigger DAG '$DagId' after $maxAttempts attempts due to repeated DagNotFound."
}

Invoke-Compose -Command @("exec", "airflow-scheduler", "airflow", "dags", "list-runs", "-d", $DagId, "-o", "table")
