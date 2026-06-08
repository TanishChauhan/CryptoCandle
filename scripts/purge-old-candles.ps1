# Purge old 1-minute candles (same logic as automated data-retention service).
#
# Usage:
#   .\scripts\purge-old-candles.ps1
#   .\scripts\purge-old-candles.ps1 -KeepDays 2 -SummarizeFirst

param(
    [int]$KeepDays = 0,
    [switch]$SummarizeFirst
)

$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path (Join-Path $PSScriptRoot ".."))

if (-not (Test-Path ".env")) {
    Write-Error ".env not found. Copy .env.example to .env first."
}

$env:RETENTION_RUN_ONCE = "true"
if ($KeepDays -gt 0) {
    $env:RETENTION_KEEP_DAYS = "$KeepDays"
}
if ($SummarizeFirst) {
    $env:RETENTION_SUMMARIZE_FIRST = "true"
} elseif ($KeepDays -gt 0) {
    $env:RETENTION_SUMMARIZE_FIRST = "false"
}

Write-Host "Running one-shot retention via data-retention container..."
docker compose --env-file .env run --rm data-retention python -m services.retention.runner --once
Write-Host "Done."
