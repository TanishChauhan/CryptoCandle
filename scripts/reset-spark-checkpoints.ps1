# Reset Spark streaming checkpoints after Kafka offset drift or container rebuild.
# Usage: .\scripts\reset-spark-checkpoints.ps1

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
. (Join-Path $PSScriptRoot "lib\docker-compose.ps1")

Write-Host "Stopping spark-streaming..."
Invoke-Compose -Command @("stop", "spark-streaming") | Out-Null

$checkpointDir = Join-Path (Get-Location) "data\checkpoints\stream_trades"
if (Test-Path $checkpointDir) {
    Write-Host "Removing $checkpointDir ..."
    Remove-Item -Recurse -Force $checkpointDir
}

Write-Host "Starting spark-streaming..."
Invoke-Compose -Command @("up", "-d", "spark-streaming") | Out-Null
Write-Host "Done. Spark will recreate checkpoints from STARTING_OFFSETS (default: latest)."
