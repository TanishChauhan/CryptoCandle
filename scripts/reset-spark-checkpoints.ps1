# Reset Spark streaming checkpoints after Kafka offset drift or container rebuild.
# Usage: .\scripts\reset-spark-checkpoints.ps1

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "Stopping spark-streaming..."
docker compose --env-file .env stop spark-streaming

$checkpointDir = Join-Path (Get-Location) "data\checkpoints\stream_trades"
if (Test-Path $checkpointDir) {
    Write-Host "Removing $checkpointDir ..."
    Remove-Item -Recurse -Force $checkpointDir
}

Write-Host "Starting spark-streaming..."
docker compose --env-file .env up -d spark-streaming
Write-Host "Done. Spark will recreate checkpoints from STARTING_OFFSETS (default: latest)."
