# Trigger Airflow DAGs without the web UI (scheduler must be running).
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("crypto_hourly_backfill", "crypto_daily_batch", "crypto_data_quality")]
    [string]$DagId
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

docker compose --env-file .env exec airflow-scheduler airflow dags trigger $DagId
docker compose --env-file .env exec airflow-scheduler airflow dags list-runs -d $DagId -o table
