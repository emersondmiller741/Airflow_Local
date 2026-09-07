# ============================================
# Airflow Method 2 - Create / Start
# ============================================

$AirflowDir = "C:\Airflow_Local"

Set-Location $AirflowDir

Write-Host "Starting Airflow initialization..." -ForegroundColor Cyan

docker compose up airflow-init

if ($LASTEXITCODE -ne 0) {
    Write-Host "Airflow initialization failed." -ForegroundColor Red
    exit 1
}

Write-Host "Initialization complete." -ForegroundColor Green
Write-Host "Starting Airflow services..." -ForegroundColor Cyan

docker compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to start Airflow." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Airflow is starting." -ForegroundColor Green
Write-Host ""
Write-Host "Check containers with:" -ForegroundColor Yellow
Write-Host "  docker compose ps"
Write-Host ""
Write-Host "Airflow UI:" -ForegroundColor Yellow
Write-Host "  http://localhost:8080"