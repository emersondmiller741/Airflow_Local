Set-Location "C:\Airflow_local"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Airflow 3.0.0 Teardown" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Stopping containers and removing orphan containers..." -ForegroundColor Yellow

docker compose down --remove-orphans --volumes --rmi all

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Teardown failed." -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Airflow environment removed." -ForegroundColor Green
Write-Host ""
Write-Host "Preserved:" -ForegroundColor Cyan
Write-Host "  config\airflow.cfg"
Write-Host "  dags\"
Write-Host "  logs\"
Write-Host "  plugins\"
Write-Host "  docker-compose.yaml"
Write-Host ""