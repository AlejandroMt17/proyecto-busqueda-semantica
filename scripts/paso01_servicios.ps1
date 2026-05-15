# Paso 1 (PDF / Semana 1): MinIO + Elasticsearch + Tika
$ErrorActionPreference = "Stop"
$Repo = Split-Path -Parent $PSScriptRoot
Set-Location $Repo

Write-Host "Levantando docker compose (MinIO :9000, ES :9200, Tika :9998)..." -ForegroundColor Cyan
docker compose up -d

$deadline = (Get-Date).AddMinutes(3)
function Wait-Http($url) {
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
            if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) { return $true }
        } catch { Start-Sleep -Seconds 3 }
    }
    return $false
}

if (-not (Wait-Http "http://127.0.0.1:9000/minio/health/live")) { throw "MinIO no respondio en :9000" }
if (-not (Wait-Http "http://127.0.0.1:9200")) { throw "Elasticsearch no respondio en :9200" }

Write-Host "OK - MinIO consola: http://127.0.0.1:9001 (minioadmin / minioadmin123)" -ForegroundColor Green
Write-Host "OK - API S3 app: http://127.0.0.1:9000 (admin / admin12345 tras minio-init)" -ForegroundColor Green
Write-Host "OK - Elasticsearch: http://127.0.0.1:9200" -ForegroundColor Green
Write-Host "Siguiente: .\scripts\paso02_subir_arxiv.ps1" -ForegroundColor Yellow
