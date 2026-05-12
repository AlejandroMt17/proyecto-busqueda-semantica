# Arranca MinIO en Windows sin Docker (binario oficial).
# Dejá esta ventana abierta mientras generás/subís datos.
# API: http://127.0.0.1:9000   Consola: http://127.0.0.1:9001
# Usuario: minioadmin  Clave: minioadmin123

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$BinDir = Join-Path $RepoRoot ".tools\minio"
$Exe = Join-Path $BinDir "minio.exe"
$Data = Join-Path $BinDir "data"

New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
New-Item -ItemType Directory -Force -Path $Data | Out-Null

if (-not (Test-Path $Exe)) {
    $url = "https://dl.min.io/server/minio/release/windows-amd64/minio.exe"
    Write-Host "Descargando MinIO desde $url ..."
    Invoke-WebRequest -Uri $url -OutFile $Exe
}

$env:MINIO_ROOT_USER = "minioadmin"
$env:MINIO_ROOT_PASSWORD = "minioadmin123"

Write-Host ""
Write-Host "MinIO (sin Docker)"
Write-Host "  S3 API:    http://127.0.0.1:9000"
Write-Host "  Consola:   http://127.0.0.1:9001"
Write-Host "  Usuario:   minioadmin"
Write-Host "  Clave:     minioadmin123"
Write-Host ""
Write-Host "En otra ventana: pip install -r requirements.txt"
Write-Host "                 python scripts\generate_data.py --seed 42"
Write-Host ""

Set-Location $BinDir
& $Exe server $Data --console-address ":9001"
