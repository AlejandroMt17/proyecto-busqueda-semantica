# Paso 4 (Semana 2 / Fase 1): ETL sobre arXiv en MinIO → features/run_date=...
param(
    [string]$RunDate,
    [string]$InputGlob,
    [int]$MinChunks = 500000,
    [switch]$Pilot,
    [switch]$SkipStats,
    [switch]$WordTokenization
)

$ErrorActionPreference = "Stop"
$Repo = Split-Path -Parent $PSScriptRoot
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }

$venvPy = Join-Path $Repo ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPy)) { throw "Creá el venv: python -m venv .venv && .\.venv\Scripts\pip install -r requirements.txt" }

$resolveCfg = @"
import sys
sys.path.insert(0, r'$Repo\src')
from project_config import load_project_config
cfg = load_project_config(r'$Repo\conf\config.yaml')
net = (cfg.get('network') or {}).get('host') or ''
rd = str(cfg.get('run_date') or '').strip()
bucket = (cfg.get('minio') or {}).get('bucket') or 'semantic-raw'
ep = (cfg.get('minio') or {}).get('endpoint') or 'http://127.0.0.1:9000'
print(net)
print(rd)
print(bucket)
print(ep)
"@
$lines = & $venvPy -c $resolveCfg
$NetworkHost = $lines[0].Trim()
$RunDateCfg = $lines[1].Trim()
$Bucket = $lines[2].Trim()
$MinioEndpoint = $lines[3].Trim()

if (-not $NetworkHost) { throw "Definí network.host en conf/config.yaml" }
if (-not $RunDate) { $RunDate = if ($env:RUN_DATE) { $env:RUN_DATE.Trim() } else { $RunDateCfg } }
if (-not $RunDate) { throw "Definí --RunDate o run_date en config.yaml" }

if ($Pilot) {
    $pilotLocal = Join-Path $Repo "data\raw\arxiv_pilot_5k.jsonl"
    $pilotKey = "arxiv-pilot-5k.jsonl"
    $src = "C:\Users\crism\Downloads\archive (2)\arxiv-metadata-oai-snapshot.json"
    if (-not (Test-Path $pilotLocal)) {
        New-Item -ItemType Directory -Force -Path (Split-Path $pilotLocal) | Out-Null
        Write-Host "Generando muestra 5000 líneas → $pilotLocal" -ForegroundColor Yellow
        Get-Content -LiteralPath $src -TotalCount 5000 -Encoding UTF8 | Set-Content -LiteralPath $pilotLocal -Encoding UTF8
    }
    & (Join-Path $Repo "scripts\paso02_subir_arxiv.ps1") -SourcePath $pilotLocal -S3Key $pilotKey
    $InputGlob = "s3a://${Bucket}/${pilotKey}"
    $MinChunks = 1000
    $SkipStats = $true
}

if (-not $InputGlob) {
    $InputGlob = "s3a://${Bucket}/arxiv-metadata-oai-snapshot.json"
}

# arXiv usa "abstract", no "text" — esquema explícito evita inferencia lenta en ~5 GB
$arxivSchema = "id STRING, abstract STRING, title STRING"

$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$app = Join-Path $Repo "src\etl_features.py"

$extra = @(
    "--run-date", $RunDate,
    "--master", "spark://${NetworkHost}:7077",
    "--driver-host", $NetworkHost,
    "--s3-endpoint", $MinioEndpoint,
    "--input-json-glob", $InputGlob,
    "--input-schema", $arxivSchema,
    "--output-dir", "s3a://${Bucket}/features/run_date=${RunDate}/",
    "--min-chunks", "$MinChunks"
)
if ($SkipStats) { $extra += "--skip-stats" }
if ($WordTokenization) { $extra += "--word-tokenization" }

Write-Host "ETL arXiv RUN_DATE=$RunDate input=$InputGlob" -ForegroundColor Cyan
& $submit --packages $pkg $app @extra
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Siguiente: .\scripts\paso05_validar_etl.ps1 -RunDate $RunDate" -ForegroundColor Yellow
