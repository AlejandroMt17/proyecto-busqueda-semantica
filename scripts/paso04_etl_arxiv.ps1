# Paso 4 (Semana 2 / Fase 1): ETL sobre arXiv en MinIO -> features/run_date=...
param(
    [string]$RunDate,
    [string]$InputGlob,
    [int]$MinChunks = 500000,
    [switch]$Pilot,
    [switch]$SkipStats,
    [switch]$WordTokenization
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_paso_common.ps1")

$Repo = Get-PasoRepo
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }
$venvPy = Get-PasoPython -Repo $Repo
if (-not (Test-Path -LiteralPath $venvPy) -and $venvPy -eq "python") {
    throw "Crea el venv: python -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}

$cfg = Get-PasoConfigMap
$lanIp = $cfg.host
$Bucket = $cfg.bucket
$MinioEndpoint = $cfg.endpoint
$RunDate = Get-PasoRunDate -Override $RunDate
$pyConfs = Get-PasoSparkSubmitPythonConfs -Repo $Repo

if ($Pilot) {
    $pilotLocal = Join-Path $Repo "data\raw\arxiv_pilot_5k.jsonl"
    $pilotKey = "arxiv-pilot-5k.jsonl"
    $src = Join-Path $Repo "data\raw\arxiv-metadata-oai-snapshot.json"
    if (-not (Test-Path -LiteralPath $pilotLocal)) {
        New-Item -ItemType Directory -Force -Path (Split-Path $pilotLocal) | Out-Null
        Write-Host "Generando muestra 5000 lineas -> $pilotLocal" -ForegroundColor Yellow
        Get-Content -LiteralPath $src -TotalCount 5000 -Encoding UTF8 | Set-Content -LiteralPath $pilotLocal -Encoding UTF8
    }
    & (Join-Path $Repo "scripts\paso02_subir_arxiv.ps1") -SourcePath $pilotLocal -S3Key $pilotKey
    $InputGlob = "s3a://$Bucket/$pilotKey"
    $MinChunks = 1000
    $SkipStats = $true
    if (-not $WordTokenization) {
        Write-Host "AVISO: en workers remotos conviene -WordTokenization si no tienen tokenizers/HF instalados." -ForegroundColor Yellow
    }
}

if (-not $InputGlob) {
    $InputGlob = "s3a://$Bucket/arxiv-metadata-oai-snapshot.json"
}

$arxivSchema = "id STRING, abstract STRING, title STRING"
$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$app = Join-Path $Repo "src\etl_features.py"

$extra = @(
    "--run-date", $RunDate,
    "--master", "spark://$($lanIp):7077",
    "--driver-host", $lanIp,
    "--s3-endpoint", $MinioEndpoint,
    "--input-json-glob", $InputGlob,
    "--input-schema", $arxivSchema,
    "--output-dir", "s3a://$Bucket/features/run_date=$RunDate/",
    "--min-chunks", "$MinChunks"
)
if ($SkipStats) { $extra += "--skip-stats" }
if ($WordTokenization) { $extra += "--word-tokenization" }

Write-Host "ETL arXiv RUN_DATE=$RunDate input=$InputGlob" -ForegroundColor Cyan
# --conf es de spark-submit; debe ir ANTES del .py (si va despues, etl_features.py lo rechaza).
& $submit --packages $pkg @pyConfs $app @extra
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Siguiente: .\scripts\paso05_validar_etl.ps1 -RunDate $RunDate" -ForegroundColor Yellow
