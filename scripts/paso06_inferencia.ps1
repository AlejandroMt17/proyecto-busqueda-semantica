# Paso 6 (Fase 2): embeddings con batch_inference.py
param([string]$RunDate)

$ErrorActionPreference = "Stop"
$Repo = Split-Path -Parent $PSScriptRoot
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }

$venvPy = Join-Path $Repo ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPy)) { throw "Creá el venv: python -m venv .venv && .\.venv\Scripts\pip install -r requirements.txt" }

$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy

$resolveCfg = @"
import sys
sys.path.insert(0, r'$Repo\src')
from project_config import load_project_config
cfg = load_project_config(r'$Repo\conf\config.yaml')
print((cfg.get('network') or {}).get('host') or '')
print(str(cfg.get('run_date') or '').strip())
"@
$lines = & $venvPy -c $resolveCfg
$NetworkHost = $lines[0].Trim()
if (-not $RunDate) { $RunDate = if ($env:RUN_DATE) { $env:RUN_DATE.Trim() } else { $lines[1].Trim() } }
if (-not $NetworkHost) { throw "Definí network.host en conf/config.yaml" }

Write-Host "RUN_DATE=$RunDate  network.host=$NetworkHost" -ForegroundColor Cyan

& $venvPy -c "import pyarrow; import sentence_transformers; import torch" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Instalando dependencias Fase 2..." -ForegroundColor Yellow
    & $venvPy -m pip install -q "pyarrow>=10,<20" "sentence-transformers>=2.2,<3" "torch>=2.0,<3"
}

$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$app = Join-Path $Repo "src\batch_inference.py"

$args = @(
    "--packages", $pkg,
    "--driver-memory", "4g",
    $app,
    "--run-date", $RunDate,
    "--master", "spark://${NetworkHost}:7077",
    "--driver-host", $NetworkHost,
    "--skip-stats",
    "--validate-output"
)
if ($env:SPARK_EXECUTOR_PYTHON) {
    $args = @("--conf", "spark.pyspark.python=$($env:SPARK_EXECUTOR_PYTHON)") + $args
}

& $submit @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Siguiente: .\scripts\paso07_persistencia.ps1" -ForegroundColor Yellow
