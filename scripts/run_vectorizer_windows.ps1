# Ejemplo Windows - Fase 2 (embeddings) con Spark.
#
# - Fuerza el Python del repo (.venv) para driver y executors en local[*]
#   (evita "PyArrow not found" cuando spark-submit usa otro Python).
# - IP / MinIO: respeta $env:SEMANTIC_SEARCH_HOST; si no, lee network.host de conf/config.yaml.
# - RUN_DATE: si no está definido, usa run_date del config.yaml.
#
# En modo cluster, cada worker Linux necesita pyarrow, sentence-transformers y torch
# en el Python del PATH o en el que indiques con $env:SPARK_EXECUTOR_PYTHON.

$ErrorActionPreference = "Stop"

$Repo = "D:\Users\crism\OneDrive\Documentos\GitHub\proyecto-busqueda-semantica"
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }
$Mode = if ($env:SPARK_MODE) { $env:SPARK_MODE } else { "local" }   # local | cluster

$venvPy = Join-Path $Repo ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPy)) { throw "No existe el venv: $venvPy" }

# Mismo Python para driver y workers locales (obligatorio para Pandas UDF + Arrow).
$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy

$resolveCfg = @"
import os, sys
sys.path.insert(0, r'$Repo\src')
from project_config import load_project_config
cfg = load_project_config(r'$Repo\conf\config.yaml')
net = (cfg.get('network') or {}).get('host') or ''
rd = str(cfg.get('run_date') or '2026-05-12').strip()
print(net)
print(rd)
"@
$cfgLines = & $venvPy -c $resolveCfg
$NetworkHost = ($cfgLines[0]).Trim()
$RunDateFromCfg = ($cfgLines[1]).Trim()
$RunDate = if ($env:RUN_DATE) { $env:RUN_DATE.Trim() } else { $RunDateFromCfg }

if (-not $NetworkHost) { throw "No pude resolver network.host (config.yaml o SEMANTIC_SEARCH_HOST)." }

Write-Host "RUN_DATE=$RunDate  network.host=$NetworkHost  SPARK_MODE=$Mode  PYSPARK_PYTHON=$venvPy" -ForegroundColor Cyan
if ($env:SEMANTIC_SEARCH_HOST) {
  Write-Host "Nota: SEMANTIC_SEARCH_HOST=$($env:SEMANTIC_SEARCH_HOST) sobrescribe la IP del YAML." -ForegroundColor Yellow
}

$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$app = Join-Path $Repo "src\batch_inference.py"

# Asegura dependencias del driver (falla rápido si faltan).
& $venvPy -c "import pyarrow; import sentence_transformers; import torch" 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "Instalando dependencias Fase 2 en el venv..." -ForegroundColor Yellow
  & $venvPy -m pip install -q "pyarrow>=10,<20" "sentence-transformers>=2.2,<3" "torch>=2.0,<3"
}

if ($Mode -eq "cluster") {
  $clusterArgs = @(
    "--packages", $pkg,
    "--driver-memory", "4g"
  )
  if ($env:SPARK_EXECUTOR_PYTHON) {
    $clusterArgs += @("--conf", "spark.pyspark.python=$($env:SPARK_EXECUTOR_PYTHON)")
    Write-Host "Executors remotos: spark.pyspark.python=$($env:SPARK_EXECUTOR_PYTHON)" -ForegroundColor Cyan
  } else {
    Write-Host "AVISO: sin SPARK_EXECUTOR_PYTHON los workers usan python3 del PATH (debe tener torch, pyarrow, sbert)." -ForegroundColor Yellow
  }
  $clusterArgs += @(
    $app,
    "--run-date", $RunDate,
    "--master", "spark://$($NetworkHost):7077",
    "--driver-host", $NetworkHost,
    "--skip-stats",
    "--validate-output"
  )
  & $submit @clusterArgs
} else {
  & $submit `
    --packages $pkg `
    --driver-memory 6g `
    $app `
      --run-date $RunDate `
      --master "local[*]" `
      --skip-stats `
      --validate-output
}
