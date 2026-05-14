# Orquesta Fase 1 → Fase 2 → Fase 3 (manual).
#
# Modo local (por defecto):
#   .\scripts\run_pipeline_windows.ps1
#
# Modo cluster (tú eres el master; workers ya conectados a spark://IP:7077):
#   $env:SPARK_MODE = "cluster"
#   $env:SEMANTIC_SEARCH_HOST = "10.169.72.85"   # si no, usa network.host del YAML
#   $env:ES_HOST = "10.169.72.85"                # IP que TODOS los executors alcancen (Elasticsearch)
#   # Si el driver es Windows y los workers Linux: NO pongas una ruta Linux aquí.
#   # En cada worker Linux, en $SPARK_HOME/conf/spark-env.sh:
#   #   export PYSPARK_PYTHON=/home/usuario/proyecto/.venv/bin/python3
#   # (solo si driver y workers comparten la misma ruta de Python, podés usar SPARK_EXECUTOR_PYTHON)
#   .\scripts\run_pipeline_windows.ps1
#
# Opcional cluster: entrada ETL en MinIO (si no pasás nada, el ETL usa el default s3a del script Python).
#   $env:INPUT_JSON_GLOB = "s3a://semantic-raw/arxiv-metadata-oai-snapshot.json"
#
# S3A / HADOOP_HOME en workers:
#   Solo workers Linux, todos con Spark en /opt/spark:
#     $env:EXECUTOR_HADOOP_HOME = "/opt/spark"
#   Worker Windows (p. ej. VirtualBox): Hadoop exige ruta ABSOLUTA con unidad. NO uses /opt/spark.
#     $env:EXECUTOR_HADOOP_HOME = "C:/spark"
#     (debe coincidir con SPARK_HOME en ESE worker; winutils en bin: wiki Hadoop Windows)
#   Rutas distintas o mixto Linux+Windows: no uses EXECUTOR_HADOOP_HOME; en cada nodo spark-env:
#     export HADOOP_HOME="${SPARK_HOME}"   (Linux)   |   set HADOOP_HOME=%SPARK_HOME%   (Windows)
#
# RUN_DATE: $env:RUN_DATE o run_date en conf/config.yaml

$ErrorActionPreference = "Stop"

$Repo = Split-Path -Parent $PSScriptRoot

$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }
$venvPy = Join-Path $Repo ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPy)) { throw "No existe el venv: $venvPy" }

$Mode = if ($env:SPARK_MODE) { $env:SPARK_MODE.Trim().ToLowerInvariant() } else { "local" }

$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy

$resolveCfg = @"
import sys
from pathlib import Path
sys.path.insert(0, r'$Repo\src')
from project_config import load_project_config
cfg = load_project_config(r'$Repo\conf\config.yaml')
rd = str(cfg.get('run_date') or '').strip()
raw = (cfg.get('paths') or {}).get('raw') or 'data/raw/arxiv_sample.jsonl'
raw_abs = str((Path(r'$Repo') / raw).resolve())
net = (cfg.get('network') or {}).get('host') or ''
print(rd or '2026-05-12')
print(raw_abs)
print(net or '')
"@
$cfgLines = & $venvPy -c $resolveCfg
$RunDateFromCfg = ($cfgLines[0]).Trim()
$RawJsonAbs = ($cfgLines[1]).Trim()
$NetworkHost = ($cfgLines[2]).Trim()
$RunDate = if ($env:RUN_DATE) { $env:RUN_DATE.Trim() } else { $RunDateFromCfg }

$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$pkgEs = "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0"
$pkgEsS3 = "$pkgEs,$pkg"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"

$esHost = if ($env:ES_HOST) { $env:ES_HOST.Trim() } elseif ($Mode -eq "cluster" -and $NetworkHost) { $NetworkHost } else { "127.0.0.1" }

$driverMem = if ($env:SPARK_DRIVER_MEMORY) { $env:SPARK_DRIVER_MEMORY } else { "4g" }
$execMem = if ($env:SPARK_EXECUTOR_MEMORY) { $env:SPARK_EXECUTOR_MEMORY } else { "4g" }
$cores = if ($env:SPARK_TOTAL_EXECUTOR_CORES) { $env:SPARK_TOTAL_EXECUTOR_CORES } else { "6" }

Write-Host "SPARK_MODE=$Mode  RUN_DATE=$RunDate  ES_HOST=$esHost  network.host=$NetworkHost" -ForegroundColor Cyan

function Get-ExecutorHadoopHomeConf {
  $raw = if ($null -ne $env:EXECUTOR_HADOOP_HOME) { $env:EXECUTOR_HADOOP_HOME.Trim() } else { "" }
  if ([string]::IsNullOrWhiteSpace($raw)) {
    return @()
  }
  # Hadoop en Windows JVM: rutas con unidad deben ir como C:/foo (barra normal); /opt/spark en Windows falla.
  $h = $raw
  if ($h -match '^[A-Za-z]:') {
    $h = ($h -replace '\\', '/').TrimEnd('/')
  } elseif ($h.StartsWith('/')) {
    Write-Host 'AVISO: EXECUTOR_HADOOP_HOME es ruta estilo Unix (/...). En workers Windows falla: "Hadoop home ... is not an absolute path". Usa C:/spark o quita la variable y define HADOOP_HOME en cada worker.' -ForegroundColor Yellow
  }
  Write-Host "Usando EXECUTOR_HADOOP_HOME=$h para executors (S3A / hadoop.home.dir)." -ForegroundColor Yellow
  return @(
    "--conf", "spark.executorEnv.HADOOP_HOME=$h",
    "--conf", "spark.hadoop.hadoop.home.dir=$h"
  )
}

function Get-ExecutorPythonSparkConf {
  $ep = if ($null -ne $env:SPARK_EXECUTOR_PYTHON) { $env:SPARK_EXECUTOR_PYTHON.Trim() } else { "" }
  if ([string]::IsNullOrWhiteSpace($ep)) {
    if ($Mode -eq "cluster") {
      Write-Host 'AVISO: sin SPARK_EXECUTOR_PYTHON en este PC: los executors usan el Python del worker (PATH o spark-env.sh). Instala torch, pyarrow y sentence-transformers en los workers.' -ForegroundColor Yellow
    }
    return @()
  }
  if (Test-Path -LiteralPath $ep) {
    Write-Host "Anadiendo spark.pyspark.python=$ep (ruta valida en este equipo)." -ForegroundColor Yellow
    return @("--conf", "spark.pyspark.python=$ep")
  }
  Write-Host "AVISO: SPARK_EXECUTOR_PYTHON no existe en este Windows: $ep (no se pasa a Spark)." -ForegroundColor Yellow
  Write-Host 'Con workers Linux: borra SPARK_EXECUTOR_PYTHON aqui y en cada worker edita conf/spark-env.sh: export PYSPARK_PYTHON=/ruta/real/.venv/bin/python3' -ForegroundColor Yellow
  return @()
}

if ($Mode -eq "cluster") {
  if (-not $NetworkHost) { throw "Modo cluster: define network.host en conf/config.yaml o SEMANTIC_SEARCH_HOST." }
  # Driver en Windows + S3A: sin esto a veces fallan tareas que tocan Hadoop en el driver.
  if (-not $env:HADOOP_HOME) {
    $env:HADOOP_HOME = $env:SPARK_HOME
    Write-Host "HADOOP_HOME=$($env:HADOOP_HOME) (driver, para S3A en Windows)" -ForegroundColor DarkGray
  }
  $masterUrl = "spark://${NetworkHost}:7077"
  $driverHost = if ($env:SPARK_DRIVER_HOST) { $env:SPARK_DRIVER_HOST.Trim() } else { $NetworkHost }
  $pyConf = Get-ExecutorPythonSparkConf
  $hadoopConf = Get-ExecutorHadoopHomeConf
  $clusterBase = @(
    "--packages", $pkg,
    "--driver-memory", $driverMem,
    "--executor-memory", $execMem,
    "--total-executor-cores", $cores,
    "--conf", "spark.driver.host=$driverHost",
    "--conf", "spark.driver.bindAddress=0.0.0.0"
  ) + $pyConf + $hadoopConf

  # Fase 1: entrada/salida por defecto en s3a:// (MinIO en network.host). Sobrescribí con INPUT_JSON_GLOB si hace falta.
  $etlArgs = @(
    (Join-Path $Repo "src\etl_features.py"),
    "--run-date", $RunDate,
    "--master", $masterUrl,
    "--driver-host", $driverHost,
    "--skip-stats", "--min-chunks", "0"
  )
  if ($env:INPUT_JSON_GLOB) {
    $etlArgs += @("--input-json-glob", $env:INPUT_JSON_GLOB.Trim())
  }
  & $submit @($clusterBase + $etlArgs)
  if ($LASTEXITCODE -ne 0) { throw "Fase 1 (ETL) falló: código $LASTEXITCODE" }

  $clusterBaseF2 = @(
    "--packages", $pkg,
    "--driver-memory", "6g",
    "--executor-memory", $execMem,
    "--total-executor-cores", $cores,
    "--conf", "spark.driver.host=$driverHost",
    "--conf", "spark.driver.bindAddress=0.0.0.0"
  ) + $pyConf + $hadoopConf
  & $submit @($clusterBaseF2 + @(
      (Join-Path $Repo "src\batch_inference.py"),
      "--run-date", $RunDate,
      "--master", $masterUrl,
      "--driver-host", $driverHost,
      "--skip-stats",
      "--validate-output"
    ))
  if ($LASTEXITCODE -ne 0) { throw "Fase 2 falló: código $LASTEXITCODE" }

  $clusterBaseF3 = @(
    "--packages", $pkgEsS3,
    "--driver-memory", $driverMem,
    "--executor-memory", $execMem,
    "--total-executor-cores", $cores,
    "--conf", "spark.driver.host=$driverHost",
    "--conf", "spark.driver.bindAddress=0.0.0.0"
  ) + $pyConf + $hadoopConf
  & $submit @($clusterBaseF3 + @(
      (Join-Path $Repo "src\persistence.py"),
      "--run-date", $RunDate,
      "--master", $masterUrl,
      "--driver-host", $driverHost,
      "--es-host", $esHost
    ))
  if ($LASTEXITCODE -ne 0) { throw "Fase 3 falló: código $LASTEXITCODE" }
} else {
  Write-Host "input(local)=$RawJsonAbs  PYSPARK_PYTHON=$venvPy" -ForegroundColor Cyan

  & $submit --packages $pkg --driver-memory 4g (Join-Path $Repo "src\etl_features.py") `
    --run-date $RunDate --master "local[*]" --skip-stats --min-chunks 0 `
    --input-json-glob $RawJsonAbs
  if ($LASTEXITCODE -ne 0) { throw "Fase 1 (ETL) falló: código $LASTEXITCODE" }

  & $submit --packages $pkg --driver-memory 6g (Join-Path $Repo "src\batch_inference.py") `
    --run-date $RunDate --master "local[*]" --skip-stats --validate-output
  if ($LASTEXITCODE -ne 0) { throw "Fase 2 falló: código $LASTEXITCODE" }

  & $submit --packages $pkgEs --driver-memory 4g (Join-Path $Repo "src\persistence.py") `
    --run-date $RunDate --master "local[*]" --es-host $esHost
  if ($LASTEXITCODE -ne 0) { throw "Fase 3 falló: código $LASTEXITCODE" }
}

Write-Host "Pipeline Fase 1+2+3 OK (run_date=$RunDate, mode=$Mode)" -ForegroundColor Green
