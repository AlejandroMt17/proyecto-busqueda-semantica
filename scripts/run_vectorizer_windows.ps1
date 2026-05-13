# Ejemplo Windows - Fase 2 (embeddings) con Spark.
#
# IP / MinIO: usa $env:SEMANTIC_SEARCH_HOST si está definida; si no, lee
# `network.host` de conf/config.yaml. Los buckets y credenciales también salen
# del config (`minio.*`), por lo que NO se pasan como --s3-*.

$ErrorActionPreference = "Stop"

$Repo = "D:\Users\crism\OneDrive\Documentos\GitHub\proyecto-busqueda-semantica"
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }
$RunDate = if ($env:RUN_DATE) { $env:RUN_DATE } else { "2026-05-12" }
$Mode = if ($env:SPARK_MODE) { $env:SPARK_MODE } else { "local" }   # local | cluster

$venvPy = Join-Path $Repo ".venv\Scripts\python.exe"
$resolveHost = @"
import os, sys
sys.path.insert(0, r'$Repo\src')
from project_config import load_project_config
cfg = load_project_config(r'$Repo\conf\config.yaml')
print((cfg.get('network') or {}).get('host') or '')
"@
$NetworkHost = (& $venvPy -c $resolveHost).Trim()
if (-not $NetworkHost) { throw "No pude resolver network.host (config.yaml o SEMANTIC_SEARCH_HOST)." }

$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$app = Join-Path $Repo "src\spark_vectorizer.py"

if ($Mode -eq "cluster") {
  & $submit `
    --packages $pkg `
    --driver-memory 4g `
    $app `
      --run-date $RunDate `
      --master "spark://$($NetworkHost):7077" `
      --driver-host $NetworkHost `
      --skip-stats `
      --validate-output
} else {
  & $submit `
    --packages $pkg `
    --driver-memory 6g `
    $app `
      --run-date $RunDate `
      --master "local[*]" `
      --input-glob "s3a://semantic-raw/features/run_date=$RunDate/" `
      --output-s3a "s3a://semantic-raw/embeddings/run_date=$RunDate/" `
      --skip-stats `
      --validate-output
}
