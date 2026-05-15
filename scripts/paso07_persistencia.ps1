# Paso 7 (Fase 3): indexar en Elasticsearch
param([string]$RunDate)

$ErrorActionPreference = "Stop"
$Repo = Split-Path -Parent $PSScriptRoot
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }

$venvPy = Join-Path $Repo ".venv\Scripts\python.exe"
$resolveCfg = @"
import sys
sys.path.insert(0, r'$Repo\src')
from project_config import load_project_config
cfg = load_project_config(r'$Repo\conf\config.yaml')
print((cfg.get('network') or {}).get('host') or '')
print(str(cfg.get('run_date') or '').strip())
print((cfg.get('minio') or {}).get('endpoint') or 'http://127.0.0.1:9000')
"@
$lines = & $venvPy -c $resolveCfg
$NetworkHost = $lines[0].Trim()
if (-not $RunDate) { $RunDate = if ($env:RUN_DATE) { $env:RUN_DATE.Trim() } else { $lines[1].Trim() } }
$MinioEndpoint = $lines[2].Trim()
$esHost = if ($env:ES_HOST) { $env:ES_HOST.Trim() } else { $NetworkHost }

$pkgEs = "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"

& $submit --packages $pkgEs `
  (Join-Path $Repo "src\persistence.py") `
  --run-date $RunDate `
  --master "spark://${NetworkHost}:7077" `
  --driver-host $NetworkHost `
  --es-host $esHost `
  --s3-endpoint $MinioEndpoint
