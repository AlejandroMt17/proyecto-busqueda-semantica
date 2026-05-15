# Paso 5: validación de calidad Fase 1 (post-ETL)
param(
    [string]$RunDate,
    [int]$ExpectedMinRows = 500000
)

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
print((cfg.get('minio') or {}).get('bucket') or 'semantic-raw')
print((cfg.get('minio') or {}).get('endpoint') or 'http://127.0.0.1:9000')
"@
$lines = & $venvPy -c $resolveCfg
$NetworkHost = $lines[0].Trim()
if (-not $RunDate) { $RunDate = if ($env:RUN_DATE) { $env:RUN_DATE.Trim() } else { $lines[1].Trim() } }
$Bucket = $lines[2].Trim()
$Endpoint = $lines[3].Trim()

$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$glob = "s3a://${Bucket}/features/run_date=${RunDate}/*.csv"

& $submit --packages $pkg `
  --master "spark://${NetworkHost}:7077" --driver-host $NetworkHost `
  (Join-Path $Repo "scripts\validate_etl_quality.py") `
  --input-glob $glob `
  --s3-endpoint $Endpoint `
  --expected-min-rows $ExpectedMinRows
