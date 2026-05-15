# Paso 3 (Semana 1 demo): SparkPi — cluster operativo
param(
    [int]$Slices = 100,
    [string]$ExecutorMemory = "2g",
    [string]$DriverMemory = "1g",
    [string]$TotalExecutorCores = "4"
)

$ErrorActionPreference = "Stop"
$Repo = Split-Path -Parent $PSScriptRoot
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }

$resolveCfg = @"
import sys
sys.path.insert(0, r'$Repo\src')
from project_config import load_project_config
cfg = load_project_config(r'$Repo\conf\config.yaml')
print((cfg.get('network') or {}).get('host') or '')
"@
$hostIp = (& python -c $resolveCfg).Trim()
if (-not $hostIp) {
    if ($env:SEMANTIC_SEARCH_HOST) { $hostIp = $env:SEMANTIC_SEARCH_HOST.Trim() }
    elseif ($env:SPARK_MASTER_HOST) { $hostIp = $env:SPARK_MASTER_HOST.Trim() }
}
if (-not $hostIp) { throw "Definí network.host en conf/config.yaml o SEMANTIC_SEARCH_HOST" }

$env:SEMANTIC_SEARCH_HOST = $hostIp
$driverHost = $hostIp

$examplesDir = Join-Path $env:SPARK_HOME "examples\jars"
$jar = Get-ChildItem -LiteralPath $examplesDir -Filter "spark-examples_*.jar" -File | Select-Object -First 1
if (-not $jar) { throw "No se encontró spark-examples_*.jar en $examplesDir" }

$masterUrl = "spark://${hostIp}:7077"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"

Write-Host "Master: $masterUrl" -ForegroundColor Cyan
Write-Host "spark.driver.host=$driverHost" -ForegroundColor Cyan
Write-Host "Abrí http://${driverHost}:4040 mientras corre." -ForegroundColor Yellow

& $submit `
    --class org.apache.spark.examples.SparkPi `
    --master $masterUrl `
    --deploy-mode client `
    --driver-memory $DriverMemory `
    --executor-memory $ExecutorMemory `
    --total-executor-cores $TotalExecutorCores `
    --conf "spark.driver.host=$driverHost" `
    --conf "spark.driver.bindAddress=0.0.0.0" `
    $jar.FullName `
    $Slices

if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "OK - captura Master UI (8080) y Application UI (4040)." -ForegroundColor Green
Write-Host "Siguiente: .\scripts\paso04_etl_arxiv.ps1" -ForegroundColor Yellow
