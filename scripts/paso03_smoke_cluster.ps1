# Paso 3 (Semana 1 demo): SparkPi — cluster operativo
param(
    [int]$Slices = 100,
    [string]$ExecutorMemory = "2g",
    [string]$DriverMemory = "1g",
    [string]$TotalExecutorCores = "4"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_paso_common.ps1")

$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }
$lanIp = Get-PasoNetworkHost
$env:SEMANTIC_SEARCH_HOST = $lanIp

$examplesDir = Join-Path $env:SPARK_HOME "examples\jars"
$jar = Get-ChildItem -LiteralPath $examplesDir -Filter "spark-examples_*.jar" -File | Select-Object -First 1
if (-not $jar) { throw "No se encontro spark-examples_*.jar en $examplesDir" }

$masterUrl = "spark://$($lanIp):7077"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"

Write-Host "Master: $masterUrl" -ForegroundColor Cyan
Write-Host "spark.driver.host=$($lanIp)" -ForegroundColor Cyan
Write-Host "Abri http://$($lanIp):4040 mientras corre." -ForegroundColor Yellow

& $submit `
    --class org.apache.spark.examples.SparkPi `
    --master $masterUrl `
    --deploy-mode client `
    --driver-memory $DriverMemory `
    --executor-memory $ExecutorMemory `
    --total-executor-cores $TotalExecutorCores `
    --conf "spark.driver.host=$($lanIp)" `
    --conf "spark.driver.bindAddress=0.0.0.0" `
    $jar.FullName `
    $Slices

if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "OK - captura Master UI (8080) y Application UI (4040)." -ForegroundColor Green
Write-Host "Siguiente: .\scripts\paso04_etl_arxiv.ps1" -ForegroundColor Yellow
