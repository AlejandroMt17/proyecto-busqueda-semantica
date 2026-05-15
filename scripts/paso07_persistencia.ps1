# Paso 7 (Fase 3): indexar en Elasticsearch
param([string]$RunDate)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_paso_common.ps1")

$Repo = Get-PasoRepo
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }

$cfg = Get-PasoConfigMap
$lanIp = $cfg.host
$MinioEndpoint = $cfg.endpoint
$RunDate = Get-PasoRunDate -Override $RunDate
$esHost = if ($env:ES_HOST) { $env:ES_HOST.Trim() } else { $lanIp }

$pyConfs = Get-PasoSparkSubmitPythonConfs -Repo $Repo
$pkgEs = "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"

& $submit @pyConfs --packages $pkgEs `
  (Join-Path $Repo "src\persistence.py") `
  --run-date $RunDate `
  --master "spark://$($lanIp):7077" `
  --driver-host $lanIp `
  --es-host $esHost `
  --s3-endpoint $MinioEndpoint
