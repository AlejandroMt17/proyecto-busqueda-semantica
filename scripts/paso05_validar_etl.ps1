# Paso 5: validacion de calidad Fase 1 (post-ETL)
param(
    [string]$RunDate,
    [int]$ExpectedMinRows = 500000
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_paso_common.ps1")

$Repo = Get-PasoRepo
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }

$cfg = Get-PasoConfigMap
$lanIp = $cfg.host
$Bucket = $cfg.bucket
$Endpoint = $cfg.endpoint
$RunDate = Get-PasoRunDate -Override $RunDate

$pyConfs = Get-PasoSparkSubmitPythonConfs -Repo $Repo
$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$glob = "s3a://$Bucket/features/run_date=$RunDate/*.csv"

& $submit @pyConfs --packages $pkg `
  --master "spark://$($lanIp):7077" --driver-host $lanIp `
  (Join-Path $Repo "scripts\validate_etl_quality.py") `
  --input-glob $glob `
  --s3-endpoint $Endpoint `
  --expected-min-rows $ExpectedMinRows
