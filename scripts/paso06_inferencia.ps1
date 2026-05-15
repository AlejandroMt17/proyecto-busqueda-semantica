# Paso 6 (Fase 2): embeddings con batch_inference.py
param([string]$RunDate)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_paso_common.ps1")

$Repo = Get-PasoRepo
$env:SPARK_HOME = if ($env:SPARK_HOME) { $env:SPARK_HOME } else { "C:\spark" }
$venvPy = Get-PasoPython -Repo $Repo
if (-not (Test-Path -LiteralPath $venvPy)) { throw "No existe .venv\Scripts\python.exe" }

$lanIp = Get-PasoNetworkHost
$RunDate = Get-PasoRunDate -Override $RunDate
$pyConfs = Get-PasoSparkSubmitPythonConfs -Repo $Repo

Write-Host "RUN_DATE=$RunDate  network.host=$($lanIp)" -ForegroundColor Cyan

& $venvPy -c "import pyarrow; import sentence_transformers; import torch" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Instalando dependencias Fase 2..." -ForegroundColor Yellow
    & $venvPy -m pip install -q "pyarrow>=10,<20" "sentence-transformers>=2.2,<3" "torch>=2.0,<3"
}

$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$app = Join-Path $Repo "src\batch_inference.py"

$sparkArgs = $pyConfs + @(
    "--packages", $pkg,
    "--driver-memory", "4g",
    $app,
    "--run-date", $RunDate,
    "--master", "spark://$($lanIp):7077",
    "--driver-host", $lanIp,
    "--skip-stats",
    "--validate-output"
)
& $submit @sparkArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Siguiente: .\scripts\paso07_persistencia.ps1" -ForegroundColor Yellow
