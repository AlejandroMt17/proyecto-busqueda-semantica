# Levanta Spark Standalone Master en Windows (PowerShell).
# Requisitos: Java 17, Spark 3.5.x; SPARK_HOME apunta al descomprimido de Spark (no al repo).
# Si no existe sbin\start-master.cmd, se usa sbin/start-master.sh con Git Bash.

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

if ([string]::IsNullOrWhiteSpace($env:SPARK_HOME)) {
    Write-Error 'Definí SPARK_HOME, ej: $env:SPARK_HOME = "C:\spark"'
}

$startMasterCmd = Join-Path $env:SPARK_HOME 'sbin\start-master.cmd'
$startMasterSh = Join-Path $env:SPARK_HOME 'sbin\start-master.sh'
$useBash = $false

if (Test-Path -LiteralPath $startMasterCmd) {
    $startMaster = $startMasterCmd
} elseif (Test-Path -LiteralPath $startMasterSh) {
    $useBash = $true
} else {
    Write-Error "No hay start-master en $($env:SPARK_HOME)\sbin. Reinstalá Spark 3.5.x (bin-hadoop3) desde spark.apache.org."
}

if ([string]::IsNullOrWhiteSpace($env:SPARK_MASTER_HOST)) {
    Write-Error @'
Definí la IP de ESTA máquina en la LAN (la que usarán los workers), por ejemplo:
  $env:SPARK_MASTER_HOST = "192.168.1.10"
Si Get-NetIPAddress te da 192.168.56.1, suele ser VirtualBox Host-Only: para el lab usá la IP del Wi‑Fi/Ethernet (ipconfig).
'@
}

if ([string]::IsNullOrWhiteSpace($env:SPARK_CONF_DIR)) {
    $env:SPARK_CONF_DIR = Join-Path $RepoRoot 'conf'
    Write-Host "SPARK_CONF_DIR no estaba definido; uso: $($env:SPARK_CONF_DIR)"
}

Write-Host "SPARK_HOME=$($env:SPARK_HOME)"
Write-Host "SPARK_MASTER_HOST=$($env:SPARK_MASTER_HOST)"
Write-Host "SPARK_CONF_DIR=$($env:SPARK_CONF_DIR)"
Write-Host ''
Write-Host "UI: http://$($env:SPARK_MASTER_HOST):8080"
Write-Host ''

if (-not $useBash) {
    Set-Location $env:SPARK_HOME
    & $startMaster
    return
}

$gitBash64 = Join-Path $env:ProgramFiles 'Git\bin\bash.exe'
$gitBash32 = Join-Path ${env:ProgramFiles(x86)} 'Git\bin\bash.exe'
$bashFromPath = (Get-Command bash -ErrorAction SilentlyContinue).Source

$bashExe = @($gitBash64, $gitBash32, $bashFromPath) |
    Where-Object { $_ -and (Test-Path -LiteralPath $_) } |
    Select-Object -First 1

if (-not $bashExe) {
    Write-Error @'
No se encontró Git Bash (bash.exe). Opciones:
  1) Instalar Git for Windows, o
  2) Reextraer Spark oficial: debe existir C:\spark\sbin\start-master.cmd
'@
}

Write-Host "start-master.cmd no encontrado; uso Git Bash + sbin/start-master.sh ($bashExe)"
Write-Host ''

function ConvertTo-SparkUnixPath([string]$winPath) {
    if ([string]::IsNullOrWhiteSpace($winPath)) { return $winPath }
    $u = $winPath.TrimEnd('\') -replace '\\', '/'
    if ($u -match '^([A-Za-z]):/(.*)$') {
        return '/' + $Matches[1].ToLowerInvariant() + '/' + $Matches[2]
    }
    return $u
}

$shHome = ConvertTo-SparkUnixPath $env:SPARK_HOME
$shConf = ConvertTo-SparkUnixPath $env:SPARK_CONF_DIR
$mh = $env:SPARK_MASTER_HOST.Replace("'", "'\''")

$bashScript = @"
set -e
export SPARK_HOME='$shHome'
export SPARK_CONF_DIR='$shConf'
export SPARK_MASTER_HOST='$mh'
cd "`$SPARK_HOME"
./sbin/start-master.sh
"@

& $bashExe -lc $bashScript
