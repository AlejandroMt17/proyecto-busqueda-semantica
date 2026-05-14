<#
.SYNOPSIS
  Installs winutils.exe and hadoop.dll into Spark's bin folder (required for S3A / Hadoop on Windows).

.DESCRIPTION
  Spark on Windows looks for %HADOOP_HOME%\bin\winutils.exe (often HADOOP_HOME = SPARK_HOME).
  Run this on every Windows machine that runs Spark executors or the driver when using s3a://.

.PARAMETER SparkHome
  Root folder that contains a bin directory (Spark or Hadoop home; default: $env:SPARK_HOME).

.PARAMETER WinutilsHadoopVersion
  Hadoop version folder under cdarlint/winutils (default: 3.3.5). Use 3.3.6 when pairing with Hadoop 3.3.6.

.EXAMPLE
  .\scripts\install_winutils_windows.ps1
  .\scripts\install_winutils_windows.ps1 -SparkHome 'D:\spark\spark-3.5.1-bin-hadoop3'
  .\scripts\install_winutils_windows.ps1 -SparkHome 'C:\Apache\hadoop-3.3.6' -WinutilsHadoopVersion '3.3.6'
#>
param(
    [string] $SparkHome = $env:SPARK_HOME,
    [string] $WinutilsHadoopVersion = '3.3.5'
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($SparkHome)) {
    Write-Error 'Set SPARK_HOME or pass -SparkHome to the install root (Spark or Hadoop) that should contain bin\ (e.g. C:\spark).'
}

$SparkHome = $SparkHome.TrimEnd('\', '/')
$binDir = Join-Path $SparkHome 'bin'
if (-not (Test-Path -LiteralPath $binDir)) {
    New-Item -ItemType Directory -Path $binDir -Force | Out-Null
}

# Hadoop 3.3.x winutils matches Spark 3.x "hadoop3" distributions (community build).
$base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-$WinutilsHadoopVersion/bin"
$files = @('winutils.exe', 'hadoop.dll')

foreach ($name in $files) {
    $uri = "$base/$name"
    $out = Join-Path $binDir $name
    Write-Host "Downloading $name -> $out"
    Invoke-WebRequest -Uri $uri -OutFile $out -UseBasicParsing
}

Write-Host ''
Write-Host 'Done. Point HADOOP_HOME at this root on this machine, e.g.:'
Write-Host "  setx HADOOP_HOME `"$SparkHome`""
Write-Host 'Repeat on every Windows worker that runs executors (e.g. 192.168.56.1).'
