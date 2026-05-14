<#
.SYNOPSIS
  Installs Apache Hadoop (binary tarball) on Windows and adds winutils.exe / hadoop.dll.

.DESCRIPTION
  Downloads hadoop-VERSION.tar.gz from archive.apache.org, extracts under -InstallParent,
  then runs install_winutils_windows.ps1 with a matching Winutils version.
  Typical size is several hundred MB; use a disk outside OneDrive if you sync the repo.

.PARAMETER Version
  Hadoop release (default 3.3.6, aligns with Spark 3.x hadoop3 client).

.PARAMETER InstallParent
  Directory that will contain the folder hadoop-<Version> (default: %LOCALAPPDATA%\Apache).

.PARAMETER Force
  Remove existing hadoop-<Version> and re-download the tarball.

.PARAMETER SkipWinutils
  Only download/extract Hadoop; skip winutils (not recommended on Windows).

.EXAMPLE
  .\scripts\install_hadoop_windows.ps1
  .\scripts\install_hadoop_windows.ps1 -InstallParent 'C:\Apache' -Version '3.3.6'
#>
param(
    [string] $Version = '3.3.6',
    [string] $InstallParent = $(Join-Path $env:LOCALAPPDATA 'Apache'),
    [switch] $Force,
    [switch] $SkipWinutils
)

$ErrorActionPreference = 'Stop'

$name = "hadoop-$Version"
$hadoopHome = Join-Path $InstallParent $name
$hadoopCmd = Join-Path $hadoopHome 'bin\hadoop.cmd'
$winutilsExe = Join-Path $hadoopHome 'bin\winutils.exe'
$winutilsScript = Join-Path $PSScriptRoot 'install_winutils_windows.ps1'

if (-not (Test-Path -LiteralPath $InstallParent)) {
    New-Item -ItemType Directory -Path $InstallParent -Force | Out-Null
}

$needTarball = $Force -or -not (Test-Path -LiteralPath $hadoopCmd)

if ($needTarball) {
    if ((Test-Path -LiteralPath $hadoopHome) -and $Force) {
        Write-Host "Removing $hadoopHome (-Force)"
        Remove-Item -LiteralPath $hadoopHome -Recurse -Force
    }
    $url = "https://archive.apache.org/dist/hadoop/common/$name/$name.tar.gz"
    # Unique name avoids "file in use" when another download still holds the default temp path.
    $tarball = Join-Path ([System.IO.Path]::GetTempPath()) "$name-$PID-$(Get-Random).tar.gz"
    Write-Host "Downloading (large): $url"
    Write-Host "Destination: $InstallParent\$name"
    Invoke-WebRequest -Uri $url -OutFile $tarball -UseBasicParsing
    Write-Host 'Extracting tarball (may take a few minutes)...'
    tar -xzf $tarball -C $InstallParent
    Remove-Item -LiteralPath $tarball -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "Hadoop already extracted: $hadoopHome"
}

if (-not (Test-Path -LiteralPath $hadoopCmd)) {
    Write-Error "Expected $hadoopCmd after install; check -InstallParent and tarball layout."
}

if (-not $SkipWinutils) {
    if (-not (Test-Path -LiteralPath $winutilsExe)) {
        Write-Host 'Installing winutils for this Hadoop version...'
        & $winutilsScript -SparkHome $hadoopHome -WinutilsHadoopVersion $Version
    } else {
        Write-Host "winutils.exe already present: $winutilsExe"
    }
}

Write-Host ''
Write-Host 'Verify:'
& $hadoopCmd version
Write-Host ''
Write-Host 'Set HADOOP_HOME for new shells:'
Write-Host "  setx HADOOP_HOME `"$hadoopHome`""
Write-Host 'Current session:'
Write-Host "  `$env:HADOOP_HOME = '$hadoopHome'"
Write-Host ''
Write-Host 'On Spark workers, set the same HADOOP_HOME (or EXECUTOR_HADOOP_HOME in run_pipeline_windows.ps1).'
