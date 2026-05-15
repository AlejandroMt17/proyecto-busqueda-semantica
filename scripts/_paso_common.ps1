function Get-PasoRepo {
    return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Get-PasoPython {
    param([string]$Repo = (Get-PasoRepo))
    $venv = Join-Path $Repo ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venv) { return $venv }
    return "python"
}

function Invoke-PasoConfigRaw {
    param(
        [ValidateSet("host", "run_date", "bucket", "endpoint", "executor_python")]
        [Parameter(Mandatory = $true)]
        [string[]]$Keys
    )
    $reader = Join-Path $PSScriptRoot "_read_config.py"
    $py = Get-PasoPython
    $out = & $py $reader @Keys 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo leer config.yaml: $($out -join [Environment]::NewLine)"
    }
    return @($out | ForEach-Object { "$_".Trim() })
}

function Get-PasoConfigValue {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("host", "run_date", "bucket", "endpoint", "executor_python")]
        [string]$Key
    )
    $line = (Invoke-PasoConfigRaw -Keys $Key | Select-Object -First 1)
    return "$line".Trim()
}

function Get-PasoConfigMap {
    $keys = @("host", "run_date", "bucket", "endpoint")
    $lines = Invoke-PasoConfigRaw -Keys $keys
    if ($lines.Count -lt $keys.Count) {
        throw "config.yaml devolvio $($lines.Count) valores, se esperaban $($keys.Count). Salida: $($lines -join ' | ')"
    }
    return [ordered]@{
        host     = $lines[0]
        run_date = $lines[1]
        bucket   = $lines[2]
        endpoint = $lines[3]
    }
}

function Get-PasoNetworkHost {
    $lanIp = Get-PasoConfigValue -Key host
    if (-not $lanIp) {
        if ($env:SEMANTIC_SEARCH_HOST) { $lanIp = $env:SEMANTIC_SEARCH_HOST.Trim() }
        elseif ($env:SPARK_MASTER_HOST) { $lanIp = $env:SPARK_MASTER_HOST.Trim() }
    }
    if (-not $lanIp) {
        throw "Defini network.host en conf/config.yaml (Wi-Fi, ej. 192.168.100.26) o SEMANTIC_SEARCH_HOST"
    }
    return $lanIp
}

function Get-PasoSparkSubmitPythonConfs {
    param([string]$Repo = (Get-PasoRepo))
    $venvPy = Get-PasoPython -Repo $Repo
    if (Test-Path -LiteralPath $venvPy) {
        $driverPy = (Resolve-Path -LiteralPath $venvPy).Path
    } else {
        $driverPy = "python"
    }
    $executorPy = if ($env:SPARK_EXECUTOR_PYTHON) {
        $env:SPARK_EXECUTOR_PYTHON.Trim()
    } else {
        try { Get-PasoConfigValue -Key executor_python } catch { "python" }
    }
    if (-not $executorPy) { $executorPy = "python" }

    # Si PYSPARK_PYTHON apunta al venv del driver, los workers remotos fallan (ruta inexistente).
    Remove-Item Env:PYSPARK_PYTHON -ErrorAction SilentlyContinue
    $env:PYSPARK_DRIVER_PYTHON = $driverPy

    Write-Host "Driver Python:  $driverPy" -ForegroundColor Cyan
    Write-Host "Worker Python:  $executorPy  (misma ruta/comando en CADA worker)" -ForegroundColor Yellow

    return @(
        "--conf", "spark.pyspark.driver.python=$driverPy",
        # Ejecutores: comando en el PATH de cada worker (no ruta absoluta del PC del driver).
        "--conf", "spark.pyspark.python=$executorPy"
    )
}

function Get-PasoRunDate {
    param([string]$Override)
    if ($Override) { return $Override.Trim() }
    if ($env:RUN_DATE) { return $env:RUN_DATE.Trim() }
    $fromCfg = Get-PasoConfigValue -Key run_date
    if ($fromCfg) { return $fromCfg }
    return (Get-Date -Format "yyyy-MM-dd")
}
