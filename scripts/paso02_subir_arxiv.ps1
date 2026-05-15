# Paso 2: subir arxiv-metadata-oai-snapshot.json a MinIO (bucket semantic-raw)
param(
    [string]$SourcePath = "C:\Users\crism\Downloads\archive (2)\arxiv-metadata-oai-snapshot.json",
    [string]$S3Key = "arxiv-metadata-oai-snapshot.json",
    [string]$Endpoint = "http://127.0.0.1:9000",
    [string]$Bucket = "semantic-raw",
    [string]$AccessKey = "admin",
    [string]$SecretKey = "admin12345",
    [switch]$SkipIfSameSize
)

$ErrorActionPreference = "Stop"
$Repo = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path -LiteralPath $SourcePath)) {
    throw "No existe el archivo: $SourcePath"
}

$local = Get-Item -LiteralPath $SourcePath
Write-Host "Origen: $($local.FullName) ($([math]::Round($local.Length/1GB, 2)) GB)" -ForegroundColor Cyan

$py = Join-Path $Repo ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }

& $py -c @"
import sys
from pathlib import Path
import boto3
from botocore.client import Config

src = Path(r'''$($local.FullName)''')
endpoint = '''$Endpoint'''
bucket = '''$Bucket'''
key = '''$S3Key'''
skip_same = $($SkipIfSameSize.IsPresent)

client = boto3.client(
    's3',
    endpoint_url=endpoint,
    aws_access_key_id='''$AccessKey''',
    aws_secret_access_key='''$SecretKey''',
    config=Config(signature_version='s3v4'),
)
local_size = src.stat().st_size
try:
    h = client.head_object(Bucket=bucket, Key=key)
    remote = h['ContentLength']
    print(f'Remoto actual: {remote:,} bytes')
    if skip_same and remote == local_size:
        print('SKIP: mismo tamaño, no se re-sube.')
        sys.exit(0)
except client.exceptions.ClientError:
    pass

print('Subiendo (puede tardar varios minutos)...')
from boto3.s3.transfer import TransferConfig
cfg = TransferConfig(multipart_threshold=64 * 1024 * 1024, multipart_chunksize=64 * 1024 * 1024)
client.upload_file(str(src), bucket, key, Config=cfg)
h2 = client.head_object(Bucket=bucket, Key=key)
print(f'OK s3://{bucket}/{key} — {h2[\"ContentLength\"]:,} bytes')
"@

if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Siguiente: .\scripts\paso03_smoke_cluster.ps1" -ForegroundColor Yellow
