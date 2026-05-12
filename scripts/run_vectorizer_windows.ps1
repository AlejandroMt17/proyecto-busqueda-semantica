# Ejemplo Windows — Fase 2 (embeddings) con Spark
# Cada executor/worker necesita `sentence-transformers` y `torch` en el mismo Python que use PySpark.

$ErrorActionPreference = "Stop"
$Repo = "D:\Users\crism\OneDrive\Documentos\GitHub\proyecto-busqueda-semantica"
$env:SPARK_HOME = "C:\spark"
$MasterIP = "10.84.18.85"
$RunDate = "2026-05-12"

$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$app = Join-Path $Repo "src\spark_vectorizer.py"

& $submit `
  --packages $pkg `
  $app `
  --run-date $RunDate `
  --master "spark://$($MasterIP):7077" `
  --s3-endpoint "http://$($MasterIP):9000" `
  --driver-host $MasterIP `
  --input-glob "s3a://semantic-raw/features/run_date=$RunDate/*.csv" `
  --output-s3a "s3a://semantic-raw/embeddings/run_date=$RunDate/"
