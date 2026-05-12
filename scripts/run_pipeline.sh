#!/usr/bin/env bash
# Orquesta Fase 1 + Fase 2 (Fase 3 pendiente). Uso: bash scripts/run_pipeline.sh 2026-05-12 [spark_master_url]
set -euo pipefail

RUN_DATE="${1:?Usage: $0 YYYY-MM-DD [spark://IP:7077]}"
MASTER="${2:-local[*]}"
PKG="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:-}"

spark-submit --packages "$PKG" \
  "$ROOT/src/etl_features.py" \
  --run-date "$RUN_DATE" \
  --master "$MASTER" \
  --s3-endpoint "${S3_ENDPOINT:-http://127.0.0.1:9000}"

spark-submit --packages "$PKG" \
  "$ROOT/src/spark_vectorizer.py" \
  --run-date "$RUN_DATE" \
  --master "$MASTER" \
  --s3-endpoint "${S3_ENDPOINT:-http://127.0.0.1:9000}"

echo "Pipeline Fase 1+2 completado para run_date=$RUN_DATE"
