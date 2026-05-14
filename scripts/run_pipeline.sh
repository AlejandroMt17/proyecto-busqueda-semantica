#!/usr/bin/env bash
# Orquesta Fase 1 → Fase 2 → Fase 3 (manual integrador).
# Uso: bash scripts/run_pipeline.sh YYYY-MM-DD [spark_master_url]
set -euo pipefail

RUN_DATE="${1:?Usage: $0 YYYY-MM-DD [spark://IP:7077]}"
MASTER="${2:-local[*]}"
PKG_S3="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
PKG_ES="org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RAW_JSON="${RAW_JSON:-$ROOT/data/raw/arxiv_sample.jsonl}"

spark-submit --packages "$PKG_S3" \
  "$ROOT/src/etl_features.py" \
  --run-date "$RUN_DATE" \
  --master "$MASTER" \
  --input-json-glob "$RAW_JSON" \
  --s3-endpoint "${S3_ENDPOINT:-http://127.0.0.1:9000}"

spark-submit --packages "$PKG_S3" \
  "$ROOT/src/batch_inference.py" \
  --run-date "$RUN_DATE" \
  --master "$MASTER" \
  --s3-endpoint "${S3_ENDPOINT:-http://127.0.0.1:9000}"

spark-submit --packages "${PERSISTENCE_PACKAGES:-$PKG_ES}" \
  "$ROOT/src/persistence.py" \
  --run-date "$RUN_DATE" \
  --master "$MASTER" \
  --es-host "${ES_HOST:-127.0.0.1}"

echo "Pipeline Fase 1+2+3 completado para run_date=$RUN_DATE"
