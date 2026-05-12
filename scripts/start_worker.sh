#!/usr/bin/env bash
# Conecta este nodo como Worker al master del equipo.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-$ROOT/conf}"

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
if [[ ! -x "$SPARK_HOME/sbin/start-worker.sh" ]]; then
  echo "No encuentro start-worker.sh en SPARK_HOME=$SPARK_HOME" >&2
  echo "Exportá SPARK_HOME apuntando a tu instalación de Spark 3.5.1." >&2
  exit 1
fi

if [[ -z "${SPARK_MASTER_URL:-}" ]]; then
  echo "Definí la URL del master (misma IP que SPARK_MASTER_HOST del master, puerto 7077):" >&2
  echo "  export SPARK_MASTER_URL=spark://192.168.x.x:7077" >&2
  echo "Luego: bash scripts/start_worker.sh" >&2
  exit 1
fi

echo "SPARK_HOME=$SPARK_HOME"
echo "SPARK_MASTER_URL=$SPARK_MASTER_URL"
echo "SPARK_CONF_DIR=$SPARK_CONF_DIR"
exec "$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL"
