#!/usr/bin/env bash
# Levanta el Spark Standalone Master en esta máquina (nodo master del equipo).
# Requisitos: Java 17, Spark 3.5.x en SPARK_HOME.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-$ROOT/conf}"

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
if [[ ! -x "$SPARK_HOME/sbin/start-master.sh" ]]; then
  echo "No encuentro start-master.sh en SPARK_HOME=$SPARK_HOME" >&2
  echo "Exportá SPARK_HOME apuntando a tu instalación de Spark 3.5.1." >&2
  exit 1
fi

if [[ -z "${SPARK_MASTER_HOST:-}" ]]; then
  echo "Definí la IP de ESTA máquina (la que verán los workers en la red):" >&2
  echo "  export SPARK_MASTER_HOST=192.168.x.x" >&2
  echo "Luego volvé a ejecutar: bash scripts/start_master.sh" >&2
  exit 1
fi

echo "SPARK_HOME=$SPARK_HOME"
echo "SPARK_MASTER_HOST=$SPARK_MASTER_HOST"
echo "SPARK_CONF_DIR=$SPARK_CONF_DIR"
exec "$SPARK_HOME/sbin/start-master.sh"
