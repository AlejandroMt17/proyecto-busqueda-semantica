#!/bin/bash
MASTER=${1:-"spark://10.84.18.85:7077"}
$SPARK_HOME/sbin/start-worker.sh $MASTER --cores 4 --memory 6g
echo "[INFO] Worker conectado a $MASTER"
