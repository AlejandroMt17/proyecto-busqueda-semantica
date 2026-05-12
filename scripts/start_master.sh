
#!/bin/bash
export SPARK_MASTER_HOST=10.84.18.85
$SPARK_HOME/sbin/start-master.sh
echo "[INFO] Master levantado en spark://10.84.18.85:7077"
echo "[INFO] UI disponible en http://10.84.18.85:8080"