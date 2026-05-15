# Proyecto Integrador — Búsqueda Semántica con Apache Spark

## Descripción
Sistema de búsqueda semántica que indexa documentos semanalmente usando embeddings
distribuidos con Apache Spark y los almacena en Elasticsearch para consultas por similitud.

A diferencia de la búsqueda por palabras clave, este sistema permite encontrar documentos
por significado: una consulta como "problemas de motor" puede traer documentos que hablen
de "falla de combustión" aunque no compartan palabras exactas.

---

## Equipo
| Nombre | Rol |
|---|---|
| Integrante 1 | Líder técnico / Cluster |
| Integrante 2 | ETL y datos |
| Integrante 3 | Inferencia y persistencia |

---

## Tecnologías utilizadas
- Apache Spark 3.5.1 (PySpark)
- Python 3.10+
- Sentence-Transformers (all-MiniLM-L6-v2)
- Elasticsearch 8.13
- MinIO (S3 compatible, vía API en la red del laboratorio)
- Docker

---

## Requisitos previos
En cada laptop del equipo debe estar instalado:
- Java 17
- Python 3.10+
- Apache Spark 3.5.1 descomprimido en `/opt/spark`
- Docker

Instalar dependencias Python:
pip install pyspark==3.5.1 pandas numpy sentence-transformers
pip install -r requirements.txt

---

## Estructura del proyecto
proyecto/
├── README.md
├── conf/
│   └── spark-defaults.conf
├── data/
│   ├── raw/              # Documentos crudos descargados
│   ├── features/         # Salida Fase 1 (CSV gzip: chunks)
│   └── predictions/      # Salida Fase 2 (CSV: chunk_id, doc_id, embedding_json, …)
├── models/               # (opcional) modelo cacheado HF
├── src/
│   ├── etl_features.py   # Fase 1 — ETL de documentos
│   ├── batch_inference.py   # Fase 2 — Pandas UDF + SentenceTransformer (entrada principal)
│   ├── spark_vectorizer.py  # Compat: reexporta batch_inference
│   └── persistence.py    # Fase 3 — Indexado en Elasticsearch (upsert por chunk_id)
├── tests/
│   ├── test_etl.py
│   ├── test_vectorizer.py
│   └── test_persistence.py
├── scripts/
│   ├── generate_data.py        # Dataset sintético + carga a MinIO
│   ├── validate_etl_quality.py# Chequeos de calidad salida Fase 1
│   ├── start_master.sh
│   ├── start_worker.sh
│   ├── start_master_windows.ps1
│   ├── start_worker_windows.ps1
│   ├── smoke_cluster_windows.ps1  # SparkPi — evidencia cluster Semana 1
│   ├── start_minio_windows.ps1
│   ├── run_etl_windows.ps1     # Ejemplo spark-submit ETL (Windows)
│   ├── run_vectorizer_windows.ps1
│   ├── run_pipeline.sh         # Fase 1 + Fase 2 + Fase 3 (bash)
│   └── run_pipeline_windows.ps1
├── notebooks/            # Exploración (no es entregable principal)
└── docs/
    ├── arquitectura.md   # Diagrama y decisiones técnicas
    └── etl_schema.md     # Columnas de salida Fase 1

---

## Cómo levantar el cluster

### En la laptop Master:
export SPARK_MASTER_HOST=192.168.1.100
bash scripts/start_master.sh

Verificar en el navegador: http://192.168.1.100:8080
Deben aparecer los workers conectados.

**Job trivial para la demo Semana 1 (SparkPi + capturas UI):** con workers visibles, desde el driver (`SEMANTIC_SEARCH_HOST` = IP de esa máquina) ejecutá `.\scripts\smoke_cluster_windows.ps1 -MasterHost "192.168.1.100"`.

### En cada laptop Worker

**Linux / WSL / Git Bash:**

```bash
export SPARK_HOME=/ruta/a/spark-3.5.1-bin-hadoop3
export SPARK_MASTER_URL=spark://10.84.18.85:7077   # IP del MASTER + 7077
bash scripts/start_worker.sh
```

**Windows (CMD), misma familia Spark 3.5.x que el master:**

```cmd
set SPARK_HOME=C:\spark
set SPARK_MASTER_URL=spark://10.84.18.85:7077
%SPARK_HOME%\sbin\start-worker.cmd %SPARK_MASTER_URL%
```

**Windows (PowerShell), con `SPARK_CONF_DIR` del repo y fallback Git Bash:**

```powershell
$env:SPARK_HOME = "C:\spark"
$env:SPARK_MASTER_URL = "spark://10.84.18.85:7077"
cd D:\ruta\al\repo\proyecto-busqueda-semantica
.\scripts\start_worker_windows.ps1
```

#### Qué debe tener listo cada worker (PySpark + MinIO)

1. **Java 17** y **Apache Spark 3.5.1** alineados con el master.
2. Red: salida hacia el master en **7077** (y tráfico de vuelta del master/worker según firewall).
3. **MinIO en red**: el ETL usa **S3A**. Si MinIO está en el PC del master con Docker, usá la **IP LAN del master** en `--s3-endpoint` (ej. `http://10.84.18.85:9000`), no `127.0.0.1` desde otra máquina. En el firewall del host donde corre Docker, permitir **9000** desde la LAN del laboratorio.
4. **Python**: con `spark-submit` en modo habitual (*client*), el driver ejecuta el Python del nodo donde lanzás el job; igual conviene **misma versión mayor (p. ej. 3.10)** en el equipo para evitar incompatibilidades.
5. **No** copiar el dataset al disco del worker: los executors leen/escriben **MinIO** por red.

#### Windows y S3A: `winutils.exe` (error “Could not locate Hadoop executable”)

No hace falta instalar un cluster Hadoop aparte. En **cada** PC Windows que ejecute workers (o el driver) con rutas `s3a://`, la JVM de Hadoop busca **`winutils.exe`** y **`hadoop.dll`** en `%HADOOP_HOME%\bin`; en la práctica `HADOOP_HOME` suele ser igual a `SPARK_HOME`.

Si en el log ves algo como `C:\spark351\bin\winutils.exe` en un worker (`192.168.56.1`, etc.), en **esa máquina** instalá los binarios en esa ruta:

```powershell
cd <RAIZ_DEL_REPO>
.\scripts\install_winutils_windows.ps1 -SparkHome 'C:\spark351'
```

(`-SparkHome` debe ser el **mismo** directorio raíz de Spark que usa ese worker en `SPARK_HOME`.) Repetí en cada Windows del equipo. Alternativa: unificar todos con el mismo `SPARK_HOME` (ej. `C:\spark`) y una sola instalación de winutils.

Documentación de fondo: [Hadoop on Windows](https://wiki.apache.org/hadoop/WindowsProblems).

---

## Semana 2 — Fase 1: ETL (`src/etl_features.py`)

Cumple el enunciado típico: **parámetros** (`--run-date`, rutas S3A, `--master`, `--s3-endpoint`, `--driver-host`), **logging** `INFO` al inicio y al cierre (con conteos y duración en segundos) y **`ERROR` + traceback** ante fallos.

### Ejecutar el ETL (cluster `spark://` + MinIO en IP LAN)

Levantá también **Tika** si querés extracción PDF/DOCX por servidor (opcional; si Tika no responde, el ETL usa librerías en el worker).

```powershell
docker compose up -d
```

Por defecto el ETL escribe features en **`s3a://<bucket>/features/run_date=<fecha>/`** (config `minio.bucket` + `network.host`). Ajustá IP, fecha y Tika.

```powershell
$env:SPARK_HOME = "C:\spark"
$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkg `
  src/etl_features.py `
  --run-date 2026-05-12 `
  --master spark://10.84.18.85:7077 `
  --s3-endpoint http://10.84.18.85:9000 `
  --driver-host 10.84.18.85 `
  --tika-endpoint http://10.84.18.85:9998
```

**Archivos crudos en MinIO** (PDF/HTML/…): subilos bajo un prefijo, por ejemplo `s3a://semantic-raw/upload/...` y ejecutá con:

`--input-files-glob "s3a://semantic-raw/upload/**/*"`

Plantilla equivalente: `scripts/run_etl_windows.ps1` (editá variables al inicio).

**Informe de ejecución (entregable):** guardá la salida de consola (tiempo `duración_s`, `leídos`, `generados`) y una **captura de la Spark UI** (aplicación en RUNNING / COMPLETED y vista de Executors).

### Esquema del CSV generado

Ver `docs/etl_schema.md` (nombre, tipo y propósito de cada columna).

### Prueba de calidad

Tras el ETL, validá la salida en **MinIO** (misma `run_date`) vía `spark-submit` al cluster:

```powershell
$env:SPARK_HOME = "C:\spark"
$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$ip = "10.84.18.85"
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkg `
  --master "spark://${ip}:7077" --driver-host $ip `
  scripts/validate_etl_quality.py `
  --input-glob "s3a://semantic-raw/features/run_date=2026-05-12/*.csv" `
  --s3-endpoint "http://${ip}:9000" `
  --expected-min-rows 1000
```

### Demo en clase (15 min)

Misma línea de `spark-submit` con la **fecha de corte** pedida por el docente, Spark UI abierta mientras corre, y en MinIO mostrar un **CSV** de salida (cabecera + algunas filas).

### Actualización del documento técnico

Registrar en `docs/arquitectura.md` decisiones de la Fase 1 (lectura S3A, chunking, particiones de salida, riesgos y mitigaciones).

### Tests unitarios (Fases 1–3 y utilidades)

```powershell
pip install -r requirements.txt
python -m pytest tests/ -q
```

---

## Fase 2 — Inferencia (`src/batch_inference.py`)

Lee por defecto **`s3a://…/features/run_date=<fecha>/`** y escribe **`s3a://…/predictions/run_date=<fecha>/`** en **CSV** con `embedding_json` (vector 384d serializado; Spark no escribe `ArrayType` en CSV de forma portable). Opcional: `--output-format parquet|both`.

La inferencia usa **Pandas UDF** en modo *Scalar Iterator*: el modelo (p. ej. `sentence-transformers/all-MiniLM-L6-v2`) se carga **una vez por partición** en cada executor (sin `.toPandas()` masivo en el driver).

**Dependencias en cada nodo que ejecute tareas:** el mismo `pip install -r requirements.txt` (incluye `sentence-transformers` y `torch`).

```powershell
$env:SPARK_HOME = "C:\spark"
$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkg `
  src/batch_inference.py `
  --run-date 2026-05-12 `
  --master spark://10.84.18.85:7077 `
  --driver-host 10.84.18.85 `
  --skip-stats `
  --validate-output `
  --s3-endpoint http://10.84.18.85:9000
```

*(Rutas `--input-glob` / `--output-s3a` son opcionales si coinciden con los defaults en bucket `semantic-raw`.)*

Plantilla: `scripts/run_vectorizer_windows.ps1`. `spark_vectorizer.py` sigue siendo punto de entrada compatible.

---

## Fase 3 — Persistencia (`src/persistence.py`)

Lee por defecto **`s3a://…/predictions/run_date=<fecha>/`**, crea el índice Elasticsearch con `dense_vector` si no existe y escribe con **upsert** por `chunk_id` (re-ejecución idempotente).

Requiere Elasticsearch en marcha (p. ej. `docker compose up -d`) en una IP alcanzable desde el driver y los executors, y el conector Ivy:

`org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0`

```powershell
$pkgEs = "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$ip = "10.84.18.85"
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkgEs `
  src/persistence.py --run-date 2026-05-12 `
  --master spark://${ip}:7077 --driver-host $ip `
  --es-host $ip --s3-endpoint http://${ip}:9000
```

---

## Cómo ejecutar el pipeline completo

El script orquesta las tres fases **solo en modo cluster** (`spark://` según `network.host` o `SEMANTIC_SEARCH_HOST`). Variables opcionales: `ES_HOST` (default: misma IP que `network.host`), `INPUT_JSON_GLOB`, `PERSISTENCE_PACKAGES`, `EXECUTOR_HADOOP_HOME`, `SPARK_EXECUTOR_PYTHON`.

```bash
export S3_ENDPOINT="http://192.168.1.100:9000"
export ES_HOST="192.168.1.100"
bash scripts/run_pipeline.sh 2026-05-10 spark://192.168.1.100:7077
```

En Windows (misma convención de IP):

```powershell
$env:SEMANTIC_SEARCH_HOST = "10.84.18.85"
$env:ES_HOST = "10.84.18.85"
cd D:\ruta\al\repo\proyecto-busqueda-semantica
.\scripts\run_pipeline_windows.ps1
```

(`RUN_DATE` sale de `config.yaml` si no definís `$env:RUN_DATE`.)

Encadena **Fase 1 (ETL)**, **Fase 2 (inferencia CSV en S3A)** y **Fase 3 (Elasticsearch)**.

---

## Dataset y MinIO (Semana 1 — almacenamiento)

Objetivo del enunciado: dataset **en MinIO** (API S3) con volumen mínimo y **generación reproducible** con semilla fija.

### 1) Levantar MinIO (Docker)

En la raíz del repo:

```powershell
docker compose up -d
```

- API S3: `http://127.0.0.1:9000`
- Consola web: `http://127.0.0.1:9001` (usuario `minioadmin`, clave `minioadmin123`)
- Los buckets `semantic-raw` y `semantic-tabular` los crea `scripts/generate_data.py` la primera vez que sube (API S3).

*(Claves solo para entorno local; no usar en producción.)*

### 1b) Sin Docker en Windows (`docker` no reconocido)

Instalá [Docker Desktop para Windows](https://www.docker.com/products/docker-desktop/) y reiniciá la sesión (o agregá Docker al `PATH`) para poder usar `docker compose`.

**Alternativa sin Docker:** en una consola PowerShell aparte (dejala abierta):

```powershell
cd C:\ruta\al\repo\proyecto-busqueda-semantica
.\scripts\start_minio_windows.ps1
```

La primera vez descarga `minio.exe` bajo `.tools\minio\`. Los buckets se crean solos al ejecutar `generate_data.py` (no hace falta `docker compose`).

### 2) Generar y subir datos (semilla fija)

Con MinIO arriba:

```powershell
pip install -r requirements.txt
python scripts/generate_data.py --seed 42
```

Por defecto genera **500.000 documentos** (JSONL comprimido en shards) y **10.000.000 de filas** CSV tabulares, y los sube a los buckets anteriores. Todo el muestreo depende solo de `--seed` (numpy `Generator`).

Para probar el ETL incremental (`--since-date` en `src/etl_features.py`), podés generar JSON con `source_updated` fija, por ejemplo: `python scripts/generate_data.py --seed 42 --source-updated 2026-05-12`.

Prueba rápida (pocos registros, **sin** MinIO):

```powershell
python scripts/generate_data.py --dry-run
```

Variables opcionales (equivalentes a flags): `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET_DOCS`, `MINIO_BUCKET_TABULAR`.

### 3) Wikipedia real (opcional, pipeline futuro)

- Fuente adicional: Wikipedia ES dump — https://dumps.wikimedia.org/eswiki/latest/
- Puede convivir con el dataset sintético reproducible; el script `scripts/generate_data.py` cubre el requisito de **semilla fija** y volumen mínimo sin depender del dump.

### Alternativa NFS

Si la consigna exige NFS en lugar de MinIO: montar el share en el nodo de datos y copiar allí los mismos artefactos generados localmente (misma semilla, mismos nombres de archivo). El script actual está orientado a S3/MinIO por simplicidad en Windows/Linux.

---

## Puertos requeridos
| Puerto | Nodo | Uso |
|---|---|---|
| 7077 | Master | Comunicación del cluster |
| 8080 | Master | Spark Master Web UI |
| 8081 | Workers | Spark Worker Web UI |
| 4040 | Driver | Spark Application UI |
| 9200 | Master | Elasticsearch API |
| 9000 | Master | MinIO S3 API |
| 9998 | Master (o host Docker) | Apache Tika Server (extracción de texto para el ETL) |

---

## Documentación
Ver `docs/arquitectura.md` para el diagrama completo y las decisiones técnicas del equipo.

---

## Commits del equipo (Semana 1 — requisito Git)

Cada integrante debe tener **al menos un commit** en el historial del repositorio.

Verificación (emails/nombres configurados en Git):

```powershell
git shortlog -sn -e --all
```

Si falta alguien: que haga un commit pequeño con su usuario Git configurado (`git config user.name` / `user.email`) en su máquina, por ejemplo una corrección en README con mensaje claro (`docs: ajuste sección X — Nombre Apellido`).
