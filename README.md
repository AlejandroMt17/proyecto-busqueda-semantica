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
- MinIO (almacenamiento S3 local)
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
│   └── embeddings/       # Salida Fase 2 (Parquet: vectores 384d)
├── models/               # (opcional) modelo cacheado HF
├── src/
│   ├── etl_features.py   # Fase 1 — ETL de documentos
│   ├── spark_vectorizer.py   # Fase 2 — Pandas UDF + SentenceTransformer
│   ├── batch_inference.py    # Alias de entrada a Fase 2 (mismo código)
│   └── persistence.py    # Fase 3 — Indexado en Elasticsearch (pendiente)
├── tests/
│   ├── test_etl.py
│   └── test_vectorizer.py
├── scripts/
│   ├── generate_data.py        # Dataset sintético + carga a MinIO
│   ├── validate_etl_quality.py# Chequeos de calidad salida Fase 1
│   ├── start_master.sh
│   ├── start_worker.sh
│   ├── start_minio_windows.ps1
│   ├── run_etl_windows.ps1     # Ejemplo spark-submit ETL (Windows)
│   ├── run_vectorizer_windows.ps1
│   └── run_pipeline.sh         # Fase 1 + Fase 2 (bash)
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

#### Qué debe tener listo cada worker (PySpark + MinIO)

1. **Java 17** y **Apache Spark 3.5.1** alineados con el master.
2. Red: salida hacia el master en **7077** (y tráfico de vuelta del master/worker según firewall).
3. **MinIO en red**: el ETL usa **S3A**. Si MinIO está en el PC del master con Docker, usá la **IP LAN del master** en `--s3-endpoint` (ej. `http://10.84.18.85:9000`), no `127.0.0.1` desde otra máquina. En el firewall del host donde corre Docker, permitir **9000** desde la LAN del laboratorio.
4. **Python**: con `spark-submit` en modo habitual (*client*), el driver ejecuta el Python del nodo donde lanzás el job; igual conviene **misma versión mayor (p. ej. 3.10)** en el equipo para evitar incompatibilidades.
5. **No** copiar el dataset al disco del worker: los executors leen/escriben **MinIO** por red.

---

## Semana 2 — Fase 1: ETL (`src/etl_features.py`)

Cumple el enunciado típico: **parámetros** (`--run-date`, rutas S3A, `--master`, `--s3-endpoint`, `--driver-host`), **logging** `INFO` al inicio y al cierre (con conteos y duración en segundos) y **`ERROR` + traceback** ante fallos.

### Ejecutar el ETL (master como driver, MinIO en el master)

Levantá también **Tika** si querés extracción PDF/DOCX por servidor (opcional; hay fallback local):

```powershell
docker compose up -d
```

Desde la raíz del repo (ajustá IP y fecha). Salida por defecto: **`data/features/run_date=<fecha>/`** (PDF). Copia opcional a MinIO con `--output-s3a`.

```powershell
$env:SPARK_HOME = "C:\spark"
$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkg `
  src/etl_features.py `
  --run-date 2026-05-12 `
  --master spark://10.84.18.85:7077 `
  --s3-endpoint http://10.84.18.85:9000 `
  --driver-host 10.84.18.85 `
  --tika-endpoint http://10.84.18.85:9998 `
  --output-s3a s3a://semantic-raw/features/run_date=2026-05-12/
```

**Solo tu PC** (MinIO y Tika en `localhost`, salida local):

```powershell
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkg `
  src/etl_features.py `
  --run-date 2026-05-12 `
  --master "local[*]" `
  --s3-endpoint http://127.0.0.1:9000 `
  --tika-endpoint http://127.0.0.1:9998
```

**Archivos crudos en MinIO** (PDF/HTML/…): subilos bajo un prefijo, por ejemplo `s3a://semantic-raw/upload/...` y ejecutá con:

`--input-files-glob "s3a://semantic-raw/upload/**/*"`

Plantilla equivalente: `scripts/run_etl_windows.ps1` (editá variables al inicio).

**Informe de ejecución (entregable):** guardá la salida de consola (tiempo `duración_s`, `leídos`, `generados`) y una **captura de la Spark UI** (aplicación en RUNNING / COMPLETED y vista de Executors).

### Esquema del CSV generado

Ver `docs/etl_schema.md` (nombre, tipo y propósito de cada columna).

### Prueba de calidad

Tras el ETL (lectura desde **carpeta local** generada por defecto):

```powershell
$env:SPARK_HOME = "C:\spark"
$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
$feat = "D:\Users\crism\OneDrive\Documentos\GitHub\proyecto-busqueda-semantica\data\features\run_date=2026-05-12"
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkg `
  scripts/validate_etl_quality.py `
  --input-glob ($feat -replace "\\", "/") `
  --expected-min-rows 1000
```

*(Ajustá la ruta `run_date=...`; si usás solo `file://`, escapá según Spark en Windows.)*

Desde **S3A**:

```powershell
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkg `
  scripts/validate_etl_quality.py `
  --input-glob "s3a://semantic-raw/features/run_date=2026-05-12/*.csv" `
  --s3-endpoint http://10.84.18.85:9000 `
  --expected-min-rows 1000
```

### Demo en clase (15 min)

Misma línea de `spark-submit` con la **fecha de corte** pedida por el docente, Spark UI abierta mientras corre, y en MinIO / descarga parcial mostrar un **CSV** de salida (cabecera + algunas filas).

### Actualización del documento técnico

Registrar en `docs/arquitectura.md` decisiones de la Fase 1 (lectura S3A, chunking, particiones de salida, riesgos y mitigaciones).

### Tests unitarios (Fase 1 y utilidades Fase 2)

```powershell
pip install -r requirements.txt
python -m pytest tests/ -q
```

---

## Fase 2 — Embeddings (`src/spark_vectorizer.py`)

Lee `data/features/run_date=<fecha>/` (salida del ETL) y escribe **`data/embeddings/run_date=<fecha>/`** en **Parquet** con columnas `chunk_key`, `doc_id`, `chunk_id`, metadatos opcionales y `embedding` (lista de 384 `float`).

La inferencia usa **Pandas UDF** en modo *Scalar Iterator*: el modelo `sentence-transformers/all-MiniLM-L6-v2` se carga **una vez por partición** en cada executor (requisito del enunciado: evitar `.toPandas()` masivo en el driver).

**Dependencias en cada nodo que ejecute tareas:** el mismo `pip install -r requirements.txt` (incluye `sentence-transformers` y `torch`).

Solo PC local (Fase 1 ya generó CSV bajo `data/features/...`):

```powershell
$env:SPARK_HOME = "C:\spark"
$pkg = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkg `
  src/spark_vectorizer.py `
  --run-date 2026-05-12 `
  --master "local[*]" `
  --s3-endpoint http://127.0.0.1:9000
```

Con datos en MinIO (ajustá bucket/prefijo al tuyo):

```powershell
& "$env:SPARK_HOME\bin\spark-submit.cmd" --packages $pkg `
  src/spark_vectorizer.py `
  --run-date 2026-05-12 `
  --master spark://10.84.18.85:7077 `
  --driver-host 10.84.18.85 `
  --input-glob "s3a://semantic-raw/features/run_date=2026-05-12/*.csv" `
  --output-s3a "s3a://semantic-raw/embeddings/run_date=2026-05-12/" `
  --s3-endpoint http://10.84.18.85:9000
```

Plantilla: `scripts/run_vectorizer_windows.ps1`.

---

## Cómo ejecutar el pipeline completo

Variables opcionales: `S3_ENDPOINT` (default `http://127.0.0.1:9000`).

```bash
bash scripts/run_pipeline.sh 2026-05-10
bash scripts/run_pipeline.sh 2026-05-10 spark://192.168.1.100:7077
```

Encadena **Fase 1 (ETL)** y **Fase 2 (embeddings)**. La **Fase 3** (persistencia en Elasticsearch) se añadirá cuando exista `persistence.py`.

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
cd D:\Users\crism\OneDrive\Documentos\GitHub\proyecto-busqueda-semantica
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
