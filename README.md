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

---

## Estructura del proyecto
proyecto/
├── README.md
├── conf/
│   └── spark-defaults.conf
├── data/
│   ├── raw/              # Documentos crudos descargados
│   ├── features/         # Salida Fase 1 (CSVs con chunks)
│   └── predictions/      # Salida Fase 2 (CSVs con embeddings)
├── models/               # Modelo serializado
├── src/
│   ├── etl_features.py   # Fase 1 — ETL de documentos
│   ├── batch_inference.py# Fase 2 — Inferencia de embeddings
│   └── persistence.py    # Fase 3 — Indexado en Elasticsearch
├── scripts/
│   ├── generate_data.py  # Descarga y prepara el dataset
│   ├── start_master.sh   # Levanta el nodo Master
│   ├── start_worker.sh   # Levanta un nodo Worker
│   └── run_pipeline.sh   # Orquesta las 3 fases
├── notebooks/            # Exploración (no es entregable principal)
└── docs/
    └── arquitectura.md   # Diagrama y decisiones técnicas

---

## Cómo levantar el cluster

### En la laptop Master:
export SPARK_MASTER_HOST=192.168.1.100
bash scripts/start_master.sh

Verificar en el navegador: http://192.168.1.100:8080
Deben aparecer los workers conectados.

### En cada laptop Worker:
bash scripts/start_worker.sh

---

## Cómo ejecutar el pipeline completo

bash scripts/run_pipeline.sh --run-date 2026-05-10

Esto ejecuta las 3 fases en secuencia:
1. ETL — extrae y limpia documentos, genera CSVs de features
2. Inferencia — genera embeddings distribuidos con Spark
3. Persistencia — indexa los vectores en Elasticsearch

---

## Dataset
- Fuente: Wikipedia ES dump
- URL: https://dumps.wikimedia.org/eswiki/latest/
- Volumen: 500,000+ documentos
- Instrucciones de descarga: ver scripts/generate_data.py

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

---

## Documentación
Ver `docs/arquitectura.md` para el diagrama completo y las decisiones técnicas del equipo.
