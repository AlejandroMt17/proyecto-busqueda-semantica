# Arquitectura — Búsqueda semántica (Proyecto 1)

> Plantilla para el **documento de diseño** (Semana 1) y la **versión final** (Semana 4). Completá cada sección con decisiones reales del equipo y eliminá los bloques “TODO”.

## 1. Problema de negocio

- **Qué** indexamos semanalmente (fuentes: PDF/HTML/JSONL, MinIO, etc.).
- **Para qué** sirve la búsqueda semántica frente a búsqueda por keywords.
- **Quién** consume el índice (API, demo curl, otro).

TODO: 2–3 párrafos.

## 2. Vista de alto nivel

```text
[Fuentes crudas] → [Fase 1 ETL Spark] → CSV features run_date=…
                        ↓
[Fase 2 Inferencia Spark + Sentence-Transformers] → CSV/Parquet predicciones run_date=…
                        ↓
[Fase 3 Persistencia Spark] → Elasticsearch (dense_vector, kNN)
```

TODO: diagrama (draw.io, Excalidraw o imagen exportada) con IPs: master, workers, MinIO, ES.

## 3. Infraestructura y cluster

| Nodo | Rol | IP | Notas |
|------|-----|-----|--------|
| | Master + driver | | |
| | Worker | | |
| | Worker | | |
| | MinIO / Docker | | Puerto 9000 |
| | Elasticsearch | | Puerto 9200 |

- Versión **Java**, **Spark**, **Python** alineadas en todos los nodos.
- Puertos firewall abiertos: 7077, 8080, 8081, 4040, 9000, 9200, …

TODO: tabla completa.

## 4. Datos

- Criterio de volumen cumplido: ≥ 500k documentos **y/o** estrategia (semilla, sintético, `generate_data.py --seed`).
- Ubicación canónica: buckets/prefijos MinIO (`semantic-raw`, …).
- Formato crudo y convención de paths S3A.

TODO.

## 5. Fase 1 — ETL

- Extracción de texto (Tika opcional, fallback).
- Chunking: tamaño máximo, overlap, documentos cortos.
- Particiones Spark y rutas de salida `data/features/run_date=…`.
- Validaciones de calidad (`validate_etl_quality.py`).

TODO: enlazar con `docs/etl_schema.md`.

## 6. Fase 2 — Inferencia

- Modelo (`all-MiniLM-L6-v2` u otro): por qué, dimensiones, idioma.
- Patrón **Pandas UDF** / carga del modelo una vez por partición (no inferencia solo en driver).
- Recursos: `--executor-memory`, `--total-executor-cores`, `num_partitions`.

TODO.

## 7. Fase 3 — Persistencia

- Elasticsearch: versión, `dense_vector`, similaridad coseno.
- **Idempotencia**: upsert por `chunk_id`, re-ejecución misma `run_date`.
- Shards / réplicas (si aplica) y decisión de reindexado vs incremental.

TODO.

## 8. Configuración y operación

- `conf/config.yaml` y variables de entorno (`SEMANTIC_SEARCH_HOST`, `RUN_DATE`, …).
- Cómo levantar MinIO, ES, Tika (`docker compose`).
- Cómo arrancar master/workers y pipeline (`run_pipeline_windows.ps1`).

TODO: comandos concretos que el equipo usa en el lab.

## 9. Riesgos y mitigaciones

| Riesgo | Mitigación |
|--------|------------|
| Workers no ven MinIO | IP LAN + firewall 9000 |
| Driver host no enrutable | `spark.driver.host` = IP del driver |
| OOM en inferencia | Reducir batch / particiones / memoria executor |

TODO: ampliar.

## 10. Plan semanas 2–4

- Semana 2: …
- Semana 3: …
- Semana 4: …

TODO: responsables y hitos.
