# Esquema de salida — Fase 1 (`etl_features.py`)

Salida por defecto (convención del PDF del curso):

`data/features/run_date=<YYYY-MM-DD>/`


(varios `part-*.csv.gz` + `_SUCCESS`, cabecera en la primera línea).

Opcionalmente se puede duplicar la misma tabla en MinIO con `--output-s3a s3a://.../run_date=.../`.

## Columnas (tabla de featuress) 
## toris


| Columna | Tipo (CSV) | Descripción |
|---------|------------|-------------|
| `doc_id` | string | Identificador estable del documento: `id` del JSON o *hash* SHA1 truncado de la ruta S3/local del archivo binario. |
| `chunk_id` | entero | Índice del fragmento (0…N) tras segmentar por **tokens** con solapamiento (`--overlap-tokens`, default 32), ventana máxima `--max-tokens-per-chunk` (default 256) y el mismo tokenizer que el modelo de embeddings; fallback por caracteres si falla el tokenizer. Los trozos más cortos que `--min-chunk-chars` (default 30) se descartan salvo que no quede ninguno (en cuyo caso se conservan los no vacíos). |
| `title` | string | Título del documento (metadato JSON) o nombre de archivo inferido de la ruta. |
| `source_uri` | string | URI lógica de la fuente: campo `source_uri` del JSON si existe; si no, prefijo sintético; en archivos binarios la ruta `path` devuelta por `binaryFile`. |
| `raw_text` | string | Texto del **chunk** ya limpio (NFKC, espacios, boilerplate simple removido). Equivale al cuerpo que consumirá la Fase 2. |
| `ingestion_date` | string (`YYYY-MM-DD`) | Fecha de ingesta al pipeline; por defecto `--ingestion-date` o `--run-date`. |

## Entradas soportadas :

1. **JSONL(.gz)** vía `--input-json-glob` (Spark `read.json`): campos mínimos `id`, `title`, `text`; opcionales `source_uri`, `source_updated` (para `--since-date`).
2. **Archivos** vía `--input-files-glob` y `binaryFile`: `.pdf`, `.docx`, `.html`/`.htm`, `.txt`/`.md` (y otros con Tika si está activo).

## Extracción y limpieza

- **PDF/DOCX**: primero **Apache Tika** (`--tika-endpoint`, p. ej. `http://127.0.0.1:9998` con `docker compose`); si falla, **pypdf** / **python-docx**.
- **HTML**: **BeautifulSoup** + eliminación de `script`, `style`, `nav`, etc.
- **Limpieza**: normalización **Unicode NFKC**, colapso de espacios/saltos, líneas largas de separadores, frases tipo *confidential* genéricas.

## Reproducibilidad incremental

Si los JSON incluyen `source_updated`, podés filtrar con `--since-date YYYY-MM-DD` para aproximar “documentos nuevos desde la última corrida”.
