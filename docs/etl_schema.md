# Esquema de salida — Fase 1 (`etl_features.py`)

El ETL escribe **CSV comprimido con gzip** (varios `part-*.csv.gz` + `_SUCCESS`) bajo:

`s3a://semantic-raw/features/run_date=<YYYY-MM-DD>/`

(con `--output-dir` se puede cambiar la base).

## Columnas

| Columna     | Tipo (CSV) | Descripción |
|------------|------------|-------------|
| `doc_id`   | string     | Identificador del documento fuente (coincide con `id` del JSONL de entrada). |
| `chunk_id` | entero     | Índice del fragmento dentro del documento (0..N). Un documento puede generar varios chunks. |
| `title`    | string     | Título del documento (metadato para trazabilidad / UI). |
| `text_chunk` | string   | Texto normalizado y recortado a ~`chunk_max_chars` caracteres (sin solapamiento entre chunks). |
| `run_date` | string     | Fecha de corte del proceso (`--run-date`, formato `YYYY-MM-DD`). |
| `text_len` | entero     | Longitud en caracteres de `text_chunk` (control de calidad y límites de modelo). |

## Notas

- Primera fila del CSV: **cabecera** (`header=true`).
- La entrada esperada es JSONL gzip con campos `id`, `title`, `text` (como genera `scripts/generate_data.py`).
