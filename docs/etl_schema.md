# Esquema de salida — Fase 1 (`etl_features.py`)

Salida por defecto (convención del PDF del curso):

`data/features/run_date=<YYYY-MM-DD>/`


(varios `part-*.csv.gz` + `_SUCCESS`, cabecera en la primera línea).

Opcionalmente se puede duplicar la misma tabla en MinIO con `--output-s3a s3a://.../run_date=.../`.

## Columnas (tabla de features)

| Columna | Tipo (CSV) | Descripción |
|---------|------------|-------------|
| `doc_id` | string | Identificador estable del documento: `id` del JSON. |
| `chunk_id` | entero | Índice del fragmento **dentro del documento** (0…N). Se genera segmentando `raw_text` por **tokens** del tokenizer del modelo de embeddings (`--tokenizer-model`, default `sentence-transformers/all-MiniLM-L6-v2`), con ventana `--max-tokens-per-chunk` (default 256) y solapamiento `--overlap-tokens` (default 32). Chunks con < `--min-chunk-tokens` (default 30) o < `--min-chunk-chars` (default 30) se descartan, salvo que el documento sólo produzca uno corto: en ese caso se conserva íntegro para no perderlo. Con `--word-tokenization` se cuenta por palabras (offline). |
| `title` | string | Título del documento (campo `title` del JSON). |
| `source_uri` | string | URI lógica de la fuente: glob/path del JSON de entrada (`--input-json-glob`). |
| `raw_text` | string | Texto del **chunk** ya limpio (normalización Unicode y colapso de espacios). Equivale al cuerpo que consumirá la Fase 2. |
| `ingestion_date` | string (`YYYY-MM-DD`) | Fecha de ingesta al pipeline, igual a `--run-date`. |

## Entradas soportadas

**JSONL(.gz)** vía `--input-json-glob` (Spark `read.json`): el esquema por defecto es `id STRING, text STRING, title STRING` (`--input-schema`). Si el JSON usa otros nombres, el script intenta mapear automáticamente entre estos candidatos:

- `doc_id` ← `doc_id` | `id` | `arxiv_id` | `paper_id`
- `raw_text` ← `raw_text` | `abstract` | `text` | `body` | `content`
- `title` ← `title` (opcional)

## Limpieza

- Se filtran filas con `raw_text` nulo y con `length(raw_text) <= 50`.
- `regexp_replace` colapsa espacios/saltos y elimina caracteres no ASCII.
- A nivel chunk, los textos se reconstruyen a partir de los offsets de tokens del tokenizer (preservan la puntuación original).

## Decisión de chunking (sección 5.4 del PDF)

- **Tokenizer:** el mismo del modelo de Fase 2 (`all-MiniLM-L6-v2`), para que el conteo de tokens sea el real del modelo. Se desactiva la truncation interna del tokenizer para ver todo el documento.
- **Tamaño / overlap:** 256 / 32 (parametrizables vía `--max-tokens-per-chunk` y `--overlap-tokens`, defaults desde `conf/config.yaml` → `model.max_tokens` / `model.overlap_tokens`).
- **Documentos cortos:** si el documento entero tiene menos tokens que `--min-chunk-tokens` se conserva como un único chunk (se evita perder información).
- **Modo offline:** `--word-tokenization` cuenta por palabras (no descarga modelo HF); menos preciso pero útil sin red.
