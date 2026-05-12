#!/usr/bin/env python3
"""
Fase 1 — ETL de documentos (alineado al enunciado de búsqueda semántica).

- Fuentes: JSONL (MinIO/S3A o local) y/o archivos binarios (PDF, DOCX, HTML, TXT) vía binaryFile.
- Extracción: pypdf / python-docx / BeautifulSoup; Apache Tika (REST) si --tika-endpoint responde.
- Limpieza: NFKC, espacios, líneas repetitivas tipo boilerplate.
- Chunks: ~N tokens con solapamiento (overlap) y tokenizer HuggingFace (tokenizers); fallback por caracteres si falla la carga.
- Salida CSV (cabecera + gzip): doc_id, chunk_id, title, source_uri, raw_text, ingestion_date
  por defecto en data/features/run_date=YYYY-MM-DD/ (convención del PDF).

Ejemplo local (MinIO + Tika en localhost):

  docker compose up -d
  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 ^
    src/etl_features.py --run-date 2026-05-12 --master "local[*]" ^
    --s3-endpoint http://127.0.0.1:9000 --tika-endpoint http://127.0.0.1:9998
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import os
import re
import sys
import time
import traceback
import unicodedata
import urllib.parse
from pathlib import Path, PurePosixPath

import requests
from pyspark.sql import Row, SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("etl_features")

DEFAULT_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOKENIZER = "sentence-transformers/all-MiniLM-L6-v2"


def normalize_unicode(text: str) -> str:
    return unicodedata.normalize("NFKC", text or "")


def clean_boilerplate(text: str) -> str:
    t = normalize_unicode(text)
    t = re.sub(r"\r\n|\r", "\n", t)
    t = re.sub(r"[ \t\f\v]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    t = re.sub(r"(?:=|-|_){6,}", " ", t)
    t = re.sub(r"(?i)\b(confidential|internal use only|all rights reserved)\b", "", t)
    return t.strip()


def tika_extract(endpoint: str, data: bytes) -> str:
    url = f"{endpoint.rstrip('/')}/tika"
    r = requests.put(
        url,
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=180,
    )
    r.raise_for_status()
    return (r.text or "").strip()


def extract_pdf_bytes(data: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    parts = [p.extract_text() or "" for p in reader.pages]
    return "\n".join(parts)


def extract_docx_bytes(data: bytes) -> str:
    from docx import Document

    doc = Document(io.BytesIO(data))
    return "\n".join(p.text for p in doc.paragraphs if p.text)


def extract_html_bytes(data: bytes) -> str:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(data, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "noscript"]):
        tag.decompose()
    return soup.get_text(separator="\n")


def extract_txt_bytes(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def title_from_path(path: str) -> str:
    try:
        p = urllib.parse.urlparse(path.replace("s3a://", "http://dummy/"))
        tail = PurePosixPath(p.path).name
        return tail or "untitled"
    except Exception:
        return "untitled"


def extract_any(path: str, raw: bytes, tika_endpoint: str | None) -> tuple[str, str, str]:
    """Devuelve (title, texto_plano, tipo)."""
    title = title_from_path(path)
    low = path.lower()
    kind = "unknown"
    text = ""
    try:
        if low.endswith(".pdf"):
            kind = "pdf"
            if tika_endpoint:
                try:
                    text = tika_extract(tika_endpoint, raw)
                except Exception:
                    text = extract_pdf_bytes(raw)
            else:
                text = extract_pdf_bytes(raw)
        elif low.endswith(".docx"):
            kind = "docx"
            if tika_endpoint:
                try:
                    text = tika_extract(tika_endpoint, raw)
                except Exception:
                    text = extract_docx_bytes(raw)
            else:
                text = extract_docx_bytes(raw)
        elif low.endswith(".html") or low.endswith(".htm"):
            kind = "html"
            text = extract_html_bytes(raw)
        elif low.endswith(".txt") or low.endswith(".md") or low.endswith(".csv"):
            kind = "text"
            text = extract_txt_bytes(raw)
        else:
            kind = "bytes"
            if tika_endpoint:
                try:
                    text = tika_extract(tika_endpoint, raw)
                except Exception:
                    text = extract_txt_bytes(raw)
            else:
                text = extract_txt_bytes(raw)
    except Exception:
        text = ""
    return title, clean_boilerplate(text), kind


def chunk_by_chars(text: str, max_chars: int) -> list[str]:
    if not text:
        return []
    t = re.sub(r"\s+", " ", text).strip()
    if not t:
        return []
    out: list[str] = []
    start = 0
    while start < len(t):
        end = min(len(t), start + max_chars)
        if end < len(t):
            cut = t.rfind(" ", start, end)
            if cut == -1 or cut <= start:
                cut = end
            piece = t[start:cut].strip()
            start = cut + 1
        else:
            piece = t[start:end].strip()
            start = end
        if piece:
            out.append(piece)
    return out if out else [""]


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETL Fase 1 — documentos + features (Spark).")
    p.add_argument("--run-date", required=True, help="Fecha de corte YYYY-MM-DD (partición de salida).")
    p.add_argument(
        "--ingestion-date",
        default=None,
        help="Columna ingestion_date (YYYY-MM-DD). Por defecto igual a --run-date.",
    )
    p.add_argument(
        "--since-date",
        default=None,
        help="Si el JSON trae columna source_updated (YYYY-MM-DD), filtrar filas >= esta fecha.",
    )
    p.add_argument(
        "--master",
        default="local[*]",
        help='Spark master, ej. spark://10.84.18.85:7077 o "local[*]".',
    )
    p.add_argument(
        "--input-json-glob",
        default="s3a://semantic-raw/text/v1/*.jsonl.gz",
        help="Patrón JSONL (puede ser vacío si solo usás --input-files-glob).",
    )
    p.add_argument(
        "--input-files-glob",
        default=None,
        help="Patrón binaryFile (PDF/DOCX/HTML/TXT…), ej. s3a://semantic-raw/upload/**/*",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Salida principal (CSV gzip). Por defecto: carpeta local data/features/run_date=...",
    )
    p.add_argument(
        "--output-s3a",
        default=None,
        help="Si se define, escribe una copia adicional a esta URI s3a://.../run_date=.../",
    )
    p.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    p.add_argument("--s3-access-key", default="minioadmin")
    p.add_argument("--s3-secret-key", default="minioadmin123")
    p.add_argument(
        "--tika-endpoint",
        default=os.environ.get("TIKA_ENDPOINT", "http://127.0.0.1:9998"),
        help="Apache Tika server (docker compose service tika). Vacío '' para desactivar.",
    )
    p.add_argument("--driver-host", default=None)
    p.add_argument("--jars-packages", default=DEFAULT_PACKAGES)
    p.add_argument("--tokenizer-model", default=DEFAULT_TOKENIZER)
    p.add_argument("--max-tokens-per-chunk", type=int, default=256)
    p.add_argument(
        "--overlap-tokens",
        type=int,
        default=32,
        help="Solapamiento entre ventanas consecutivas (tokens), como en el enunciado (~32).",
    )
    p.add_argument(
        "--min-chunk-chars",
        type=int,
        default=30,
        help="Descarta chunks demasiado cortos tras decodificar (filtro tipo enunciado).",
    )
    return p.parse_args(argv)


def build_spark(args: argparse.Namespace) -> SparkSession:
    b = (
        SparkSession.builder.appName(f"etl_features_{args.run_date}")
        .master(args.master)
        .config("spark.jars.packages", args.jars_packages)
    )
    if args.driver_host:
        b = b.config("spark.driver.host", args.driver_host).config("spark.driver.bindAddress", "0.0.0.0")
    spark = b.getOrCreate()
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.path.style.access", "true")
    ep = args.s3_endpoint.strip()
    if ep.startswith("http://"):
        ep = ep[len("http://") :]
        hconf.setBoolean("fs.s3a.connection.ssl.enabled", False)
    elif ep.startswith("https://"):
        ep = ep[len("https://") :]
        hconf.setBoolean("fs.s3a.connection.ssl.enabled", True)
    else:
        hconf.setBoolean("fs.s3a.connection.ssl.enabled", False)
    hconf.set("fs.s3a.endpoint", ep)
    hconf.set("fs.s3a.access.key", args.s3_access_key)
    hconf.set("fs.s3a.secret.key", args.s3_secret_key)
    hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    return spark


def binary_file_partition(iterator, cfg: dict):
    tika = (cfg.get("tika_endpoint") or "").strip() or None
    for row in iterator:
        path = row.path
        raw = bytes(row.content)
        title, body, _ = extract_any(path, raw, tika)
        dk = hashlib.sha1(path.encode("utf-8", errors="replace")).hexdigest()[:24]
        yield Row(doc_key=dk, title=title, raw_text=body, source_uri=path)


def filter_chunks_by_min_chars(parts: list[str], min_chars: int) -> list[str]:
    """Quita fragmentos triviales; si todos caen bajo el umbral, conserva contenido no vacío."""
    if not parts:
        return [""]
    kept = [p for p in parts if len(p) >= min_chars]
    if kept:
        return kept
    nonempty = [p for p in parts if p.strip()]
    if nonempty:
        return nonempty
    return [parts[0]] if parts else [""]


def chunk_partition(iterator, cfg: dict):
    model = cfg["tokenizer_model"]
    max_t = int(cfg["max_tokens"])
    overlap = max(0, int(cfg.get("overlap_tokens", 0)))
    step = max(1, max_t - overlap) if overlap < max_t else max_t
    min_chars = max(0, int(cfg.get("min_chunk_chars", 0)))
    ingestion = cfg["ingestion_date"]
    tok_holder: list = []

    def tokenizer():
        if not tok_holder:
            from tokenizers import Tokenizer

            tok_holder.append(Tokenizer.from_pretrained(model))
        return tok_holder[0]

    for row in iterator:
        doc_key = row.doc_key
        title = row.title or ""
        src = row.source_uri or ""
        raw = clean_boilerplate(row.raw_text or "")
        if not raw:
            yield Row(
                doc_id=str(doc_key),
                chunk_id=0,
                title=title,
                source_uri=src,
                raw_text="",
                ingestion_date=ingestion,
            )
            continue
        try:
            t = tokenizer()
            enc = t.encode(raw)
            ids = enc.ids
            if not ids:
                parts = [""]
            else:
                raw_parts: list[str] = []
                i = 0
                while i < len(ids):
                    chunk_ids = ids[i : i + max_t]
                    raw_parts.append(t.decode(chunk_ids, skip_special_tokens=True).strip())
                    if i + max_t >= len(ids):
                        break
                    i += step
                parts = filter_chunks_by_min_chars(raw_parts, min_chars)
        except Exception:
            raw_parts = chunk_by_chars(raw, max(max_t * 4, 256))
            parts = filter_chunks_by_min_chars(raw_parts, min_chars)
        for i, piece in enumerate(parts):
            yield Row(
                doc_id=str(doc_key),
                chunk_id=int(i),
                title=title,
                source_uri=src,
                raw_text=piece,
                ingestion_date=ingestion,
            )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    ingestion = args.ingestion_date or args.run_date
    tika_ep = (args.tika_endpoint or "").strip() or None

    out_local = args.output_dir
    if not out_local:
        d = REPO_ROOT / "data" / "features" / f"run_date={args.run_date}"
        d.mkdir(parents=True, exist_ok=True)
        out_local = str(d.resolve())

    logger.info(
        "Inicio ETL Fase 1 run_date=%s ingestion_date=%s master=%s json=%s files=%s "
        "out_local=%s out_s3a=%s s3=%s tika=%s tokenizer=%s max_tokens=%s overlap=%s min_chunk_chars=%s",
        args.run_date,
        ingestion,
        args.master,
        args.input_json_glob,
        args.input_files_glob,
        out_local,
        args.output_s3a,
        args.s3_endpoint,
        tika_ep,
        args.tokenizer_model,
        args.max_tokens_per_chunk,
        args.overlap_tokens,
        args.min_chunk_chars,
    )

    t0 = time.perf_counter()
    spark: SparkSession | None = None
    try:
        spark = build_spark(args)

        parts = []
        if args.input_json_glob and str(args.input_json_glob).strip():
            dfj_raw = spark.read.option("multiLine", "false").json(args.input_json_glob)
            if args.since_date and "source_updated" in dfj_raw.columns:
                dfj_raw = dfj_raw.filter(F.col("source_updated") >= F.lit(args.since_date))
            dfj = dfj_raw.select(
                F.col("id").cast("string").alias("doc_key"),
                F.coalesce(F.col("title"), F.lit("")).cast("string").alias("title"),
                F.coalesce(F.col("text"), F.lit("")).cast("string").alias("raw_text"),
                F.coalesce(
                    F.col("source_uri"),
                    F.concat(F.lit("s3a://semantic-raw/text/v1#id="), F.col("id").cast("string")),
                )
                .cast("string")
                .alias("source_uri"),
            )
            parts.append(dfj)

        if args.input_files_glob:
            cfg_bin = {"tika_endpoint": tika_ep}
            bdf = (
                spark.read.format("binaryFile")
                .option("recursiveFileLookup", "true")
                .load(args.input_files_glob)
            )
            rdd_b = bdf.rdd.mapPartitions(lambda it: binary_file_partition(it, cfg_bin))
            schema_b = StructType(
                [
                    StructField("doc_key", StringType(), False),
                    StructField("title", StringType(), True),
                    StructField("raw_text", StringType(), True),
                    StructField("source_uri", StringType(), False),
                ]
            )
            parts.append(spark.createDataFrame(rdd_b, schema_b))

        if not parts:
            logger.error("Definí al menos --input-json-glob o --input-files-glob.")
            return 1

        base = parts[0]
        for p in parts[1:]:
            base = base.unionByName(p)

        n_in = base.count()
        logger.info("Documentos / archivos base (antes de chunking): %s", n_in)

        cfg_chunk = {
            "tokenizer_model": args.tokenizer_model,
            "max_tokens": args.max_tokens_per_chunk,
            "overlap_tokens": args.overlap_tokens,
            "min_chunk_chars": args.min_chunk_chars,
            "ingestion_date": ingestion,
        }
        rdd_c = base.rdd.mapPartitions(lambda it: chunk_partition(it, cfg_chunk))
        schema_out = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk_id", IntegerType(), False),
                StructField("title", StringType(), True),
                StructField("source_uri", StringType(), False),
                StructField("raw_text", StringType(), True),
                StructField("ingestion_date", StringType(), False),
            ]
        )
        df_out = spark.createDataFrame(rdd_c, schema_out)
        df_out.cache()
        n_out = df_out.count()
        logger.info("Registros generados (chunks / filas CSV): %s", n_out)

        (
            df_out.write.mode("overwrite")
            .option("header", True)
            .option("compression", "gzip")
            .csv(out_local)
        )
        logger.info("Escrito local: %s", out_local)

        if args.output_s3a:
            (
                df_out.write.mode("overwrite")
                .option("header", True)
                .option("compression", "gzip")
                .csv(args.output_s3a)
            )
            logger.info("Escrito S3A: %s", args.output_s3a)

        elapsed = time.perf_counter() - t0
        logger.info(
            "Cierre OK — duración_s=%.2f leídos=%s generados=%s salida_local=%s",
            elapsed,
            n_in,
            n_out,
            out_local,
        )
        return 0
    except Exception:
        logger.error("ETL falló — traceback completo:\n%s", traceback.format_exc())
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
