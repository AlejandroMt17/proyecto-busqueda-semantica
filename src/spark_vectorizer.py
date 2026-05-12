#!/usr/bin/env python3
"""
Fase 2 — Embeddings distribuidos con SentenceTransformer.

Lee la salida de Fase 1 (CSV gzip con cabecera) y escribe Parquet con una columna
`embedding` (array<float>, 384 dims para all-MiniLM-L6-v2).

La inferencia usa Pandas UDF en modo *Scalar Iterator* para cargar el modelo
una sola vez por partición (evita penalización por no estar distribuido).

Ejemplo local:

  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
    src/spark_vectorizer.py --run-date 2026-05-12 --master "local[*]"
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import traceback
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("spark_vectorizer")

DEFAULT_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_EMBEDDING_DIMS = 384


def chunk_row_key(doc_id: str, chunk_id: int) -> str:
    return f"{doc_id}_{int(chunk_id)}"


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fase 2 — embeddings (Spark + Pandas UDF).")
    p.add_argument("--run-date", required=True, help="Misma partición que Fase 1 (YYYY-MM-DD).")
    p.add_argument(
        "--input-glob",
        default=None,
        help="CSV Fase 1 (directorio o glob). Por defecto: data/features/run_date=<fecha>/.",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Salida Parquet local. Por defecto: data/embeddings/run_date=<fecha>/.",
    )
    p.add_argument("--output-s3a", default=None, help="Copia adicional s3a://.../run_date=.../")
    p.add_argument("--master", default="local[*]")
    p.add_argument("--driver-host", default=None)
    p.add_argument("--jars-packages", default=DEFAULT_PACKAGES)
    p.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    p.add_argument("--s3-access-key", default="minioadmin")
    p.add_argument("--s3-secret-key", default="minioadmin123")
    p.add_argument("--model-name", default=DEFAULT_MODEL)
    p.add_argument("--embedding-dims", type=int, default=DEFAULT_EMBEDDING_DIMS)
    p.add_argument("--encode-batch-size", type=int, default=32)
    p.add_argument("--num-partitions", type=int, default=0, help=">0 para reparticionar antes del UDF.")
    return p.parse_args(argv)


def build_spark(args: argparse.Namespace) -> SparkSession:
    b = (
        SparkSession.builder.appName(f"spark_vectorizer_{args.run_date}")
        .master(args.master)
        .config("spark.jars.packages", args.jars_packages)
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(max(1, args.encode_batch_size)))
    )
    if args.driver_host:
        b = b.config("spark.driver.host", args.driver_host).config("spark.driver.bindAddress", "0.0.0.0")
    return b.getOrCreate()


def apply_s3_conf(spark: SparkSession, args: argparse.Namespace) -> None:
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.path.style.access", "true")
    # Evita NativeIO$Windows.access0 en Windows: bufferiza los uploads en memoria.
    hconf.set("fs.s3a.fast.upload", "true")
    hconf.set("fs.s3a.fast.upload.buffer", "bytebuffer")
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


def make_embed_pandas_udf(model_name: str, encode_batch_size: int, expected_dims: int):
    @pandas_udf(ArrayType(FloatType()))
    def embed_text_batches(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(model_name)
        for texts in iterator:
            tlist = texts.astype(str).fillna("").tolist()
            tlist = [s if str(s).strip() else " " for s in tlist]
            arr = model.encode(
                tlist,
                batch_size=max(1, encode_batch_size),
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,
            )
            if arr.ndim != 2 or arr.shape[1] != expected_dims:
                raise ValueError(f"embedding shape inesperada {arr.shape}, esperado (*,{expected_dims})")
            out = [row.astype("float64").tolist() for row in arr]
            yield pd.Series(out)

    return embed_text_batches


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    in_path = args.input_glob
    if not in_path:
        in_path = str((REPO_ROOT / "data" / "features" / f"run_date={args.run_date}").resolve())

    out_local = args.output_dir
    if not out_local:
        d = REPO_ROOT / "data" / "embeddings" / f"run_date={args.run_date}"
        d.mkdir(parents=True, exist_ok=True)
        out_local = str(d.resolve())

    logger.info(
        "Inicio Fase 2 run_date=%s master=%s input=%s out_local=%s out_s3a=%s model=%s dims=%s",
        args.run_date,
        args.master,
        in_path,
        out_local,
        args.output_s3a,
        args.model_name,
        args.embedding_dims,
    )
    t0 = time.perf_counter()
    spark: SparkSession | None = None
    try:
        spark = build_spark(args)
        if str(in_path).startswith("s3a://") or (args.output_s3a and str(args.output_s3a).startswith("s3a://")):
            apply_s3_conf(spark, args)

        df = spark.read.option("header", True).option("inferSchema", True).csv(in_path)
        required = {"doc_id", "chunk_id", "raw_text"}
        miss = required - set(df.columns)
        if miss:
            logger.error("Faltan columnas requeridas en CSV Fase 1: %s", miss)
            return 1

        if args.num_partitions and args.num_partitions > 0:
            df = df.repartition(args.num_partitions)

        embed_fn = make_embed_pandas_udf(args.model_name, args.encode_batch_size, args.embedding_dims)
        df_emb = df.withColumn("embedding", embed_fn(F.col("raw_text")))
        df_out = df_emb.withColumn(
            "chunk_key",
            F.concat_ws("_", F.col("doc_id").cast("string"), F.col("chunk_id").cast("string")),
        )

        def col_or_null(name: str) -> F.Column:
            if name in df_out.columns:
                return F.col(name).cast("string")
            return F.lit(None).cast("string")

        df_final = df_out.select(
            "chunk_key",
            "doc_id",
            "chunk_id",
            col_or_null("title"),
            col_or_null("source_uri"),
            col_or_null("ingestion_date"),
            "embedding",
        )
        n = 0
        df_final.cache()
        try:
            n = df_final.count()
            logger.info("Filas vectorizadas: %s", n)

            df_final.write.mode("overwrite").parquet(out_local)
            logger.info("Parquet local: %s", out_local)

            if args.output_s3a:
                df_final.write.mode("overwrite").parquet(args.output_s3a)
                logger.info("Parquet S3A: %s", args.output_s3a)
        finally:
            df_final.unpersist()

        logger.info("Cierre OK — duración_s=%.2f filas=%s", time.perf_counter() - t0, n)
        return 0
    except Exception:
        logger.error("Vectorizador falló:\n%s", traceback.format_exc())
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
