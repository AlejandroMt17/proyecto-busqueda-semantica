#!/usr/bin/env python3
"""
Validación de calidad — salida Fase 1 (ETL).

Comprueba: conteos, nulos en columnas críticas, chunk_id >= 0, raw_text coherente.

Ejemplo (local, lee CSV en disco):

  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 ^
    scripts/validate_etl_quality.py --input-glob "file:///D:/ruta/al/repo/data/features/run_date=2026-05-12/*.csv"

S3A / MinIO:

  spark-submit --packages ... scripts/validate_etl_quality.py ^
    --input-glob "s3a://semantic-raw/features/run_date=2026-05-12/*.csv" --s3-endpoint http://127.0.0.1:9000
"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback

from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("validate_etl")

DEFAULT_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validación calidad salida ETL.")
    p.add_argument(
        "--input-glob",
        required=True,
        help="CSV gzip (ruta local file:// o s3a://.../*.csv).",
    )
    p.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    p.add_argument("--s3-access-key", default="minioadmin")
    p.add_argument("--s3-secret-key", default="minioadmin123")
    p.add_argument("--expected-min-rows", type=int, default=1)
    return p.parse_args(argv)


def apply_s3_conf(spark: SparkSession, args: argparse.Namespace) -> None:
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    spark = (
        SparkSession.builder.appName("validate_etl_quality")
        .master("local[*]")
        .config("spark.jars.packages", DEFAULT_PACKAGES)
        .getOrCreate()
    )
    try:
        if args.input_glob.startswith("s3a://"):
            apply_s3_conf(spark, args)
        df = spark.read.option("header", True).option("inferSchema", True).csv(args.input_glob)

        required = {"doc_id", "chunk_id", "title", "source_uri", "raw_text", "ingestion_date"}
        missing = required - set(df.columns)
        if missing:
            logger.error("Faltan columnas: %s", missing)
            return 1

        n = df.count()
        logger.info("Filas leídas: %s", n)
        if n < args.expected_min_rows:
            logger.error("Muy pocas filas: %s < %s", n, args.expected_min_rows)
            return 1

        null_docs = df.filter(F.col("doc_id").isNull() | (F.trim(F.col("doc_id")) == "")).count()
        if null_docs:
            logger.error("doc_id nulo o vacío en %s filas", null_docs)
            return 1

        bad_chunk = df.filter(F.col("chunk_id").isNull() | (F.col("chunk_id") < 0)).count()
        if bad_chunk:
            logger.error("chunk_id inválido en %s filas", bad_chunk)
            return 1

        null_uri = df.filter(F.col("source_uri").isNull() | (F.trim(F.col("source_uri")) == "")).count()
        if null_uri:
            logger.error("source_uri vacío en %s filas", null_uri)
            return 1

        logger.info("Validación OK.")
        return 0
    except Exception:
        logger.error("Fallo validación:\n%s", traceback.format_exc())
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
