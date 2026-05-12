#!/usr/bin/env python3
"""
Fase 1 — ETL de documentos (Semana 2).

Lee JSONL comprimido desde MinIO (S3A), normaliza texto, genera chunks y escribe CSV en S3A.

Cluster (desde la raíz del repo, con MinIO accesible en la IP del master):

  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 ^
    src/etl_features.py --run-date 2026-05-12 --master spark://10.84.18.85:7077 ^
    --s3-endpoint http://10.84.18.85:9000 --driver-host 10.84.18.85

Local (sin cluster, MinIO en localhost):

  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 ^
    src/etl_features.py --run-date 2026-05-12 --master "local[*]"
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
import traceback

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("etl_features")

DEFAULT_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


def chunk_text(text: str, max_chars: int = 900) -> list[str]:
    """Parte texto en trozos deterministas (sin solapamiento) para embeddings posteriores."""
    if text is None:
        return []
    t = re.sub(r"\s+", " ", str(text)).strip()
    if not t:
        return []
    chunks: list[str] = []
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
            chunks.append(piece)
    return chunks


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETL Fase 1 — features para búsqueda semántica (Spark).")
    p.add_argument("--run-date", required=True, help="Fecha de corte YYYY-MM-DD (partición lógica).")
    p.add_argument(
        "--master",
        default="local[*]",
        help='Spark master URL, ej. spark://10.84.18.85:7077 o "local[*]".',
    )
    p.add_argument(
        "--input-glob",
        default="s3a://semantic-raw/text/v1/*.jsonl.gz",
        help="Patrón de entrada (S3A o file://).",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Salida CSV (directorio) en S3A. Por defecto: s3a://semantic-raw/features/run_date=.../",
    )
    p.add_argument(
        "--s3-endpoint",
        default="http://127.0.0.1:9000",
        help="MinIO/S3 (host:puerto sin esquema interno). Los workers deben alcanzar esta IP:puerto.",
    )
    p.add_argument("--s3-access-key", default="minioadmin")
    p.add_argument("--s3-secret-key", default="minioadmin123")
    p.add_argument(
        "--driver-host",
        default=None,
        help="IP del driver visible por workers (obligatorio en cluster PySpark). Ej. 10.84.18.85",
    )
    p.add_argument(
        "--jars-packages",
        default=DEFAULT_PACKAGES,
        help="Paquetes Maven para S3A (coma separada).",
    )
    p.add_argument("--chunk-max-chars", type=int, default=900, help="Tamaño máximo aproximado por chunk.")
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    out = args.output_dir or f"s3a://semantic-raw/features/run_date={args.run_date}/"

    logger.info(
        "Inicio ETL Fase 1 run_date=%s master=%s input=%s output=%s endpoint=%s",
        args.run_date,
        args.master,
        args.input_glob,
        out,
        args.s3_endpoint,
    )

    t0 = time.perf_counter()
    spark: SparkSession | None = None
    try:
        spark = build_spark(args)

        max_c = args.chunk_max_chars

        def chunk_rows_local(rows):
            for row in rows:
                doc_id = row["id"]
                title = row["title"] or ""
                text = row["text"] or ""
                run_date = row["run_date"]
                parts = chunk_text(text, max_c)
                if not parts:
                    yield (str(doc_id), 0, title, "", run_date, 0)
                    continue
                for i, part in enumerate(parts):
                    yield (str(doc_id), int(i), title, part, run_date, int(len(part)))

        df_in = (
            spark.read.option("multiLine", "false")
            .json(args.input_glob)
            .select(
                F.col("id").cast("string").alias("id"),
                F.col("title").cast("string").alias("title"),
                F.col("text").cast("string").alias("text"),
            )
            .withColumn("run_date", F.lit(args.run_date))
        )

        n_in = df_in.count()
        logger.info("Registros leídos (documentos): %s", n_in)

        rdd = df_in.rdd.mapPartitions(chunk_rows_local)
        schema_out = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk_id", IntegerType(), False),
                StructField("title", StringType(), True),
                StructField("text_chunk", StringType(), True),
                StructField("run_date", StringType(), False),
                StructField("text_len", IntegerType(), False),
            ]
        )
        df_out = spark.createDataFrame(rdd, schema_out)
        df_out.cache()
        n_out = df_out.count()
        logger.info("Registros generados (chunks): %s", n_out)

        (
            df_out.write.mode("overwrite")
            .option("header", True)
            .option("compression", "gzip")
            .csv(out)
        )

        elapsed = time.perf_counter() - t0
        logger.info(
            "Cierre OK — duración_s=%.2f leídos=%s generados=%s salida=%s",
            elapsed,
            n_in,
            n_out,
            out,
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
