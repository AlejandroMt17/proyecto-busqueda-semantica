#!/usr/bin/env python3
"""
Fase 3 — Persistencia en Elasticsearch (manual: Proyecto 1, sección 5.2).

Lee los CSV de Fase 2 (``embedding_json`` + metadatos), asegura el índice con
``dense_vector`` y escribe con **upsert** por ``chunk_id`` (re-ejecución semanal
idempotente).

Usa el conector ``org.elasticsearch.spark.sql`` (elasticsearch-hadoop).

Ejemplo:

  spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0 \\
    src/persistence.py --run-date 2026-05-12 --master "local[*]"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any

import requests
from project_config import load_project_config
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, FloatType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("persistence")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "config.yaml"
DEFAULT_ES_SPARK_PKG = "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0"
DEFAULT_S3_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


def ensure_index(
    base_url: str,
    index: str,
    dims: int,
    auth: tuple[str, str] | None,
) -> None:
    """Crea el índice con mapping dense_vector si no existe (manual sección 5.2)."""
    url = f"{base_url.rstrip('/')}/{index}"
    head = requests.head(url, auth=auth, timeout=30)
    if head.status_code == 200:
        logger.info("Índice %s ya existe — se mantiene mapping (idempotente).", index)
        return
    body: dict[str, Any] = {
        "mappings": {
            "properties": {
                "chunk_id": {"type": "keyword"},
                "doc_id": {"type": "keyword"},
                "run_date": {"type": "keyword"},
                "text": {"type": "text"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": int(dims),
                    "index": True,
                    "similarity": "cosine",
                },
            }
        }
    }
    r = requests.put(url, json=body, auth=auth, timeout=60)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"No se pudo crear índice {index}: HTTP {r.status_code} {r.text[:500]}")
    logger.info("Índice %s creado con dense_vector dims=%s.", index, dims)


def apply_s3_conf(spark: SparkSession, args: argparse.Namespace) -> None:
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.path.style.access", "true")
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    if argv is None:
        argv = sys.argv[1:]
    cfg = load_project_config(DEFAULT_CONFIG) if DEFAULT_CONFIG.is_file() else {}
    es_cfg = cfg.get("elasticsearch") or {}
    minio = cfg.get("minio") or {}
    spark_cfg = cfg.get("spark") or {}
    paths_cfg = cfg.get("paths") or {}

    cfg_driver_host = spark_cfg.get("driver_host")
    if isinstance(cfg_driver_host, str):
        cfg_driver_host = cfg_driver_host.strip() or None
    if not cfg_driver_host:
        cfg_driver_host = os.environ.get("SPARK_DRIVER_HOST") or None

    es_host = str(es_cfg.get("host") or "127.0.0.1").strip()
    es_port = int(es_cfg.get("port") or 9200)
    es_index = str(es_cfg.get("index") or "documents").strip()
    es_dims = int(es_cfg.get("dims") or 384)
    es_user = (es_cfg.get("user") or "").strip() or None
    es_password = (es_cfg.get("password") or "").strip() or None
    es_use_ssl = bool(es_cfg.get("use_ssl", False))

    p = argparse.ArgumentParser(description="Fase 3 — persistencia en Elasticsearch.")
    p.add_argument("--run-date", required=True, help="Partición YYYY-MM-DD (misma que Fase 1/2).")
    p.add_argument(
        "--input-glob",
        default=None,
        help="CSV Fase 2. Por defecto data/predictions/run_date=<fecha>/ o s3a en cluster.",
    )
    p.add_argument(
        "--jars-packages",
        default=None,
        help="Paquetes Ivy (coma). Por defecto: ES connector y, si la entrada es s3a, también S3A.",
    )
    p.add_argument("--master", default=spark_cfg.get("master", "local[*]"))
    p.add_argument("--driver-host", default=cfg_driver_host)
    p.add_argument("--es-host", default=es_host, help="Host Elasticsearch alcanzable desde Spark driver.")
    p.add_argument("--es-port", type=int, default=es_port)
    p.add_argument("--es-index", default=es_index)
    p.add_argument("--es-dims", type=int, default=es_dims)
    p.add_argument("--s3-endpoint", default=minio.get("endpoint", "http://127.0.0.1:9000"))
    p.add_argument("--s3-access-key", default=minio.get("access_key", "minioadmin"))
    p.add_argument("--s3-secret-key", default=minio.get("secret_key", "minioadmin123"))
    p.add_argument("--s3-bucket", default=minio.get("bucket", "semantic-raw"))
    ns = p.parse_args(argv)
    ns._paths_cfg = paths_cfg  # type: ignore[attr-defined]
    ns._es_user = es_user  # type: ignore[attr-defined]
    ns._es_password = es_password  # type: ignore[attr-defined]
    ns.es_use_ssl = es_use_ssl  # type: ignore[attr-defined]
    if not (ns.jars_packages or "").strip():
        master_cluster = str(ns.master).startswith("spark://")
        in_guess = ns.input_glob
        if not in_guess:
            in_guess = (
                f"s3a://{ns.s3_bucket}/predictions/run_date={ns.run_date}/"
                if master_cluster
                else str(
                    (
                        REPO_ROOT
                        / (paths_cfg.get("predictions") or "data/predictions")
                        / f"run_date={ns.run_date}"
                    ).resolve()
                )
            )
        if str(in_guess).startswith("s3a://"):
            ns.jars_packages = f"{DEFAULT_ES_SPARK_PKG},{DEFAULT_S3_PACKAGES}"
        else:
            ns.jars_packages = DEFAULT_ES_SPARK_PKG
    return ns


def build_spark(args: argparse.Namespace) -> SparkSession:
    b = (
        SparkSession.builder.appName(f"persistence_{args.run_date}")
        .master(args.master)
        .config("spark.jars.packages", args.jars_packages)
    )
    driver_py = (os.environ.get("PYSPARK_DRIVER_PYTHON") or os.environ.get("PYSPARK_PYTHON") or "").strip()
    if driver_py:
        b = b.config("spark.pyspark.driver.python", driver_py)
        logger.info("spark.pyspark.driver.python=%s", driver_py)
    if not str(args.master).startswith("spark://"):
        if driver_py:
            b = b.config("spark.pyspark.python", driver_py)
    else:
        exec_py = (os.environ.get("SPARK_EXECUTOR_PYTHON") or "").strip()
        if exec_py:
            b = b.config("spark.pyspark.python", exec_py)
    if args.driver_host:
        b = b.config("spark.driver.host", args.driver_host).config("spark.driver.bindAddress", "0.0.0.0")
    return b.getOrCreate()


def _es_base_url(args: argparse.Namespace) -> str:
    scheme = "https" if args.es_use_ssl else "http"
    return f"{scheme}://{args.es_host}:{args.es_port}"


def _es_auth(args: argparse.Namespace) -> tuple[str, str] | None:
    u = getattr(args, "_es_user", None)
    p = getattr(args, "_es_password", None)
    if u and p:
        return u, p
    return None


def verify_es_count(base_url: str, index: str, run_date: str, auth: tuple[str, str] | None) -> int:
    """Post-verificación: documentos con run_date dado."""
    q = {"query": {"term": {"run_date": run_date}}}
    url = f"{base_url.rstrip('/')}/{index}/_count"
    r = requests.post(url, json=q, auth=auth, timeout=60)
    r.raise_for_status()
    data = r.json()
    return int(data.get("count", 0))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    paths_cfg = getattr(args, "_paths_cfg", {}) or {}
    master_cluster = str(args.master).startswith("spark://")

    in_path = args.input_glob
    if not in_path:
        if master_cluster:
            in_path = f"s3a://{args.s3_bucket}/predictions/run_date={args.run_date}/"
        else:
            base = paths_cfg.get("predictions") or "data/predictions"
            in_path = str((REPO_ROOT / base / f"run_date={args.run_date}").resolve())

    base_url = _es_base_url(args)
    auth = _es_auth(args)

    logger.info(
        "Inicio Fase 3 run_date=%s master=%s input=%s es=%s index=%s",
        args.run_date,
        args.master,
        in_path,
        base_url,
        args.es_index,
    )
    t0 = time.perf_counter()
    spark: SparkSession | None = None
    try:
        ensure_index(base_url, args.es_index, args.es_dims, auth)

        spark = build_spark(args)
        if str(in_path).startswith("s3a://"):
            apply_s3_conf(spark, args)

        raw = spark.read.option("header", True).option("inferSchema", True).csv(in_path)
        if "embedding_json" not in raw.columns:
            logger.error("Falta columna embedding_json (salida esperada de Fase 2 CSV).")
            return 1

        emb = raw.withColumn(
            "embedding",
            F.from_json(F.col("embedding_json"), ArrayType(FloatType())),
        )
        text_col = F.col("raw_text").cast("string") if "raw_text" in raw.columns else F.lit("").cast("string")
        df_es = emb.select(
            F.col("chunk_id").cast("string").alias("chunk_id"),
            F.col("doc_id").cast("string").alias("doc_id"),
            F.lit(args.run_date).cast("string").alias("run_date"),
            text_col.alias("text"),
            F.col("embedding"),
        )

        n = df_es.count()
        logger.info("Registros a indexar: %s", n)

        spark.conf.set("es.nodes", args.es_host)
        spark.conf.set("es.port", str(args.es_port))
        spark.conf.set("es.nodes.wan.only", "true")
        spark.conf.set("es.index.auto.create", "false")
        if auth:
            spark.conf.set("es.net.http.auth.user", auth[0])
            spark.conf.set("es.net.http.auth.pass", auth[1])

        df_es.write.format("org.elasticsearch.spark.sql").option("es.resource", args.es_index).option(
            "es.mapping.id", "chunk_id"
        ).option("es.write.operation", "upsert").mode("append").save()

        cnt = verify_es_count(base_url, args.es_index, args.run_date, auth)
        logger.info(
            "Cierre OK Fase 3 — duración_s=%.2f escritos_en_lote=%s count_por_run_date_en_ES=%s",
            time.perf_counter() - t0,
            n,
            cnt,
        )
        return 0
    except Exception:
        logger.error("persistence falló:\n%s", traceback.format_exc())
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
