#!/usr/bin/env python3
"""
Fase 2 — Inferencia distribuida de embeddings (manual: Proyecto 1, sección 5.2).

Lee los CSV de Fase 1 (``data/features/run_date=YYYY-MM-DD/`` o s3a equivalente),
aplica SentenceTransformer vía Pandas UDF (modelo una vez por partición) y escribe
``data/predictions/run_date=YYYY-MM-DD/`` en **CSV** con ``embedding_json`` (Spark
no puede escribir ``ArrayType`` en CSV de forma portable).

Paralelismo real en executors (no ``collect`` / ``toPandas`` en el driver).

Ejemplo:

  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
    src/batch_inference.py --run-date 2026-05-12 --master "local[*]"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import traceback
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from project_config import load_project_config
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("batch_inference")

DEFAULT_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "config.yaml"
DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_EMBEDDING_DIMS = 384


def check_driver_python_deps() -> None:
    """Falla antes de arrancar Spark si el Python del driver no puede correr el UDF."""
    exe = sys.executable
    missing: list[str] = []
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        missing.append("pyarrow>=4")
    try:
        import sentence_transformers  # noqa: F401
    except ImportError:
        missing.append("sentence-transformers")
    try:
        import torch  # noqa: F401
    except ImportError:
        missing.append("torch")
    if missing:
        raise SystemExit(
            f"Python del driver ({exe}) no tiene dependencias de Fase 2: {', '.join(missing)}.\n"
            "Instala en el MISMO entorno que usa spark-submit:\n"
            f"  \"{exe}\" -m pip install pyarrow sentence-transformers torch\n"
            "En Windows (solo driver), sin fijar el intérprete de los workers:\n"
            "  $env:PYSPARK_DRIVER_PYTHON = (Resolve-Path '.\\.venv\\Scripts\\python.exe').Path\n"
            "  Remove-Item Env:PYSPARK_PYTHON -ErrorAction SilentlyContinue\n"
            "En cluster, `spark.pyspark.python` queda en `python` (PATH en cada worker); "
            "opcional: $env:SPARK_EXECUTOR_PYTHON si todos los workers comparten la misma ruta."
        )


def chunk_row_key(doc_id: str, chunk_id: int) -> str:
    return f"{doc_id}_{int(chunk_id)}"


def _model_name_from_cfg(cfg: dict) -> str:
    m = cfg.get("model") or {}
    mn = (m.get("name") or "").strip()
    if mn and "/" not in mn:
        return f"sentence-transformers/{mn}"
    return mn or DEFAULT_MODEL


def _embedding_dims_from_cfg(cfg: dict) -> int:
    m = cfg.get("model") or {}
    es = cfg.get("elasticsearch") or {}
    for key in ("embedding_dims", "dims"):
        v = m.get(key)
        if isinstance(v, int) and v > 0:
            return v
    v2 = es.get("dims")
    if isinstance(v2, int) and v2 > 0:
        return v2
    return DEFAULT_EMBEDDING_DIMS


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    if argv is None:
        argv = sys.argv[1:]
    cfg_path = DEFAULT_CONFIG
    cfg = load_project_config(cfg_path) if cfg_path.is_file() else {}
    minio = cfg.get("minio") or {}
    spark_cfg = cfg.get("spark") or {}
    paths_cfg = cfg.get("paths") or {}
    cfg_driver_host = spark_cfg.get("driver_host")
    if isinstance(cfg_driver_host, str):
        cfg_driver_host = cfg_driver_host.strip() or None
    if not cfg_driver_host:
        cfg_driver_host = os.environ.get("SPARK_DRIVER_HOST") or None

    default_model = _model_name_from_cfg(cfg)
    default_dims = _embedding_dims_from_cfg(cfg)

    p = argparse.ArgumentParser(description="Fase 2 — inferencia de embeddings (Spark + Pandas UDF).")
    p.add_argument("--run-date", required=True, help="Misma partición que Fase 1 (YYYY-MM-DD).")
    p.add_argument(
        "--input-glob",
        default=None,
        help="CSV Fase 1 (directorio o glob). Por defecto: data/features/run_date=<fecha>/.",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Salida local (CSV por defecto). Por defecto: "
            "<paths.predictions>/run_date=<fecha>/ en repo."
        ),
    )
    p.add_argument(
        "--output-s3a",
        default=None,
        help="Salida adicional o única en cluster: s3a://.../predictions/run_date=.../",
    )
    p.add_argument(
        "--output-format",
        choices=("csv", "parquet", "both"),
        default="csv",
        help="Formato de salida (manual: CSV particionado en data/predictions/). Default: csv.",
    )
    p.add_argument("--master", default=spark_cfg.get("master", "local[*]"))
    p.add_argument("--driver-host", default=cfg_driver_host)
    p.add_argument("--jars-packages", default=DEFAULT_PACKAGES)
    p.add_argument("--s3-endpoint", default=minio.get("endpoint", "http://127.0.0.1:9000"))
    p.add_argument("--s3-access-key", default=minio.get("access_key", "minioadmin"))
    p.add_argument("--s3-secret-key", default=minio.get("secret_key", "minioadmin123"))
    p.add_argument("--s3-bucket", default=minio.get("bucket", "semantic-raw"))
    p.add_argument("--model-name", default=default_model)
    p.add_argument("--embedding-dims", type=int, default=default_dims)
    p.add_argument(
        "--encode-batch-size",
        type=int,
        default=int((cfg.get("model") or {}).get("batch_size") or 32),
    )
    p.add_argument(
        "--num-partitions",
        type=int,
        default=int(spark_cfg.get("num_partitions") or 0),
        help=">0 para reparticionar antes del UDF.",
    )
    p.add_argument(
        "--skip-stats",
        action="store_true",
        help="Salta count() pre-write.",
    )
    p.add_argument(
        "--cache-intermediate",
        action="store_true",
        help="Aplica .cache() al DataFrame final antes de escribir.",
    )
    p.add_argument(
        "--validate-output",
        action="store_true",
        help="Tras escribir, valida muestra (CSV: embedding_json; Parquet: embedding).",
    )
    ns = p.parse_args(argv)
    ns._paths_cfg = paths_cfg  # type: ignore[attr-defined]
    return ns


def build_spark(args: argparse.Namespace) -> SparkSession:
    b = (
        SparkSession.builder.appName(f"batch_inference_{args.run_date}")
        .master(args.master)
        .config("spark.jars.packages", args.jars_packages)
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(max(1, args.encode_batch_size)))
    )
    driver_py = (os.environ.get("PYSPARK_DRIVER_PYTHON") or os.environ.get("PYSPARK_PYTHON") or "").strip()
    if driver_py:
        b = b.config("spark.pyspark.driver.python", driver_py)
        logger.info("spark.pyspark.driver.python=%s", driver_py)

    if not str(args.master).startswith("spark://"):
        if driver_py:
            b = b.config("spark.pyspark.python", driver_py)
            logger.info("spark.pyspark.python=%s (local: mismo que driver)", driver_py)
    else:
        exec_py = (os.environ.get("SPARK_EXECUTOR_PYTHON") or "python").strip()
        b = b.config("spark.pyspark.python", exec_py)
        logger.info("spark.pyspark.python=%s (executors: PATH por nodo)", exec_py)

    if args.driver_host:
        b = b.config("spark.driver.host", args.driver_host).config("spark.driver.bindAddress", "0.0.0.0")
    return b.getOrCreate()


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


def _default_predictions_dir(paths_cfg: dict, run_date: str) -> Path:
    base = (paths_cfg.get("predictions") or "data/predictions").strip().strip("/")
    return REPO_ROOT / base / f"run_date={run_date}"


def _validate_parquet(spark: SparkSession, path: str, expected_dims: int) -> None:
    logger.info("Validando salida Parquet en %s ...", path)
    df = spark.read.parquet(path)
    cols = set(df.columns)
    required = {"chunk_key", "doc_id", "chunk_id", "embedding"}
    missing = required - cols
    if missing:
        raise RuntimeError(f"Parquet sin columnas requeridas: {missing}")
    sample = df.limit(5).collect()
    if not sample:
        raise RuntimeError(f"Parquet vacío: {path}")
    bad = [r for r in sample if r["embedding"] is None or len(r["embedding"]) != expected_dims]
    if bad:
        raise RuntimeError(
            f"`embedding` con dimensión inesperada en {len(bad)}/{len(sample)} filas; "
            f"esperaba {expected_dims}."
        )
    logger.info(
        "Validación OK — sample=%d filas, dims=%d, primera chunk_key=%s",
        len(sample),
        expected_dims,
        sample[0]["chunk_key"],
    )


def _validate_csv_predictions(spark: SparkSession, path: str, expected_dims: int) -> None:
    logger.info("Validando salida CSV en %s ...", path)
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    if "embedding_json" not in df.columns:
        raise RuntimeError("CSV de predicciones sin columna embedding_json")
    parsed = df.withColumn(
        "_emb",
        F.from_json(F.col("embedding_json"), ArrayType(FloatType())),
    )
    sample = parsed.limit(5).collect()
    if not sample:
        raise RuntimeError(f"CSV vacío: {path}")
    bad = [r for r in sample if r["_emb"] is None or len(r["_emb"]) != expected_dims]
    if bad:
        raise RuntimeError(
            f"embedding_json con dimensión inesperada en {len(bad)}/{len(sample)} filas; "
            f"esperaba {expected_dims}."
        )
    logger.info(
        "Validación OK — sample=%d filas, dims=%d, chunk_id=%s",
        len(sample),
        expected_dims,
        sample[0].asDict().get("chunk_id"),
    )


def _write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)
    logger.info("Parquet: %s", path)


def _write_csv_predictions(df_emb: DataFrame, path: str) -> None:
    """CSV con embedding serializado (Fase 3 lo parsea con from_json)."""

    def col_or_null(name: str) -> F.Column:
        if name in df_emb.columns:
            return F.col(name).cast("string")
        return F.lit(None).cast("string")

    with_json = df_emb.withColumn("embedding_json", F.to_json(F.col("embedding")))
    out = with_json.select(
        F.col("chunk_id").cast("string").alias("chunk_id"),
        F.col("doc_id").cast("string").alias("doc_id"),
        col_or_null("title"),
        col_or_null("source_uri"),
        col_or_null("ingestion_date"),
        col_or_null("raw_text"),
        F.col("embedding_json"),
    )
    out.write.mode("overwrite").option("header", "true").csv(path)
    logger.info("CSV predicciones: %s", path)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    master_is_cluster = str(args.master).startswith("spark://")
    paths_cfg = getattr(args, "_paths_cfg", {}) or {}

    in_path = args.input_glob
    if not in_path:
        if master_is_cluster:
            in_path = f"s3a://{args.s3_bucket}/features/run_date={args.run_date}/"
        else:
            in_path = str((REPO_ROOT / "data" / "features" / f"run_date={args.run_date}").resolve())

    out_local: str | None = args.output_dir
    out_s3a: str | None = args.output_s3a
    if not out_local and not out_s3a:
        if master_is_cluster:
            out_s3a = f"s3a://{args.s3_bucket}/predictions/run_date={args.run_date}/"
        else:
            d = _default_predictions_dir(paths_cfg, args.run_date)
            d.mkdir(parents=True, exist_ok=True)
            out_local = str(d.resolve())

    fmt = (args.output_format or "csv").lower()

    logger.info(
        "Inicio Fase 2 run_date=%s master=%s input=%s out_local=%s out_s3a=%s format=%s model=%s dims=%s",
        args.run_date,
        args.master,
        in_path,
        out_local,
        out_s3a,
        fmt,
        args.model_name,
        args.embedding_dims,
    )
    t0 = time.perf_counter()
    spark: SparkSession | None = None
    try:
        check_driver_python_deps()
        spark = build_spark(args)
        if str(in_path).startswith("s3a://") or (out_s3a and str(out_s3a).startswith("s3a://")):
            apply_s3_conf(spark, args)

        df = spark.read.option("header", True).option("inferSchema", True).csv(in_path)
        required = {"doc_id", "chunk_id", "raw_text"}
        miss = required - set(df.columns)
        if miss:
            logger.error("Faltan columnas requeridas en CSV Fase 1: %s", miss)
            return 1

        num_part = int(args.num_partitions or 0)
        if num_part > 0:
            df = df.repartition(num_part)

        embed_fn = make_embed_pandas_udf(args.model_name, args.encode_batch_size, args.embedding_dims)
        df_emb = df.withColumn("embedding", embed_fn(F.col("raw_text")))
        df_emb = df_emb.withColumn(
            "chunk_key",
            F.concat_ws("_", F.col("doc_id").cast("string"), F.col("chunk_id").cast("string")),
        )

        def col_or_null(name: str) -> F.Column:
            if name in df_emb.columns:
                return F.col(name).cast("string")
            return F.lit(None).cast("string")

        df_parquet = df_emb.select(
            "chunk_key",
            "doc_id",
            "chunk_id",
            col_or_null("title"),
            col_or_null("source_uri"),
            col_or_null("ingestion_date"),
            "embedding",
        )

        will_double_pass = (out_local is not None) and (out_s3a is not None)
        cached = False
        if args.cache_intermediate or will_double_pass:
            df_emb = df_emb.cache()
            cached = True

        n: int | None = None
        try:
            if not args.skip_stats:
                n = df_emb.count()
                logger.info("Filas vectorizadas: %s", n)

            def parquet_sidecar(base: str) -> str:
                return str(Path(base.rstrip("/")) / "_parquet")

            def write_to(path: str) -> None:
                if fmt == "csv":
                    _write_csv_predictions(df_emb, path)
                elif fmt == "parquet":
                    _write_parquet(df_parquet, path)
                else:
                    _write_csv_predictions(df_emb, path)
                    _write_parquet(df_parquet, parquet_sidecar(path))

            if out_local:
                write_to(out_local)
            if out_s3a:
                write_to(out_s3a)
        finally:
            if cached:
                df_emb.unpersist()

        if args.validate_output:

            def parquet_sidecar(base: str) -> str:
                return str(Path(base.rstrip("/")) / "_parquet")

            for target in (out_local, out_s3a):
                if not target:
                    continue
                if fmt == "parquet" or fmt == "both":
                    pv = target if fmt == "parquet" else parquet_sidecar(target)
                    _validate_parquet(spark, pv, args.embedding_dims)
                if fmt in ("csv", "both"):
                    _validate_csv_predictions(spark, target, args.embedding_dims)

        logger.info(
            "Cierre OK Fase 2 — duración_s=%.2f filas=%s",
            time.perf_counter() - t0,
            n if n is not None else "n/a",
        )
        return 0
    except Exception:
        logger.error("batch_inference falló:\n%s", traceback.format_exc())
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
