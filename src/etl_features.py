#!/usr/bin/env python3
"""
Fase 1 — ETL de features para búsqueda semántica.

Lee metadata de arXiv (JSON o JSONL[.gz]) desde MinIO/S3 vía s3a y escribe
chunks limpios en CSV particionado por `run_date`.

Ejemplo cluster: si no pasas ``--driver-host``, se infiere la IP de salida
hacia el host del master (evita ``host.docker.internal`` en Windows). Puedes
forzar una IP con ``--driver-host``, ``spark.driver_host`` en ``config.yaml``
o la variable ``SPARK_DRIVER_HOST``.

    La IP del cluster/MinIO se centraliza en ``conf/config.yaml`` (``network.host``)
    o con la variable de entorno ``SEMANTIC_SEARCH_HOST``.

    spark-submit --master spark://<tu-ip>:7077 \\
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
        src/etl_features.py ...

Si el worker cae con ``Connection reset`` al transferir ``aws-java-sdk-bundle``
(~280MB), copia esos JARs a ``%SPARK_HOME%\\jars`` en **cada** worker (y driver)
y ejecuta con ``--no-spark-packages`` (sin ``--packages`` en spark-submit).
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import socket
import sys
from pathlib import Path

from project_config import load_project_config
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, trim, monotonically_increasing_id, lit,
    regexp_replace, length, count, sum as sum_, when,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "config.yaml"
DEFAULT_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


def parse_args(argv: list[str], cfg: dict) -> argparse.Namespace:
    minio = cfg.get("minio", {})
    spark_cfg = cfg.get("spark", {})
    cfg_driver_host = spark_cfg.get("driver_host")
    if isinstance(cfg_driver_host, str):
        cfg_driver_host = cfg_driver_host.strip() or None
    if not cfg_driver_host:
        cfg_driver_host = os.environ.get("SPARK_DRIVER_HOST") or None

    p = argparse.ArgumentParser(description="Fase 1 — ETL features (Spark).")
    p.add_argument("--run-date", default=cfg.get("run_date"),
                   help="Partición YYYY-MM-DD (default: config.yaml).")
    p.add_argument("--master", default=spark_cfg.get("master", "local[*]"),
                   help="Spark master URL.")
    p.add_argument(
        "--driver-host",
        default=cfg_driver_host,
        help=(
            "spark.driver.host: IP/hostname alcanzable desde los workers. "
            "Si se omite en modo spark://, se intenta inferir la IP de salida hacia el master."
        ),
    )
    p.add_argument("--jars-packages", default=DEFAULT_PACKAGES)
    p.add_argument(
        "--no-spark-packages",
        action="store_true",
        help=(
            "No usar spark.jars.packages. Requiere hadoop-aws y aws-java-sdk-bundle "
            "en el classpath del driver (se intentan localizar en ~/.ivy2/jars) y "
            "en cada worker (SPARK_HOME/jars o --executor-s3-jars)."
        ),
    )
    p.add_argument(
        "--driver-s3-jars",
        default=None,
        help=(
            "JARs S3A para el driver, separados por ; (Windows) o : (Linux). "
            "Por defecto con --no-spark-packages se buscan en ~/.ivy2/jars."
        ),
    )
    p.add_argument(
        "--executor-s3-jars",
        default=None,
        help=(
            "Misma lista de JARs pero con rutas válidas en los WORKERS Linux "
            "(p. ej. /opt/spark/jars-extra/hadoop-aws-3.3.4.jar:...). "
            "Si se omite, se asume que ya están en SPARK_HOME/jars del worker."
        ),
    )

    p.add_argument("--s3-endpoint", default=minio.get("endpoint", "http://127.0.0.1:9000"))
    p.add_argument("--s3-access-key", default=minio.get("access_key", "minioadmin"))
    p.add_argument("--s3-secret-key", default=minio.get("secret_key", "minioadmin123"))
    p.add_argument("--s3-bucket", default=minio.get("bucket", "semantic-raw"))

    p.add_argument(
        "--input-json-glob",
        default=None,
        help=(
            "Ruta (s3a:// o local) del JSON/JSONL a leer. "
            "Default: s3a://<bucket>/arxiv-metadata-oai-snapshot.json"
        ),
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Salida CSV. Default: en modo spark:// = s3a://<s3-bucket>/features/run_date=<fecha>/; "
            "en local = data/features/run_date=<fecha>/."
        ),
    )
    p.add_argument("--num-output-partitions", type=int, default=20)
    p.add_argument("--min-chunks", type=int, default=500_000,
                   help="Mínimo de chunks esperados (assert).")
    p.add_argument(
        "--cache-intermediate",
        action="store_true",
        help=(
            "Activa .persist() del DataFrame intermedio. Solo recomendable cuando "
            "hay memoria de sobra (cluster). En local[*] suele provocar OOM."
        ),
    )
    p.add_argument(
        "--skip-stats",
        action="store_true",
        help=(
            "Salta el agg de count/nulls previo al write. Útil cuando no quieres "
            "recorrer los datos dos veces. El assert --min-chunks se evalúa contra 0."
        ),
    )
    p.add_argument(
        "--input-schema",
        default="id STRING, text STRING, title STRING",
        help=(
            "DDL del JSON de entrada para evitar la inferencia (mucho más rápido con .jsonl.gz). "
            "Usa 'auto' para inferir esquema (lento)."
        ),
    )
    return p.parse_args(argv)


def _spark_master_rpc_endpoint(master: str) -> tuple[str, int] | None:
    """Primer host:puerto de un master standalone ``spark://h:7077[,h2:7077]``."""
    m = re.match(r"^spark://([^/]+)", master.strip())
    if not m:
        return None
    first = m.group(1).split(",")[0].strip()
    if ":" in first:
        host, port_s = first.rsplit(":", 1)
        try:
            return host.strip(), int(port_s)
        except ValueError:
            return None
    return first.strip(), 7077


def _infer_outbound_ip(remote_host: str, remote_port: int) -> str | None:
    """
    IP local que usaría el SO para alcanzar ``remote_host:remote_port``.
    Sirve como ``spark.driver.host`` cuando los workers están en otra subred.
    """
    if remote_host in ("localhost", "127.0.0.1", "::1"):
        return None
    for kind, connect in (
        ("UDP", lambda s: s.connect((remote_host, remote_port))),
        ("TCP", lambda s: s.connect((remote_host, remote_port))),
    ):
        s: socket.socket | None = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM if kind == "UDP" else socket.SOCK_STREAM)
            s.settimeout(4.0)
            connect(s)
            ip = s.getsockname()[0]
            if ip.startswith("127.") or ip in ("0.0.0.0", "::"):
                return None
            return ip
        except OSError:
            continue
        finally:
            if s is not None:
                try:
                    s.close()
                except OSError:
                    pass
    return None


def infer_driver_host_if_needed(master: str, current: str | None) -> str | None:
    """Si ``master`` es remoto y no hay host, devuelve IP inferida o ``current``."""
    if current:
        return current
    if not str(master).startswith("spark://"):
        return None
    ep = _spark_master_rpc_endpoint(master)
    if not ep:
        return None
    host, port = ep
    return _infer_outbound_ip(host, port)


def _discover_ivy_s3_jars() -> list[Path]:
    """JARs típicos de Ivy para S3A (driver Windows)."""
    base = Path.home() / ".ivy2" / "jars"
    if not base.is_dir():
        return []
    found: dict[str, Path] = {}
    for pattern in (
        "*hadoop-aws*.jar",
        "*aws-java-sdk-bundle*.jar",
        "*wildfly-openssl*.jar",
    ):
        for p in base.glob(pattern):
            if p.is_file():
                found[p.name] = p
    return sorted(found.values(), key=lambda x: x.name)


def _parse_jar_path_list(s: str) -> list[Path]:
    sep = ";" if ";" in s else ":"
    out: list[Path] = []
    for part in s.split(sep):
        p = Path(part.strip().strip('"'))
        if p.is_file():
            out.append(p)
    return out


def _driver_s3_jar_paths(args: argparse.Namespace) -> list[Path]:
    if args.driver_s3_jars:
        return _parse_jar_path_list(args.driver_s3_jars)
    return _discover_ivy_s3_jars()


def _driver_has_class(spark: SparkSession, fqcn: str) -> bool:
    """Compureba si la JVM del driver puede cargar ``fqcn`` (p. ej. S3AFileSystem)."""
    try:
        spark.sparkContext._jvm.java.lang.Class.forName(fqcn)  # type: ignore[attr-defined]
        return True
    except Exception:
        return False


def build_spark(args: argparse.Namespace, use_s3a: bool) -> SparkSession:
    b = SparkSession.builder.appName("ETL_SemanticSearch_Phase1").master(args.master)
    pkgs = (args.jars_packages or "").strip()
    if pkgs and not args.no_spark_packages:
        b = b.config("spark.jars.packages", pkgs)
    if str(args.master).startswith("spark://"):
        b = (
            b.config("spark.files.fetchTimeout", "1200s")
            .config("spark.network.timeout", "1200s")
            .config("spark.rpc.askTimeout", "1200s")
            .config("spark.executor.heartbeatInterval", "60s")
        )
    if args.no_spark_packages and use_s3a:
        ex = (args.executor_s3_jars or "").strip()
        if ex:
            b = b.config("spark.executor.extraClassPath", ex)
            log.info("spark.executor.extraClassPath (S3A): %s", ex)
        elif str(args.master).startswith("spark://"):
            log.warning(
                "Sin --executor-s3-jars: se asume que los workers ya tienen los mismos "
                "JARs en SPARK_HOME/jars. Si las tareas fallan con ClassNotFoundException "
                "en el executor, copia los JARs allí o define --executor-s3-jars."
            )
    if args.driver_host:
        b = b.config("spark.driver.host", args.driver_host).config(
            "spark.driver.bindAddress", "0.0.0.0"
        )
    spark = b.getOrCreate()
    if args.no_spark_packages and use_s3a:
        if not _driver_has_class(spark, "org.apache.hadoop.fs.s3a.S3AFileSystem"):
            dj = _driver_s3_jar_paths(args)
            spark.stop()
            raise RuntimeError(
                "S3AFileSystem NO está en el classpath del DRIVER. En `client` mode "
                "(spark-submit estándar) `spark.driver.extraClassPath` no funciona "
                "tras arrancar el JVM. Copia los JARs a SPARK_HOME/jars del driver, "
                "p. ej. (PowerShell):\n"
                "  Copy-Item -Path '%s' -Destination $env:SPARK_HOME\\jars -Force\n"
                "JARs detectados en Ivy: %s\n"
                "Alternativa: pasa --jars <ruta;ruta;ruta> a `spark-submit` "
                "(NO al script), o vuelve a usar `spark-submit --packages …`."
                % (
                    "', '".join(str(p) for p in dj) if dj else "(ninguno)",
                    [p.name for p in dj] if dj else [],
                )
            )
    return spark


def apply_s3_conf(spark: SparkSession, args: argparse.Namespace) -> None:
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.path.style.access", "true")
    # En Windows el upload por defecto (disk) llama a NativeIO$Windows.access0
    # y requiere winutils.exe/hadoop.dll. Con bytebuffer S3A no toca disco local.
    hconf.set("fs.s3a.fast.upload", "true")
    hconf.set("fs.s3a.fast.upload.buffer", "bytebuffer")
    ep = args.s3_endpoint.strip()
    if ep.startswith("http://"):
        ep = ep[len("http://"):]
        hconf.setBoolean("fs.s3a.connection.ssl.enabled", False)
    elif ep.startswith("https://"):
        ep = ep[len("https://"):]
        hconf.setBoolean("fs.s3a.connection.ssl.enabled", True)
    else:
        hconf.setBoolean("fs.s3a.connection.ssl.enabled", False)
    hconf.set("fs.s3a.endpoint", ep)
    hconf.set("fs.s3a.access.key", args.s3_access_key)
    hconf.set("fs.s3a.secret.key", args.s3_secret_key)
    hconf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def main(argv: list[str] | None = None) -> int:
    cfg = load_project_config(DEFAULT_CONFIG)
    args = parse_args(argv or sys.argv[1:], cfg)

    if not args.run_date:
        log.error("run_date no especificado (--run-date o config.yaml).")
        return 2

    input_path = args.input_json_glob or f"s3a://{args.s3_bucket}/arxiv-metadata-oai-snapshot.json"
    if args.output_dir:
        output_path = args.output_dir
    elif str(args.master).startswith("spark://"):
        # En cluster remoto, una ruta local relativa se interpreta en cada executor,
        # no en el driver. Por defecto, escribimos a MinIO/S3 (visible por todos los nodos).
        output_path = f"s3a://{args.s3_bucket}/features/run_date={args.run_date}"
    else:
        output_path = f"data/features/run_date={args.run_date}"

    if (
        args.output_dir is not None
        and str(args.master).startswith("spark://")
        and not output_path.startswith(("s3a://", "s3://", "hdfs://"))
    ):
        log.warning(
            "output-dir local (%s) con cluster remoto: los executors escribirán en SU disco, "
            "no en el driver. Usa una ruta s3a:// (p. ej. s3a://%s/features/run_date=%s) "
            "si quieres recuperarla aquí.",
            output_path, args.s3_bucket, args.run_date,
        )

    log.info(
        "Iniciando ETL Fase 1 — run_date=%s master=%s input=%s output=%s",
        args.run_date, args.master, input_path, output_path,
    )

    inferred = infer_driver_host_if_needed(args.master, args.driver_host)
    if not args.driver_host and inferred:
        args.driver_host = inferred
        log.info(
            "Inferido spark.driver.host=%s (ruta hacia %s). "
            "Sobrescribe con --driver-host si los workers no alcanzan esta IP.",
            inferred,
            args.master,
        )
    elif str(args.master).startswith("spark://") and not args.driver_host:
        log.warning(
            "No se pudo inferir spark.driver.host hacia el master. "
            "Define --driver-host, spark.driver_host en config.yaml o SPARK_DRIVER_HOST."
        )

    if (
        str(args.master).startswith("spark://")
        and (args.jars_packages or "").strip()
        and not args.no_spark_packages
    ):
        log.warning(
            "Modo cluster con Ivy/packages: el driver puede enviar ~280MB "
            "(aws-java-sdk-bundle) a cada executor. Si el worker se desconecta "
            "(Connection reset / worker lost), copia hadoop-aws-*.jar y "
            "aws-java-sdk-bundle-*.jar desde la cache Ivy (.ivy2/jars) a "
            "SPARK_HOME/jars en cada nodo del cluster y relanza con "
            "--no-spark-packages (y sin --packages en spark-submit)."
        )

    use_s3a = input_path.startswith("s3a://") or output_path.startswith("s3a://")
    try:
        spark = build_spark(args, use_s3a)
    except RuntimeError as e:
        log.error("%s", e)
        return 5
    log.info(
        "spark.driver.host=%s UI=%s",
        spark.sparkContext.getConf().get("spark.driver.host", "(default JVM)"),
        spark.sparkContext.uiWebUrl or "(sin UI)",
    )
    spark.sparkContext.setLogLevel("WARN")
    if input_path.startswith("s3a://") or output_path.startswith("s3a://"):
        apply_s3_conf(spark, args)

    try:
        log.info("Leyendo %s ...", input_path)
        reader = spark.read
        schema_arg = (args.input_schema or "").strip()
        if schema_arg and schema_arg.lower() != "auto":
            try:
                reader = reader.schema(schema_arg)
            except Exception as e:
                log.error("--input-schema inválido: %s (%s)", schema_arg, e)
                return 4
            log.info("Esquema explícito (DDL): %s", schema_arg)
        else:
            log.info("Esquema: inferencia automática (puede ser muy lenta con .jsonl.gz).")
        raw = reader.json(input_path)
        available = set(raw.columns)
        log.info("Columnas en lectura: %s", sorted(available))

        def pick(*candidates: str) -> str | None:
            for c in candidates:
                if c in available:
                    return c
            return None

        id_col = pick("doc_id", "id", "arxiv_id", "paper_id")
        text_col = pick("raw_text", "abstract", "text", "body", "content")
        title_col = pick("title")

        if id_col is None or text_col is None:
            log.error(
                "Esquema incompatible (necesito id y texto). Encontré: %s",
                sorted(available),
            )
            return 3

        log.info("Mapeo de columnas: doc_id<-%s, raw_text<-%s, title<-%s",
                 id_col, text_col, title_col or "(literal null)")

        title_expr = col(title_col) if title_col else lit(None).cast("string")
        metadata = raw.select(
            col(id_col).cast("string").alias("doc_id"),
            title_expr.alias("title"),
            col(text_col).alias("raw_text"),
        )

        log.info("Limpiando texto ...")
        cleaned = (
            metadata
            .filter(col("raw_text").isNotNull())
            .filter(length(col("raw_text")) > 50)
            .withColumn("raw_text", regexp_replace(col("raw_text"), r"\s+", " "))
            .withColumn("raw_text", trim(col("raw_text")))
            .withColumn("raw_text", regexp_replace(col("raw_text"), r"[^\x00-\x7F]+", ""))
        )

        # 1 abstract/documento = 1 chunk (texto corto). Cache solo si se pide
        # explícitamente (en local[*] con poca RAM produce OOM).
        chunked = (
            cleaned
            .withColumn("chunk_id", monotonically_increasing_id())
            .withColumn("source_uri", lit(input_path))
            .withColumn("ingestion_date", lit(args.run_date))
            .select(
                col("doc_id"),
                col("chunk_id"),
                col("title"),
                col("source_uri"),
                col("raw_text"),
                col("ingestion_date"),
            )
        )
        if args.cache_intermediate:
            chunked = chunked.persist(StorageLevel.MEMORY_AND_DISK)
            log.info("Cache intermedio activo (MEMORY_AND_DISK).")

        total = 0
        nulls = 0
        if args.skip_stats:
            log.info("Saltando estadísticas previas (single-pass write).")
        else:
            log.info("Contando chunks (un agg streaming, sin materializar)...")
            stats = chunked.agg(
                count(lit(1)).alias("total"),
                sum_(when(col("raw_text").isNull(), 1).otherwise(0)).alias("nulls"),
            ).collect()[0]
            total = int(stats["total"])
            nulls = int(stats["nulls"] or 0)
            log.info("Total chunks generados : %s", total)
            log.info("Chunks con texto nulo  : %s", nulls)
            assert total >= args.min_chunks, (
                f"Volumen insuficiente: {total} chunks (mínimo {args.min_chunks})"
            )

        log.info("Escribiendo features en %s ...", output_path)
        (
            chunked
            .repartition(args.num_output_partitions)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(output_path)
        )

        log.info("ETL Fase 1 completado exitosamente — %s registros escritos", total)
        if args.cache_intermediate:
            chunked.unpersist()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
