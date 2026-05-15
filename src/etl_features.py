#!/usr/bin/env python3
"""
Fase 1 — ETL de features para búsqueda semántica.

Lee metadata de arXiv (JSON o JSONL[.gz]) desde MinIO/S3 vía s3a y escribe
chunks limpios en CSV particionado por `run_date`. Los chunks se generan por
ventana deslizante en **tokens** del tokenizer del modelo de embeddings
(default 256 con solapamiento 32; ver sección 5 del PDF del proyecto).

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
import unicodedata
from pathlib import Path

from project_config import load_project_config
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lit, regexp_replace, length, count, sum as sum_, when,
    explode, pandas_udf,
)
from pyspark.sql.types import (
    ArrayType, IntegerType, StringType, StructField, StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "config.yaml"
DEFAULT_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

DEFAULT_MAX_TOKENS = 256
DEFAULT_OVERLAP_TOKENS = 32
DEFAULT_MIN_CHUNK_TOKENS = 30
DEFAULT_MIN_CHUNK_CHARS = 30
DEFAULT_TOKENIZER = "sentence-transformers/all-MiniLM-L6-v2"

CHUNK_STRUCT = StructType([
    StructField("chunk_id", IntegerType(), nullable=False),
    StructField("raw_text", StringType(), nullable=False),
    StructField("n_tokens", IntegerType(), nullable=False),
])


def normalize_unicode(text: str | None) -> str:
    return unicodedata.normalize("NFKC", text or "")


_BOILER_LINE = re.compile(r"^\s*[-=_*•·]{3,}\s*$", flags=re.MULTILINE)
_MANY_SPACES = re.compile(r"[ \t\u00a0]+")
_MANY_BLANKLINES = re.compile(r"\n{3,}")


def clean_boilerplate(text: str | None) -> str:
    """Limpieza ligera previa al chunking: normaliza saltos y colapsa whitespace."""
    if not text:
        return ""
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    t = _BOILER_LINE.sub("", t)
    t = _MANY_SPACES.sub(" ", t)
    t = _MANY_BLANKLINES.sub("\n\n", t)
    return t.strip()


def title_from_path(path: str | None) -> str:
    if not path:
        return ""
    return path.rstrip("/").rsplit("/", 1)[-1]


def chunk_by_chars(text: str | None, max_chars: int) -> list[str]:
    """Fallback simple por caracteres (sólo usado como red de seguridad)."""
    t = (text or "").strip()
    if not t:
        return []
    if max_chars <= 0 or len(t) <= max_chars:
        return [t]
    return [t[i : i + max_chars] for i in range(0, len(t), max_chars)]


def filter_chunks_by_min_chars(parts: list[str], min_chars: int) -> list[str]:
    """Quita chunks < min_chars; si todos son cortos, devuelve los no vacíos."""
    keep = [p for p in parts if len(p) >= min_chars]
    if keep:
        return keep
    return [p for p in parts if p]


def _word_offsets(text: str) -> list[tuple[int, int]]:
    return [(m.start(), m.end()) for m in re.finditer(r"\S+", text or "")]


def _load_hf_tokenizer(model_name: str):
    """Carga el tokenizer HF (offsets-aware). Devuelve None si no está disponible.

    Desactiva truncation/padding internos: queremos ver TODOS los tokens del texto
    para poder ventanear con --max-tokens-per-chunk; el modelo de Fase 2 ya
    truncará por su cuenta a su `max_seq_length`.
    """
    try:
        from tokenizers import Tokenizer  # type: ignore
    except Exception:
        return None
    try:
        tok = Tokenizer.from_pretrained(model_name)
    except Exception:
        return None
    try:
        tok.no_truncation()
    except Exception:
        pass
    try:
        tok.no_padding()
    except Exception:
        pass
    return tok


def _token_offsets(text: str, tokenizer) -> list[tuple[int, int]]:
    """Lista de offsets (start, end) en `text`, en orden, sin tokens especiales."""
    if not text:
        return []
    if tokenizer is None:
        return _word_offsets(text)
    enc = tokenizer.encode(text, add_special_tokens=False)
    return [(s, e) for s, e in enc.offsets if e > s]


def _split_text_into_chunks(
    text: str,
    offsets: list[tuple[int, int]],
    max_tokens: int,
    overlap_tokens: int,
    min_chunk_tokens: int,
    min_chunk_chars: int,
) -> list[dict]:
    """Ventana deslizante sobre offsets de tokens. Devuelve lista de chunks."""
    text = text or ""
    n = len(offsets)
    if n == 0 or not text.strip():
        return []
    max_tokens = max(1, max_tokens)
    overlap = max(0, min(overlap_tokens, max_tokens - 1))
    stride = max(1, max_tokens - overlap)

    out: list[dict] = []
    i = 0
    while i < n:
        j = min(i + max_tokens, n)
        start_char = offsets[i][0]
        end_char = offsets[j - 1][1]
        piece = text[start_char:end_char].strip()
        n_tokens = j - i
        if n_tokens >= min_chunk_tokens and len(piece) >= min_chunk_chars and piece:
            out.append({"chunk_id": len(out), "raw_text": piece, "n_tokens": int(n_tokens)})
        if j == n:
            break
        i += stride

    if not out:
        full = text.strip()
        if full and len(full) >= min_chunk_chars:
            out.append({"chunk_id": 0, "raw_text": full, "n_tokens": int(min(n, max_tokens))})
    return out


def make_chunk_udf(
    tokenizer_model: str,
    max_tokens: int,
    overlap_tokens: int,
    min_chunk_tokens: int,
    min_chunk_chars: int,
    use_word_tokenization: bool,
):
    """Pandas UDF: raw_text (string) -> array<struct<chunk_id,raw_text,n_tokens>>."""

    @pandas_udf(ArrayType(CHUNK_STRUCT))
    def _chunk(texts):  # pd.Series -> pd.Series
        import pandas as pd

        tok = None if use_word_tokenization else _load_hf_tokenizer(tokenizer_model)
        results = []
        for raw in texts.fillna("").astype(str):
            t = raw
            offs = _token_offsets(t, tok)
            results.append(_split_text_into_chunks(
                t, offs, max_tokens, overlap_tokens, min_chunk_tokens, min_chunk_chars,
            ))
        return pd.Series(results)

    return _chunk


def parse_args(argv: list[str], cfg: dict) -> argparse.Namespace:
    minio = cfg.get("minio", {})
    spark_cfg = cfg.get("spark", {})
    model_cfg = cfg.get("model", {}) or {}
    cfg_driver_host = spark_cfg.get("driver_host")
    if isinstance(cfg_driver_host, str):
        cfg_driver_host = cfg_driver_host.strip() or None
    if not cfg_driver_host:
        cfg_driver_host = os.environ.get("SPARK_DRIVER_HOST") or None

    cfg_model_name = (model_cfg.get("name") or "").strip()
    cfg_tokenizer = (
        f"sentence-transformers/{cfg_model_name}"
        if cfg_model_name and "/" not in cfg_model_name
        else (cfg_model_name or DEFAULT_TOKENIZER)
    )

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

    p.add_argument(
        "--max-tokens-per-chunk",
        type=int,
        default=int(model_cfg.get("max_tokens") or DEFAULT_MAX_TOKENS),
        help="Tamaño máximo de cada chunk en tokens del tokenizer (default 256).",
    )
    p.add_argument(
        "--overlap-tokens",
        type=int,
        default=int(model_cfg.get("overlap_tokens") or DEFAULT_OVERLAP_TOKENS),
        help="Solapamiento entre chunks consecutivos en tokens (default 32).",
    )
    p.add_argument(
        "--min-chunk-tokens",
        type=int,
        default=int(model_cfg.get("min_chunk_tokens") or DEFAULT_MIN_CHUNK_TOKENS),
        help="Mínimo de tokens para conservar un chunk (default 30).",
    )
    p.add_argument(
        "--min-chunk-chars",
        type=int,
        default=DEFAULT_MIN_CHUNK_CHARS,
        help="Mínimo de caracteres para conservar un chunk (default 30).",
    )
    p.add_argument(
        "--tokenizer-model",
        default=cfg_tokenizer,
        help=(
            "Repo HF del tokenizer para contar tokens (default: el del modelo de Fase 2). "
            "La primera ejecución descarga `tokenizer.json` desde Hugging Face."
        ),
    )
    p.add_argument(
        "--word-tokenization",
        action="store_true",
        help=(
            "Forzar tokenización por palabras (no descarga modelo HF). "
            "Útil offline; menos exacto en conteo de tokens."
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
    driver_py = (os.environ.get("PYSPARK_DRIVER_PYTHON") or os.environ.get("PYSPARK_PYTHON") or "").strip()
    if driver_py:
        b = b.config("spark.pyspark.driver.python", driver_py)
    if str(args.master).startswith("spark://"):
        exec_py = (os.environ.get("SPARK_EXECUTOR_PYTHON") or "python").strip()
        b = b.config("spark.pyspark.python", exec_py)
        log.info("spark.pyspark.python=%s (executors: PATH por nodo)", exec_py)
    elif driver_py:
        b = b.config("spark.pyspark.python", driver_py)
        log.info("spark.pyspark.python=%s (local: mismo que driver)", driver_py)
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

        log.info(
            "Segmentando en chunks (max_tokens=%s, overlap=%s, min_tokens=%s, tokenizer=%s%s)",
            args.max_tokens_per_chunk,
            args.overlap_tokens,
            args.min_chunk_tokens,
            args.tokenizer_model,
            " | word-fallback" if args.word_tokenization else "",
        )
        chunk_fn = make_chunk_udf(
            tokenizer_model=args.tokenizer_model,
            max_tokens=args.max_tokens_per_chunk,
            overlap_tokens=args.overlap_tokens,
            min_chunk_tokens=args.min_chunk_tokens,
            min_chunk_chars=args.min_chunk_chars,
            use_word_tokenization=args.word_tokenization,
        )
        chunked = (
            cleaned
            .withColumn("_chunks", chunk_fn(col("raw_text")))
            .withColumn("_c", explode(col("_chunks")))
            .withColumn("source_uri", lit(input_path))
            .withColumn("ingestion_date", lit(args.run_date))
            .select(
                col("doc_id"),
                col("_c.chunk_id").alias("chunk_id"),
                col("title"),
                col("source_uri"),
                col("_c.raw_text").alias("raw_text"),
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

        if args.skip_stats:
            log.info(
                "ETL Fase 1 completado — escritura OK (sin conteo previo; --skip-stats activo).",
            )
        else:
            log.info("ETL Fase 1 completado exitosamente — %s registros escritos", total)
        if args.cache_intermediate:
            chunked.unpersist()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
