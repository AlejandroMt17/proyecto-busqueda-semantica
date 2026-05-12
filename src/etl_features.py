import logging
import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, trim, lower,
    monotonically_increasing_id, lit,
    current_date, regexp_replace, length
)

# ─── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── Configuración ─────────────────────────────────────────
def load_config(path="conf/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)

# ─── Main ──────────────────────────────────────────────────
def main():
    cfg = load_config()
    run_date = sys.argv[1] if len(sys.argv) > 1 else cfg["run_date"]

    log.info(f"Iniciando ETL Fase 1 — run_date={run_date}")

    spark = SparkSession.builder \
        .appName("ETL_SemanticSearch_Phase1") \
        .config("spark.hadoop.fs.s3a.endpoint",
                cfg["minio"]["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key",
                cfg["minio"]["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key",
                cfg["minio"]["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Leer metadata de arXiv ──────────────────────────
    log.info("Leyendo arxiv-metadata-oai-snapshot.json ...")
    metadata = spark.read.json(
        f"s3a://{cfg['minio']['bucket']}/arxiv-metadata-oai-snapshot.json"
    ).select(
        col("id").alias("doc_id"),
        col("title"),
        col("abstract").alias("raw_text"),
        col("categories"),
        col("update_date")
    )
    log.info(f"Registros leídos de metadata: {metadata.count()}")

    # ── 2. Limpiar texto ───────────────────────────────────
    log.info("Limpiando texto ...")
    cleaned = metadata \
        .filter(col("raw_text").isNotNull()) \
        .filter(length(col("raw_text")) > 50) \
        .withColumn("raw_text",
            regexp_replace(col("raw_text"), r"\s+", " ")) \
        .withColumn("raw_text",
            trim(col("raw_text"))) \
        .withColumn("raw_text",
            regexp_replace(col("raw_text"), r"[^\x00-\x7F]+", ""))

    # ── 3. Chunking (256 palabras por chunk) ───────────────
    log.info("Segmentando en chunks ...")
    words = cleaned.withColumn(
        "words", split(col("raw_text"), " ")
    )

    # Para abstracts de arXiv el texto es corto,
    # cada abstract = 1 chunk
    chunked = words.withColumn(
        "chunk_id",
        monotonically_increasing_id()
    ).withColumn(
        "source_uri",
        lit(f"s3a://{cfg['minio']['bucket']}/arxiv-metadata-oai-snapshot.json")
    ).withColumn(
        "ingestion_date", lit(run_date)
    ).select(
        col("doc_id"),
        col("chunk_id"),
        col("title"),
        col("source_uri"),
        col("raw_text"),
        col("ingestion_date")
    )

    # ── 4. Validación ──────────────────────────────────────
    total = chunked.count()
    nulls = chunked.filter(col("raw_text").isNull()).count()
    log.info(f"Total chunks generados : {total}")
    log.info(f"Chunks con texto nulo  : {nulls}")
    assert total > 500_000, f"Volumen insuficiente: {total} chunks"

    # ── 5. Escribir CSVs particionados ─────────────────────
    output_path = f"data/features/run_date={run_date}"
    log.info(f"Escribiendo features en {output_path} ...")

    chunked.repartition(20) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

    log.info(f"ETL Fase 1 completado exitosamente — {total} registros escritos")
    spark.stop()

if __name__ == "__main__":
    main()