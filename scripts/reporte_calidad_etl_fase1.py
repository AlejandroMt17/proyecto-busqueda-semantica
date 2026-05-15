#!/usr/bin/env python3
"""
Genera un reporte de calidad en texto plano sobre la salida del ETL (Fase 1).

Ejemplo (CSV locales o gzip, cabecera en la primera línea):

  python scripts/reporte_calidad_etl_fase1.py \\
    --input-glob "data/features/run_date=2026-05-15/*.csv*" \\
    --output data/reports/reporte_calidad_fase1.txt

Para S3A / MinIO, ejecutá con spark-submit y --packages hadoop-aws como en validate_etl_quality.py
y adaptá la lectura (este script usa sesión local por defecto).
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType, ShortType

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "config.yaml"

_src = REPO_ROOT / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from project_config import load_project_config  # noqa: E402


def _numeric_types():
    return (IntegerType, LongType, ShortType, FloatType, DoubleType)


def _nulos_por_columna(df):
    agg = []
    for c in df.columns:
        field = [f for f in df.schema.fields if f.name == c][0]
        col = F.col(c)
        if isinstance(field.dataType, _numeric_types()):
            cond = F.isnan(col) | col.isNull()
        else:
            cond = col.isNull()
        agg.append(F.sum(F.when(cond, 1).otherwise(0)).alias(c))
    return df.select(agg).collect()[0].asDict()


def _vacio_doc_uri_raw(df):
    """Cuenta filas con doc_id / source_uri / raw_text nulos o en blanco (trim)."""
    vacio_doc = df.filter(
        F.col("doc_id").isNull() | (F.length(F.trim(F.col("doc_id"))) == 0)
    ).count()
    vacio_uri = df.filter(
        F.col("source_uri").isNull() | (F.length(F.trim(F.col("source_uri"))) == 0)
    ).count()
    vacio_raw = df.filter(
        F.col("raw_text").isNull() | (F.length(F.trim(F.col("raw_text"))) == 0)
    ).count()
    return vacio_doc, vacio_uri, vacio_raw


def ejecutar_validacion(
    ruta_glob: str,
    ruta_reporte: str,
    *,
    autor: str,
    min_filas_esperadas: int,
    min_longitud_raw_text: int,
) -> int:
    print("[INFO] Iniciando Spark Session para validación de calidad (local[*])...")
    spark = SparkSession.builder.appName("DataQuality_ETL_Fase1").master("local[*]").getOrCreate()

    print(f"[INFO] Leyendo datos desde: {ruta_glob}")
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(ruta_glob)
    except Exception as e:
        print(f"[ERROR] No se pudieron leer los datos. Verificá la ruta o el patrón glob. Detalles: {e}")
        spark.stop()
        return 1

    requeridas = {"doc_id", "chunk_id", "title", "source_uri", "raw_text", "ingestion_date"}
    faltan = requeridas - set(df.columns)
    if faltan:
        print(f"[ERROR] Faltan columnas obligatorias del ETL Fase 1: {sorted(faltan)}")
        spark.stop()
        return 1

    total = df.count()
    nulos = _nulos_por_columna(df)
    vacio_doc, vacio_uri, vacio_raw = _vacio_doc_uri_raw(df)

    bad_chunk = df.filter(F.col("chunk_id").isNull() | (F.col("chunk_id") < 0)).count()

    len_raw = F.length(F.col("raw_text"))
    stats_len = df.select(
        F.min(len_raw).alias("min_len_raw"),
        F.max(len_raw).alias("max_len_raw"),
        F.avg(len_raw.cast("double")).alias("avg_len_raw"),
    ).collect()[0]

    fecha = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    reporte = f"""==================================================
REPORTE DE CALIDAD DE DATOS - FASE 1 (ETL)
Proyecto: búsqueda semántica (chunks para embeddings)
Generado por: {autor}
Fecha de validación: {fecha}
==================================================

1. VOLUMETRÍA
--------------------------------------------------
Total de filas (chunks): {total:,}
Columnas: {", ".join(df.columns)}
Mínimo esperado (--min-filas): {min_filas_esperadas:,}
Estado volumetría: {"OK" if total >= min_filas_esperadas else "ALERTA"}

2. INTEGRIDAD (nulos estrictos por columna Spark isNull / isnan)
--------------------------------------------------
"""
    for col_name, cant in nulos.items():
        ok = cant == 0
        estado = "OK" if ok else "ALERTA"
        reporte += f"- {col_name}: {cant:,} nulos / NaN → {estado}\n"

    reporte += f"""
2b. CLAVES DE NEGOCIO (vacíos tras trim)
--------------------------------------------------
- doc_id vacío: {vacio_doc:,}
- source_uri vacío: {vacio_uri:,}
- raw_text vacío: {vacio_raw:,}

3. REGLAS DE NEGOCIO (chunk_id y longitud de raw_text)
--------------------------------------------------
- Filas con chunk_id nulo o < 0: {bad_chunk:,}
- Longitud raw_text — mín: {stats_len["min_len_raw"]}, máx: {stats_len["max_len_raw"]}, prom: {stats_len["avg_len_raw"] or 0:.2f}
- Umbral mínimo configurado para raw_text (caracteres): {min_longitud_raw_text}

ESTADO FINAL: """

    suma_nulos = sum(nulos.values())
    aprobado = (
        total >= min_filas_esperadas
        and suma_nulos == 0
        and vacio_doc == 0
        and vacio_uri == 0
        and vacio_raw == 0
        and bad_chunk == 0
        and (stats_len["min_len_raw"] or 0) >= min_longitud_raw_text
    )

    if aprobado:
        reporte += "APROBADO (listo para Fase 2: inferencia de embeddings)\n"
    else:
        reporte += "RECHAZADO o REVISAR (ver métricas anteriores y el ETL)\n"

    reporte += "==================================================\n"

    out_path = Path(ruta_reporte)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(reporte, encoding="utf-8")

    print(f"[ÉXITO] Reporte escrito en: {out_path.resolve()}")
    print(reporte)

    spark.stop()
    return 0 if aprobado else 2


def _default_glob_from_config() -> str:
    if not DEFAULT_CONFIG.is_file():
        return str(REPO_ROOT / "data" / "features" / "run_date=*" / "*.csv*")
    cfg = load_project_config(str(DEFAULT_CONFIG))
    run_date = (cfg.get("run_date") or "").strip() or "1970-01-01"
    rel = (cfg.get("paths") or {}).get("chunks") or "data/features"
    base = REPO_ROOT / rel / f"run_date={run_date}"
    return str(base / "*.csv*")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Reporte de calidad textual — salida ETL Fase 1.")
    p.add_argument(
        "--input-glob",
        default="",
        help="Glob CSV/CSV.GZ (ej. data/features/run_date=YYYY-MM-DD/*.csv*). Vacío = inferir de config.yaml.",
    )
    p.add_argument(
        "--output",
        default=str(REPO_ROOT / "data" / "reports" / "reporte_calidad_fase1.txt"),
        help="Ruta del .txt de salida.",
    )
    p.add_argument("--author", default="Equipo técnico", help="Texto 'Generado por: ...'")
    p.add_argument("--min-filas", type=int, default=1, help="Mínimo de filas para no marcar alerta de volumen.")
    p.add_argument(
        "--min-longitud-raw-text",
        type=int,
        default=51,
        help="Mínimo de caracteres en raw_text (el ETL filtra <=50; usar 51 para aprobar solo salidas alineadas al PDF).",
    )
    args = p.parse_args(argv or sys.argv[1:])
    glob_path = args.input_glob.strip() or _default_glob_from_config()
    return ejecutar_validacion(
        glob_path,
        args.output,
        autor=args.author,
        min_filas_esperadas=args.min_filas,
        min_longitud_raw_text=args.min_longitud_raw_text,
    )


if __name__ == "__main__":
    raise SystemExit(main())
