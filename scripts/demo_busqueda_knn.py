#!/usr/bin/env python3
"""
Demo de búsqueda semántica contra el índice indexado en Fase 3 (Elasticsearch).

1. Codifica la consulta con el mismo tipo de modelo que en Fase 2 (Sentence-Transformers).
2. Llama a Elasticsearch con kNN sobre ``embedding`` y, por defecto, **híbrido**
   (BM25 sobre ``text`` + similitud vectorial), con filtros anti-filas basura del ETL.

Uso (desde la raíz del repo, con .venv activado):

  python scripts/demo_busqueda_knn.py "máquinas con falla de combustión"
  python scripts/demo_busqueda_knn.py "quantum field theory" --run-date 2026-05-15 -k 5
  python scripts/demo_busqueda_knn.py "QFT renormalization" --mode knn --no-strict-ids
  # Español → BM25 en inglés, vector con la frase original:
  python scripts/demo_busqueda_knn.py "ecuaciones diferenciales" --bm25-query "differential equations ODE PDE"

Requiere: Elasticsearch accesible (host/puerto de ``conf/config.yaml`` o ``SEMANTIC_SEARCH_HOST``).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import requests

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

from project_config import load_project_config  # noqa: E402

# Chunks sin abstract siguen en el índice; el kNN los mezcla con scores casi idénticos.
DEFAULT_MIN_TEXT_CHARS = 20


def _arxiv_numeric_doc_id_filter() -> dict[str, Any]:
    """Filas válidas tipo arXiv nuevo: NNNN.NNNN (evita doc_id/chunk rotos del CSV)."""
    return {"regexp": {"doc_id": "[0-9]{4}\\.[0-9]{4}"}}


def _junk_must_not() -> list[dict[str, Any]]:
    return [
        {"wildcard": {"chunk_id": "*s3a*"}},
        {"wildcard": {"doc_id": "*s3a*"}},
        {"wildcard": {"doc_id": '*"*'}},
    ]


def build_data_quality_filter(*, strict_arxiv_id: bool) -> dict[str, Any]:
    must: list[dict[str, Any]] = []
    if strict_arxiv_id:
        must.append(_arxiv_numeric_doc_id_filter())
    clauses: dict[str, Any] = {
        "must_not": _junk_must_not(),
    }
    if must:
        clauses["must"] = must
    return {"bool": clauses}


def build_search_body(
    *,
    bm25_query: str,
    query_vector: list[float],
    fetch_k: int,
    num_candidates: int,
    run_date: str | None,
    mode: str,
    strict_arxiv_id: bool,
    text_boost: float,
    knn_boost: float,
) -> dict[str, Any]:
    base_filter = build_data_quality_filter(strict_arxiv_id=strict_arxiv_id)
    if run_date:
        base_filter["bool"].setdefault("filter", []).append({"term": {"run_date": run_date}})

    knn_filter = base_filter

    knn: dict[str, Any] = {
        "field": "embedding",
        "query_vector": query_vector,
        "k": fetch_k,
        "num_candidates": max(num_candidates, fetch_k * 2),
        "filter": knn_filter,
        "boost": knn_boost,
    }

    body: dict[str, Any] = {
        "_source": ["chunk_key", "chunk_id", "doc_id", "run_date", "text"],
        "size": fetch_k,
    }

    mm = {
        "multi_match": {
            "query": bm25_query,
            "fields": ["text^2", "doc_id"],
            "type": "best_fields",
            "fuzziness": "AUTO",
            "boost": text_boost,
        }
    }

    if mode == "knn":
        body["knn"] = knn
        return body

    if mode == "text":
        body["query"] = {
            "bool": {
                "filter": [base_filter],
                "should": [mm],
                "minimum_should_match": 0,
            }
        }
        return body

    # hybrid: disjunction query + knn; score ≈ text_boost * BM25 + knn_boost * cosine (ES 8.x)
    body["query"] = {
        "bool": {
            "filter": [base_filter],
            "should": [mm],
            "minimum_should_match": 0,
        }
    }
    body["knn"] = knn
    return body


def _hit_text_len(hit: dict[str, Any]) -> int:
    src = hit.get("_source") or {}
    return len(str(src.get("text") or "").strip())


def filter_hits_nonempty_text(hits: list[dict[str, Any]], *, min_chars: int) -> list[dict[str, Any]]:
    if min_chars <= 0:
        return hits
    return [h for h in hits if _hit_text_len(h) >= min_chars]


def main() -> int:
    p = argparse.ArgumentParser(description="Búsqueda kNN / híbrida en Elasticsearch (índice Fase 3).")
    p.add_argument("query", help="Texto de la consulta en lenguaje natural.")
    p.add_argument("--config", default=str(REPO / "conf" / "config.yaml"), help="Ruta a config.yaml.")
    p.add_argument(
        "-k",
        "--k",
        type=int,
        default=10,
        metavar="K",
        dest="k",
        help="Número de hits a devolver (también k del kNN).",
    )
    p.add_argument(
        "--num-candidates",
        type=int,
        default=200,
        help="Candidatos internos del índice vectorial (ES kNN). Subir si los resultados parecen 'aleatorios'.",
    )
    p.add_argument(
        "--run-date",
        default=None,
        help="Si se indica, filtra por run_date (misma partición que el pipeline).",
    )
    p.add_argument(
        "--mode",
        choices=("hybrid", "knn", "text"),
        default="hybrid",
        help="hybrid = BM25(text)+kNN (recomendado); knn = solo vector; text = solo BM25.",
    )
    p.add_argument(
        "--no-strict-ids",
        action="store_true",
        help="No exigir doc_id tipo NNNN.NNNN (por si indexas IDs distintos al estilo arXiv nuevo).",
    )
    p.add_argument(
        "--text-boost",
        type=float,
        default=0.65,
        help="Peso relativo del texto (BM25) en modo hybrid.",
    )
    p.add_argument(
        "--vector-boost",
        type=float,
        default=0.35,
        help="Peso relativo del kNN en modo hybrid.",
    )
    p.add_argument(
        "--bm25-query",
        default=None,
        metavar="TEXT",
        help="Texto solo para BM25 (multi_match). El embedding sigue usando la consulta posicional. "
        "Útil: consulta en español + sinónimos en inglés para este corpus.",
    )
    p.add_argument(
        "--min-text-chars",
        type=int,
        default=DEFAULT_MIN_TEXT_CHARS,
        help="Descarta hits cuyo abstract tenga menos caracteres (no vacío / basura). 0 = no filtrar.",
    )
    p.add_argument(
        "--allow-empty-text",
        action="store_true",
        help="No filtrar abstracts vacíos (equivale a --min-text-chars 0 y sin oversampling).",
    )
    args = p.parse_args()

    cfg = load_project_config(args.config)
    es_cfg = cfg.get("elasticsearch") or {}
    model_cfg = cfg.get("model") or {}

    host = str(es_cfg.get("host") or "127.0.0.1").strip()
    port = int(es_cfg.get("port") or 9200)
    index = str(es_cfg.get("index") or "documents").strip()
    use_ssl = bool(es_cfg.get("use_ssl", False))
    user = (es_cfg.get("user") or "").strip() or None
    password = (es_cfg.get("password") or "").strip() or None
    model_name = str(model_cfg.get("name") or "sentence-transformers/all-MiniLM-L6-v2").strip()

    scheme = "https" if use_ssl else "http"
    base = f"{scheme}://{host}:{port}"
    url = f"{base.rstrip('/')}/{index}/_search"
    auth = (user, password) if user and password else None

    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    vec = model.encode(args.query, normalize_embeddings=True).tolist()

    strict_ids = not args.no_strict_ids
    bm25_q = (args.bm25_query or "").strip() or args.query
    min_chars = 0 if args.allow_empty_text else max(0, int(args.min_text_chars))
    fetch_k = args.k if min_chars == 0 else min(120, max(args.k * 8, 40, args.k))

    body = build_search_body(
        bm25_query=bm25_q,
        query_vector=vec,
        fetch_k=fetch_k,
        num_candidates=args.num_candidates,
        run_date=args.run_date,
        mode=args.mode,
        strict_arxiv_id=strict_ids,
        text_boost=args.text_boost,
        knn_boost=args.vector_boost,
    )

    r = requests.post(url, json=body, auth=auth, timeout=120)
    if r.status_code >= 400:
        sys.stderr.write(f"HTTP {r.status_code}: {r.text[:2000]}\n")
        return 1

    data = r.json()
    hits = data.get("hits", {}).get("hits", [])
    raw_n = len(hits)
    hits = filter_hits_nonempty_text(hits, min_chars=min_chars)[: args.k]
    if args.bm25_query:
        bm25_note = f"BM25: {bm25_q!r}\n"
    else:
        bm25_note = ""
    filt_note = ""
    if min_chars > 0 and raw_n > len(hits):
        filt_note = f"(filtrados abstracts < {min_chars} chars; pedidos a ES: {fetch_k})\n"
    empty_warn = ""
    if min_chars > 0 and not hits and raw_n > 0:
        empty_warn = (
            f"AVISO: ningún hit con abstract ≥ {min_chars} caracteres entre {raw_n} devueltos. "
            f"Prueba --num-candidates 400 o --min-text-chars 0.\n"
        )
    print(
        f"Consulta: {args.query!r}\n"
        f"{bm25_note}"
        f"Modelo: {model_name}\n"
        f"Índice: {index}\n"
        f"Modo: {args.mode} (strict_arxiv_id={strict_ids})\n"
        f"{filt_note}"
        f"{empty_warn}"
        f"Resultados: {len(hits)}\n"
    )
    for i, h in enumerate(hits, 1):
        src = h.get("_source") or {}
        score = h.get("_score")
        raw_text = src.get("text") or ""
        text = raw_text[:280].replace("\n", " ")
        print(f"--- #{i} score={score}")
        print(f"    doc_id={src.get('doc_id')} chunk_id={src.get('chunk_id')} chunk_key={src.get('chunk_key')}")
        print(f"    run_date={src.get('run_date')}")
        print(f"    text: {text}{'…' if len(raw_text) > 280 else ''}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
