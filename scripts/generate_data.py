"""
generate_data.py
Descarga y prepara el dataset arXiv para SemanticFlow.
Reproducible con semilla fija (SEED=42).

Fuente: https://www.kaggle.com/datasets/Cornell-University/arxiv
Instrucciones:
  1. Descargar arxiv-metadata-oai-snapshot.json desde Kaggle
  2. Colocar en data/raw/arxiv-metadata-oai-snapshot.json
  3. Ejecutar: python scripts/generate_data.py
"""
import os
import json
import random

SEED = 42
TARGET_DOCS = 520_000
INPUT_FILE = "data/raw/arxiv-metadata-oai-snapshot.json"
OUTPUT_FILE = "data/raw/arxiv_sample.jsonl"

random.seed(SEED)

def main():
    print(f"[INFO] Leyendo {INPUT_FILE}...")
    os.makedirs("data/raw", exist_ok=True)
    selected = []

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            doc = json.loads(line)
            if doc.get("abstract") and doc.get("title"):
                selected.append({
                    "doc_id": doc["id"],
                    "title": doc["title"].replace("\n", " ").strip(),
                    "abstract": doc["abstract"].replace("\n", " ").strip(),
                    "categories": doc.get("categories", ""),
                    "update_date": doc.get("update_date", "")
                })
            if len(selected) >= TARGET_DOCS:
                break
            if i % 100_000 == 0:
                print(f"[INFO] Procesados {i:,} | Seleccionados {len(selected):,}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for doc in selected:
            f.write(json.dumps(doc) + "\n")

    size_gb = os.path.getsize(OUTPUT_FILE) / 1e9
    print(f"[INFO] Guardado: {OUTPUT_FILE}")
    print(f"[INFO] Documentos: {len(selected):,}")
    print(f"[INFO] Tamaño: {size_gb:.2f} GB")

if __name__ == "__main__":
    main()