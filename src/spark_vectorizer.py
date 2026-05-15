"""
Compatibilidad: el manual nombra Fase 2 como ``batch_inference.py``.

Usa ``python -m batch_inference`` o ``spark-submit ... src/batch_inference.py``.
"""

from batch_inference import chunk_row_key, main

__all__ = ["chunk_row_key", "main"]

if __name__ == "__main__":
    raise SystemExit(main())
# georg - configuracion Semana 3 
