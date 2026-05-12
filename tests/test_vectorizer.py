"""Pruebas ligeras Fase 2 (sin cargar SentenceTransformer)."""

from __future__ import annotations

from spark_vectorizer import chunk_row_key


def test_chunk_row_key_stable():
    assert chunk_row_key("abc123", 0) == "abc123_0"
    assert chunk_row_key("x", 12) == "x_12"
