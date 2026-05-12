"""Pruebas unitarias Fase 1 (sin cluster Spark)."""

from __future__ import annotations

import etl_features


def test_clean_boilerplate_strips_noise():
    s = "Hola\r\nmundo   \t  fin"
    assert "Hola" in etl_features.clean_boilerplate(s)
    assert "\r" not in etl_features.clean_boilerplate(s)


def test_filter_chunks_by_min_chars_keeps_long():
    parts = ["x" * 40, "ab"]
    out = etl_features.filter_chunks_by_min_chars(parts, min_chars=30)
    assert out == ["x" * 40]


def test_filter_chunks_by_min_chars_fallback_when_all_short():
    parts = ["ab", "cd"]
    out = etl_features.filter_chunks_by_min_chars(parts, min_chars=30)
    assert out == ["ab", "cd"]


def test_chunk_by_chars_splits():
    t = "uno dos tres cuatro cinco seis siete"
    chunks = etl_features.chunk_by_chars(t, max_chars=10)
    assert len(chunks) >= 2
    joined = " ".join(chunks)
    assert "uno" in joined and "siete" in joined


def test_title_from_path_uses_basename():
    assert "foo.pdf" in etl_features.title_from_path("s3a://bucket/prefix/foo.pdf")


def test_normalize_unicode_nfkc():
    assert etl_features.normalize_unicode("①") == "1"
