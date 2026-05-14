"""Pruebas ligeras Fase 3 (sin Spark ni Elasticsearch)."""

from __future__ import annotations

import types

import persistence


def test_es_base_url_http():
    args = types.SimpleNamespace(es_use_ssl=False, es_host="10.0.0.5", es_port=9200)
    assert persistence._es_base_url(args) == "http://10.0.0.5:9200"


def test_es_base_url_https():
    args = types.SimpleNamespace(es_use_ssl=True, es_host="es.example", es_port=9243)
    assert persistence._es_base_url(args) == "https://es.example:9243"


def test_es_auth_none_when_incomplete():
    args = types.SimpleNamespace(_es_user=None, _es_password="x")
    assert persistence._es_auth(args) is None


def test_es_auth_tuple_when_both_set():
    args = types.SimpleNamespace(_es_user="u", _es_password="p")
    assert persistence._es_auth(args) == ("u", "p")
