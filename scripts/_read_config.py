"""Lee conf/config.yaml (sin depender de src/project_config.py)."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "conf" / "config.yaml"
PLACEHOLDER = "{network.host}"


def _apply_host(obj: Any, host: str) -> Any:
    if isinstance(obj, dict):
        return {k: _apply_host(v, host) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_apply_host(v, host) for v in obj]
    if isinstance(obj, str) and PLACEHOLDER in obj:
        return obj.replace(PLACEHOLDER, host)
    return obj


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, dict):
        return {}
    host = (os.environ.get("SEMANTIC_SEARCH_HOST") or "").strip()
    if not host:
        net = cfg.get("network")
        if isinstance(net, dict):
            host = str(net.get("host") or "").strip()
    if host:
        cfg = _apply_host(cfg, host)
    return cfg


cfg = load_config()
_network = cfg.get("network") if isinstance(cfg.get("network"), dict) else {}
_minio = cfg.get("minio") if isinstance(cfg.get("minio"), dict) else {}

_spark = cfg.get("spark") if isinstance(cfg.get("spark"), dict) else {}

_VALUES = {
    "host": str(_network.get("host") or "").strip(),
    "run_date": str(cfg.get("run_date") or "").strip(),
    "bucket": str(_minio.get("bucket") or "semantic-raw").strip(),
    "endpoint": str(_minio.get("endpoint") or "http://127.0.0.1:9000").strip(),
    "executor_python": str(_spark.get("executor_python") or "python").strip(),
}

keys = sys.argv[1:] if len(sys.argv) > 1 else list(_VALUES.keys())
for key in keys:
    print(_VALUES.get(key, ""))
