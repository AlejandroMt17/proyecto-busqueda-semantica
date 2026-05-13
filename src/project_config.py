"""
Carga ``conf/config.yaml`` y aplica la IP del despliegue.

- En YAML, usa el marcador ``{network.host}`` en URLs que dependan de la IP.
- Valor por defecto: ``network.host`` en el YAML.
- Sobrescribe sin editar archivos: variable de entorno ``SEMANTIC_SEARCH_HOST``.
"""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

NETWORK_HOST_ENV = "SEMANTIC_SEARCH_HOST"
NETWORK_HOST_PLACEHOLDER = "{network.host}"


def resolve_network_host(cfg: dict[str, Any]) -> str | None:
    env = (os.environ.get(NETWORK_HOST_ENV) or "").strip()
    if env:
        return env
    net = cfg.get("network")
    if not isinstance(net, dict):
        return None
    h = net.get("host")
    if isinstance(h, str) and h.strip():
        return h.strip()
    return None


def _apply_host_templates(obj: Any, host: str) -> Any:
    if isinstance(obj, dict):
        return {k: _apply_host_templates(v, host) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_apply_host_templates(v, host) for v in obj]
    if isinstance(obj, str) and NETWORK_HOST_PLACEHOLDER in obj:
        return obj.replace(NETWORK_HOST_PLACEHOLDER, host)
    return obj


def load_project_config(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    with path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if cfg is None:
        return {}
    if not isinstance(cfg, dict):
        return {}
    cfg = deepcopy(cfg)
    host = resolve_network_host(cfg)
    if host:
        cfg = _apply_host_templates(cfg, host)
        net = cfg.setdefault("network", {})
        if isinstance(net, dict):
            net["host"] = host
    return cfg
