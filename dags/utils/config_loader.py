"""Configuration loader with env-var overrides.

Precedence: environment variable SETTINGS_<SECTION>_<KEY> > settings.yaml
Usage:
    from utils.config_loader import load_settings, load_entities
    settings = load_settings()
    entities = load_entities()
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR = Path(os.getenv("DEP_CONFIG_DIR", Path(__file__).resolve().parents[2] / "config"))


def _deep_get(d: dict[str, Any], dotted: str) -> Any:
    cur: Any = d
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _coerce(raw: str, current: Any) -> Any:
    """Coerce string env values to the type already present in settings.yaml."""
    if isinstance(current, bool):
        return raw.strip().lower() in ("1", "true", "yes")
    if isinstance(current, int):
        return int(raw)
    if isinstance(current, float):
        return float(raw)
    return raw


def load_settings(config_dir: Path | None = None) -> dict[str, Any]:
    path = (config_dir or CONFIG_DIR) / "settings.yaml"
    with open(path, encoding="utf-8") as fh:
        settings = yaml.safe_load(fh)

    # ENV overrides: SETTINGS_S3__RAW_BUCKET -> s3.raw_bucket (double underscore = nesting)
    for env_key, value in os.environ.items():
        if not env_key.startswith("SETTINGS_"):
            continue
        dotted = env_key[len("SETTINGS_"):].lower().replace("__", ".")
        current = _deep_get(settings, dotted)
        settings_keys = dotted.split(".")
        target = settings
        for k in settings_keys[:-1]:
            target = target.setdefault(k, {})
        target[settings_keys[-1]] = _coerce(value, current) if current is not None else value
    return settings


def load_entities(config_dir: Path | None = None) -> dict[str, Any]:
    path = (config_dir or CONFIG_DIR) / "entities.yaml"
    with open(path, encoding="utf-8") as fh:
        entities = yaml.safe_load(fh)
    # Merge defaults into each entity (entity value wins)
    defaults = entities.get("defaults", {})
    for name, cfg in entities.get("entities", {}).items():
        merged = {**defaults, **(cfg or {})}
        merged["columns"] = cfg.get("columns", [])
        entities["entities"][name] = merged
    return entities["entities"]
