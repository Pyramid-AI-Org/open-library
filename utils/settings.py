from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_settings(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Settings file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError("settings.yaml must be a mapping/object")

    return data
