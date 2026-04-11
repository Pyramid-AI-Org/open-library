from __future__ import annotations

from datetime import date
from typing import Any


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
    return default


def _coerce_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        return default
    return parsed if parsed > 0 else default


def normalize_schedule_config(config: dict[str, Any] | None) -> dict[str, Any]:
    raw = config if isinstance(config, dict) else {}
    schedule = raw.get("schedule") if isinstance(raw.get("schedule"), dict) else {}
    return {
        "enabled": _coerce_bool(schedule.get("enabled"), True),
        "interval_days": _coerce_positive_int(schedule.get("interval_days"), 1),
    }


def parse_iso_date(value: str) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def should_run_on_date(
    run_day: date,
    last_successful_run_day: date | None,
    schedule_config: dict[str, Any] | None,
) -> bool:
    normalized = normalize_schedule_config(schedule_config)
    if not normalized["enabled"]:
        return False
    if last_successful_run_day is None:
        return True

    elapsed_days = (run_day - last_successful_run_day).days
    if elapsed_days < 0:
        return False
    return elapsed_days >= int(normalized["interval_days"])
