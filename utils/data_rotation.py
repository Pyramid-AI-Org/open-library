from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from utils.time import utc_now, utc_date_yyyymmdd


@dataclass(frozen=True)
class RotationResult:
    archived: bool
    archived_path: Path | None


def _record_key(rec: dict[str, Any]) -> tuple[str, str] | None:
    source = rec.get("source")
    url = rec.get("url")
    if not isinstance(source, str) or not isinstance(url, str):
        return None
    s = source.strip()
    u = url.strip()
    if not s or not u:
        return None
    return (s, u)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(raw, dict):
        return raw
    return {}


def _latest_summary_run_date(data_root: Path) -> str:
    summary = _read_json(data_root / "latest" / "summary.json")
    raw = str(summary.get("run_date_utc") or "").strip()
    return raw[:10] if raw else ""


def _write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _iter_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(
        records, key=lambda r: (str(r.get("url") or ""), str(r.get("source") or ""))
    )
    with path.open("w", encoding="utf-8") as f:
        for rec in ordered:
            f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True))
            f.write("\n")


def _records_map(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for rec in _iter_jsonl(path):
        key = _record_key(rec)
        if key is None:
            continue
        out[key] = rec
    return out


def _month_dir(data_root: Path, yyyy: str, mm: str) -> Path:
    return data_root / "archive_v2" / yyyy / mm


def _day_dir(data_root: Path, yyyy: str, mm: str, dd: str) -> Path:
    return _month_dir(data_root, yyyy, mm) / "days" / dd


def _day_files(data_root: Path, yyyy: str, mm: str, dd: str) -> tuple[Path, Path, Path]:
    day_dir = _day_dir(data_root, yyyy, mm, dd)
    return (day_dir / "added.jsonl", day_dir / "removed.jsonl", day_dir / "meta.json")


def _base_file(data_root: Path, yyyy: str, mm: str) -> Path:
    return _month_dir(data_root, yyyy, mm) / "base.jsonl"


def _base_meta_file(data_root: Path, yyyy: str, mm: str) -> Path:
    return _month_dir(data_root, yyyy, mm) / "base.meta.json"


def _calc_added_removed(
    base_map: dict[tuple[str, str], dict[str, Any]],
    full_map: dict[tuple[str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    added: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []

    for key, rec in full_map.items():
        base_rec = base_map.get(key)
        if base_rec != rec:
            added.append(rec)

    for key, rec in base_map.items():
        if key not in full_map:
            removed.append(rec)

    return added, removed


def _write_day_delta(
    data_root: Path,
    yyyy: str,
    mm: str,
    dd: str,
    base_version: int,
    added: list[dict[str, Any]],
    removed: list[dict[str, Any]],
) -> Path:
    added_path, removed_path, meta_path = _day_files(data_root, yyyy, mm, dd)
    _write_jsonl(added_path, added)
    _write_jsonl(removed_path, removed)

    _write_json(
        meta_path,
        {
            "date": f"{yyyy}-{mm}-{dd}",
            "format": "v2-delta",
            "base_version": base_version,
            "base_path": _base_file(data_root, yyyy, mm)
            .relative_to(data_root)
            .as_posix(),
            "added_path": added_path.relative_to(data_root).as_posix(),
            "removed_path": removed_path.relative_to(data_root).as_posix(),
            "rows_added": len(added),
            "rows_removed": len(removed),
            "bytes": added_path.stat().st_size + removed_path.stat().st_size,
        },
    )
    return meta_path


def _day_dirs_in_month(data_root: Path, yyyy: str, mm: str) -> list[Path]:
    days_root = _month_dir(data_root, yyyy, mm) / "days"
    if not days_root.exists():
        return []
    return sorted(
        [
            p
            for p in days_root.iterdir()
            if p.is_dir() and len(p.name) == 2 and p.name.isdigit()
        ],
        key=lambda p: p.name,
    )


def _reconstruct_day_map(
    data_root: Path,
    yyyy: str,
    mm: str,
    dd: str,
    base_map: dict[tuple[str, str], dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    added_path, removed_path, _ = _day_files(data_root, yyyy, mm, dd)
    out: dict[tuple[str, str], dict[str, Any]] = dict(base_map)
    for rec in _iter_jsonl(removed_path):
        key = _record_key(rec)
        if key is None:
            continue
        out.pop(key, None)
    for rec in _iter_jsonl(added_path):
        key = _record_key(rec)
        if key is None:
            continue
        out[key] = rec
    return out


def _intersection_common(
    records_by_day: list[dict[tuple[str, str], dict[str, Any]]],
) -> dict[tuple[str, str], dict[str, Any]]:
    if not records_by_day:
        return {}
    common: dict[tuple[str, str], dict[str, Any]] = dict(records_by_day[0])
    for day_map in records_by_day[1:]:
        next_common: dict[tuple[str, str], dict[str, Any]] = {}
        for key, rec in common.items():
            other = day_map.get(key)
            if other == rec:
                next_common[key] = rec
        common = next_common
    return common


def _rebase_month_if_needed(
    data_root: Path,
    run_date: str,
    mid_month_refresh_day: int,
) -> None:
    yyyy, mm, dd = run_date.split("-")
    day_num = int(dd)
    if day_num != mid_month_refresh_day:
        return

    base_path = _base_file(data_root, yyyy, mm)
    base_meta_path = _base_meta_file(data_root, yyyy, mm)
    if not base_path.exists():
        return

    base_meta = _read_json(base_meta_path)
    if str(base_meta.get("last_rebased_on") or "") == run_date:
        return

    base_map = _records_map(base_path)
    day_dirs = _day_dirs_in_month(data_root, yyyy, mm)
    if not day_dirs:
        return

    daily_full_maps: list[dict[tuple[str, str], dict[str, Any]]] = []
    for day_dir in day_dirs:
        day_dd = day_dir.name
        daily_full_maps.append(
            _reconstruct_day_map(data_root, yyyy, mm, day_dd, base_map)
        )

    refreshed_base = _intersection_common(daily_full_maps)
    refreshed_base_records = list(refreshed_base.values())
    _write_jsonl(base_path, refreshed_base_records)

    base_version = int(base_meta.get("base_version") or 1) + 1
    for idx, day_dir in enumerate(day_dirs):
        day_dd = day_dir.name
        full_map = daily_full_maps[idx]
        added, removed = _calc_added_removed(refreshed_base, full_map)
        _write_day_delta(
            data_root=data_root,
            yyyy=yyyy,
            mm=mm,
            dd=day_dd,
            base_version=base_version,
            added=added,
            removed=removed,
        )

    _write_json(
        base_meta_path,
        {
            "format": "v2-base",
            "month": f"{yyyy}-{mm}",
            "base_version": base_version,
            "last_rebased_on": run_date,
            "mid_month_refresh_day": mid_month_refresh_day,
            "rows": len(refreshed_base_records),
            "bytes": base_path.stat().st_size,
        },
    )


def archive_previous_latest(
    data_root: Path,
    run_date: str | None = None,
    *,
    mid_month_refresh_day: int = 15,
) -> RotationResult:
    """Archive previous latest as month-base deltas.

    V2 layout (new runs only):
      data/archive_v2/YYYY/MM/base.jsonl
      data/archive_v2/YYYY/MM/base.meta.json
      data/archive_v2/YYYY/MM/days/DD/added.jsonl
      data/archive_v2/YYYY/MM/days/DD/removed.jsonl
      data/archive_v2/YYYY/MM/days/DD/meta.json

    Existing legacy archive files under data/archive/ are not modified.
    """

    latest_path = data_root / "latest" / "urls.jsonl"
    if not latest_path.exists():
        return RotationResult(archived=False, archived_path=None)

    if run_date is None:
        run_date = utc_date_yyyymmdd(utc_now())

    latest_run_date = _latest_summary_run_date(data_root)
    if latest_run_date == run_date:
        return RotationResult(archived=False, archived_path=None)

    yyyy, mm, dd = run_date.split("-")
    month_dir = _month_dir(data_root, yyyy, mm)
    month_dir.mkdir(parents=True, exist_ok=True)

    base_path = _base_file(data_root, yyyy, mm)
    base_meta_path = _base_meta_file(data_root, yyyy, mm)

    latest_map = _records_map(latest_path)

    if not base_path.exists():
        # First archive entry in a month initializes the base and writes an empty delta day.
        shutil.move(str(latest_path), str(base_path))
        _write_json(
            base_meta_path,
            {
                "format": "v2-base",
                "month": f"{yyyy}-{mm}",
                "base_version": 1,
                "last_rebased_on": "",
                "mid_month_refresh_day": mid_month_refresh_day,
                "rows": len(latest_map),
                "bytes": base_path.stat().st_size,
            },
        )
        meta_path = _write_day_delta(
            data_root=data_root,
            yyyy=yyyy,
            mm=mm,
            dd=dd,
            base_version=1,
            added=[],
            removed=[],
        )
        _rebase_month_if_needed(data_root, run_date, mid_month_refresh_day)
        return RotationResult(archived=True, archived_path=meta_path)

    base_map = _records_map(base_path)
    base_meta = _read_json(base_meta_path)
    base_version = int(base_meta.get("base_version") or 1)
    added, removed = _calc_added_removed(base_map, latest_map)
    meta_path = _write_day_delta(
        data_root=data_root,
        yyyy=yyyy,
        mm=mm,
        dd=dd,
        base_version=base_version,
        added=added,
        removed=removed,
    )

    latest_path.unlink(missing_ok=True)
    _rebase_month_if_needed(data_root, run_date, mid_month_refresh_day)
    return RotationResult(archived=True, archived_path=meta_path)
