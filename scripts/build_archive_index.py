from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from utils.time import utc_now


@dataclass(frozen=True)
class ArchiveEntry:
    date: str  # YYYY-MM-DD
    path: str  # path used by the viewer for selection/fetch
    bytes: int
    format: str  # legacy-full | v2-delta
    base_path: str | None = None
    added_path: str | None = None
    removed_path: str | None = None
    base_version: int | None = None


def _iter_legacy_archives(data_root: Path) -> list[ArchiveEntry]:
    archive_root = data_root / "archive"
    if not archive_root.exists():
        return []

    out: list[ArchiveEntry] = []
    # Expected: archive/YYYY/MM/DD/urls.jsonl
    for urls_path in archive_root.glob("*/ */ */urls.jsonl".replace(" ", "")):
        try:
            rel = urls_path.relative_to(data_root).as_posix()
        except ValueError:
            continue

        parts = urls_path.parts
        # .../archive/YYYY/MM/DD/urls.jsonl
        try:
            yyyy = parts[-4]
            mm = parts[-3]
            dd = parts[-2]
        except Exception:
            continue

        date = f"{yyyy}-{mm}-{dd}"
        size = urls_path.stat().st_size
        out.append(
            ArchiveEntry(
                date=date,
                path=rel,
                bytes=size,
                format="legacy-full",
            )
        )

    return out


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(obj, dict):
        return obj
    return {}


def _iter_v2_archives(data_root: Path) -> list[ArchiveEntry]:
    root = data_root / "archive_v2"
    if not root.exists():
        return []

    out: list[ArchiveEntry] = []
    for meta_path in root.glob("*/*/days/*/meta.json"):
        meta = _read_json(meta_path)
        if not meta:
            continue

        date = str(meta.get("date") or "").strip()
        if not date:
            parts = meta_path.parts
            try:
                yyyy = parts[-5]
                mm = parts[-4]
                dd = parts[-2]
                date = f"{yyyy}-{mm}-{dd}"
            except Exception:
                continue

        try:
            rel_meta = meta_path.relative_to(data_root).as_posix()
        except ValueError:
            continue

        base_path = str(meta.get("base_path") or "").strip() or None
        added_path = str(meta.get("added_path") or "").strip() or None
        removed_path = str(meta.get("removed_path") or "").strip() or None

        bytes_total = 0
        if isinstance(meta.get("bytes"), int):
            bytes_total = int(meta["bytes"])
        else:
            if added_path:
                p = data_root / added_path
                if p.exists():
                    bytes_total += p.stat().st_size
            if removed_path:
                p = data_root / removed_path
                if p.exists():
                    bytes_total += p.stat().st_size

        base_version_raw = meta.get("base_version")
        base_version = (
            int(base_version_raw) if isinstance(base_version_raw, int) else None
        )

        out.append(
            ArchiveEntry(
                date=date,
                path=rel_meta,
                bytes=bytes_total,
                format="v2-delta",
                base_path=base_path,
                added_path=added_path,
                removed_path=removed_path,
                base_version=base_version,
            )
        )

    return out


def _iter_archives(data_root: Path) -> list[ArchiveEntry]:
    out = _iter_legacy_archives(data_root)
    out.extend(_iter_v2_archives(data_root))
    out.sort(key=lambda e: e.date, reverse=True)
    return out


def build_index(data_root: Path) -> dict:
    archives = _iter_archives(data_root)
    return {
        "generated_at_utc": utc_now().isoformat(),
        "archives": [
            {
                "date": e.date,
                "path": e.path,
                "bytes": e.bytes,
                "format": e.format,
                "base_path": e.base_path,
                "added_path": e.added_path,
                "removed_path": e.removed_path,
                "base_version": e.base_version,
            }
            for e in archives
        ],
    }


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(
        description="Build data/archive/index.json for the JSONL viewer"
    )
    ap.add_argument(
        "--data-root",
        default="data",
        help="Path to data root containing latest/ and archive/ (default: data)",
    )
    args = ap.parse_args()

    data_root = Path(args.data_root)
    data_root.mkdir(parents=True, exist_ok=True)

    index = build_index(data_root)
    out_path = data_root / "archive" / "index.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    print(f"Wrote {len(index['archives'])} archive entries to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
