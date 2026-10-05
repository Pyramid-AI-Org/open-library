from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from utils.time import utc_now


@dataclass(frozen=True)
class ArchiveEntry:
    date: str  # YYYY-MM-DD the snapshot was crawled; the viewer's label
    path: str  # path used by the viewer for selection/fetch
    bytes: int
    format: str  # legacy-full | v2-delta
    base_path: str | None = None
    added_path: str | None = None
    removed_path: str | None = None
    base_version: int | None = None
    archived_on: str | None = None  # YYYY-MM-DD of the run that archived it


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Enough rows to outvote records carried forward from older crawls.
_CRAWL_DATE_SAMPLE_ROWS = 2000


def _infer_crawl_date(path: Path) -> str | None:
    """The most common `discovered_at_utc` day among a snapshot's first rows.

    A snapshot is archived by the run after the one that crawled it, so its
    archive date is not its crawl date. Archives written before `crawl_date`
    was recorded in day meta only carry the crawl date in their records.
    """
    counts: Counter[str] = Counter()
    try:
        with path.open("r", encoding="utf-8") as fh:
            for i, line in enumerate(fh):
                if i >= _CRAWL_DATE_SAMPLE_ROWS:
                    break
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                day = str(rec.get("discovered_at_utc") or "")[:10]
                if _DATE_RE.match(day):
                    counts[day] += 1
    except OSError:
        return None
    return counts.most_common(1)[0][0] if counts else None


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

        archived_on = f"{yyyy}-{mm}-{dd}"
        size = urls_path.stat().st_size
        out.append(
            ArchiveEntry(
                date=_infer_crawl_date(urls_path) or archived_on,
                path=rel,
                bytes=size,
                format="legacy-full",
                archived_on=archived_on,
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

        # The first day of a month moves the whole snapshot into the month base
        # and leaves an empty delta, until the mid-month refresh rewrites it.
        # That day's snapshot is the base, so report the base's size.
        base_file = data_root / base_path if base_path else None
        delta_is_empty = bytes_total == 0
        if delta_is_empty and base_file is not None and base_file.exists():
            bytes_total = base_file.stat().st_size

        crawl_date = str(meta.get("crawl_date") or "").strip()
        if not _DATE_RE.match(crawl_date):
            sample = base_file if delta_is_empty else (
                data_root / added_path if added_path else None
            )
            crawl_date = (_infer_crawl_date(sample) if sample else None) or date

        base_version_raw = meta.get("base_version")
        base_version = (
            int(base_version_raw) if isinstance(base_version_raw, int) else None
        )

        out.append(
            ArchiveEntry(
                date=crawl_date,
                archived_on=date,
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
    out.sort(key=lambda e: (e.date, e.archived_on or ""), reverse=True)
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
                "archived_on": e.archived_on,
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
