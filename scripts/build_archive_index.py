from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from utils.time import utc_now


@dataclass(frozen=True)
class ArchiveEntry:
    date: str  # YYYY-MM-DD
    path: str  # archive/YYYY/MM/DD/urls.jsonl (relative to data root)
    bytes: int


def _iter_archives(data_root: Path) -> list[ArchiveEntry]:
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
        out.append(ArchiveEntry(date=date, path=rel, bytes=size))

    out.sort(key=lambda e: e.date, reverse=True)
    return out


def build_index(data_root: Path) -> dict:
    archives = _iter_archives(data_root)
    return {
        "generated_at_utc": utc_now().isoformat(),
        "archives": [
            {"date": e.date, "path": e.path, "bytes": e.bytes} for e in archives
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
    out_path.write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote {len(index['archives'])} archive entries to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
