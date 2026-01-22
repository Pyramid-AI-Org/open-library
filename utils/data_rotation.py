from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from utils.time import utc_now, utc_date_yyyymmdd


@dataclass(frozen=True)
class RotationResult:
    archived: bool
    archived_path: Path | None


def archive_previous_latest(data_root: Path, run_date: str | None = None) -> RotationResult:
    """Move the existing latest file into a dated archive folder.

    Intended layout (on the data branch):
      data/latest/urls.jsonl
      data/archive/YYYY/MM/DD/urls.jsonl

    If there is no latest file, this is a no-op.
    """

    latest_path = data_root / "latest" / "urls.jsonl"
    if not latest_path.exists():
        return RotationResult(archived=False, archived_path=None)

    if run_date is None:
        run_date = utc_date_yyyymmdd(utc_now())

    yyyy, mm, dd = run_date.split("-")
    archive_dir = data_root / "archive" / yyyy / mm / dd
    archive_dir.mkdir(parents=True, exist_ok=True)

    archived_path = archive_dir / "urls.jsonl"

    # If somehow already exists (rerun same day), keep latest as canonical and do not overwrite.
    if archived_path.exists():
        return RotationResult(archived=False, archived_path=archived_path)

    shutil.move(str(latest_path), str(archived_path))
    return RotationResult(archived=True, archived_path=archived_path)
