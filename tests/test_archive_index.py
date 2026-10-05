"""The viewer's archive dropdown must label each snapshot by its crawl date and size.

PAI-1384: on 2026-10-04 the dropdown listed "2026-10-01 (0 B)". The Oct 1 crawl
had run and published 85,893 rows; nothing was lost. Two things misled readers:

- The first archive of a month moves the whole snapshot into the month base and
  writes an empty delta for that day. The index reported the delta's 0 bytes,
  and kept doing so until the mid-month refresh rewrote the day (every month
  since July showed 0 B on the 1st until the 15th).
- A snapshot is archived by the run after the one that crawled it, and was
  labelled with that later run's date. "2026-10-01" held the Sep 30 crawl and
  "2026-10-02" held the Oct 1 crawl, which is where the new BD design manual
  first appeared.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.build_archive_index import build_index
from utils.data_rotation import archive_previous_latest


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _rec(n: int, crawled: str) -> dict:
    return {
        "source": "bd_document_sweep",
        "url": f"https://example.test/{n}.pdf",
        "discovered_at_utc": f"{crawled}T02:30:52+00:00",
    }


def _publish_latest(data_root: Path, crawled: str, rows: int) -> None:
    """What a crawl run leaves behind in latest/ for the next run to archive."""
    _write_jsonl(
        data_root / "latest" / "urls.jsonl", [_rec(n, crawled) for n in range(rows)]
    )
    (data_root / "latest" / "summary.json").write_text(
        json.dumps({"run_date_utc": crawled}), encoding="utf-8"
    )


def _entry(data_root: Path, archived_on: str) -> dict:
    entries = [
        e for e in build_index(data_root)["archives"] if e["archived_on"] == archived_on
    ]
    assert len(entries) == 1, entries
    return entries[0]


def test_first_day_of_month_reports_the_base_size_not_zero(tmp_path: Path):
    _publish_latest(tmp_path, "2026-09-30", rows=5)
    archive_previous_latest(tmp_path, run_date="2026-10-01")

    base = tmp_path / "archive_v2" / "2026" / "10" / "base.jsonl"
    entry = _entry(tmp_path, "2026-10-01")

    assert entry["bytes"] == base.stat().st_size > 0
    assert entry["date"] == "2026-09-30"


def test_each_archive_is_labelled_by_the_crawl_it_holds(tmp_path: Path):
    _publish_latest(tmp_path, "2026-09-30", rows=5)
    archive_previous_latest(tmp_path, run_date="2026-10-01")
    _publish_latest(tmp_path, "2026-10-01", rows=6)
    archive_previous_latest(tmp_path, run_date="2026-10-02")

    day = tmp_path / "archive_v2" / "2026" / "10" / "days" / "02"
    meta = json.loads((day / "meta.json").read_text(encoding="utf-8"))
    entry = _entry(tmp_path, "2026-10-02")

    assert meta["crawl_date"] == "2026-10-01"
    assert meta["date"] == "2026-10-02"
    assert entry["date"] == "2026-10-01"
    assert entry["bytes"] == meta["bytes"] > 0
    assert entry["path"] == "archive_v2/2026/10/days/02/meta.json"


def test_mid_month_refresh_keeps_crawl_dates_and_fills_day_one(tmp_path: Path):
    _publish_latest(tmp_path, "2026-09-30", rows=5)
    archive_previous_latest(tmp_path, run_date="2026-10-01", mid_month_refresh_day=2)
    _publish_latest(tmp_path, "2026-10-01", rows=5)
    archive_previous_latest(tmp_path, run_date="2026-10-02", mid_month_refresh_day=2)

    days = tmp_path / "archive_v2" / "2026" / "10" / "days"
    day1 = json.loads((days / "01" / "meta.json").read_text(encoding="utf-8"))
    day2 = json.loads((days / "02" / "meta.json").read_text(encoding="utf-8"))

    assert day1["base_version"] == 2
    assert day1["crawl_date"] == "2026-09-30"
    assert day2["crawl_date"] == "2026-10-01"
    # After the refresh day one is a real delta, so its own size is reported.
    assert _entry(tmp_path, "2026-10-01")["bytes"] == day1["bytes"] > 0


def _write_meta_without_crawl_date(day: Path, archived_on: str) -> None:
    yyyy, mm, dd = archived_on.split("-")
    (day / "meta.json").write_text(
        json.dumps(
            {
                "date": archived_on,
                "format": "v2-delta",
                "base_version": 1,
                "base_path": f"archive_v2/{yyyy}/{mm}/base.jsonl",
                "added_path": f"archive_v2/{yyyy}/{mm}/days/{dd}/added.jsonl",
                "removed_path": f"archive_v2/{yyyy}/{mm}/days/{dd}/removed.jsonl",
                "bytes": (day / "added.jsonl").stat().st_size
                + (day / "removed.jsonl").stat().st_size,
            }
        ),
        encoding="utf-8",
    )


def test_archives_from_before_crawl_date_are_labelled_from_their_records(
    tmp_path: Path,
):
    month = tmp_path / "archive_v2" / "2026" / "10"
    # Two rows carried forward from an older crawl must not outvote the rest.
    _write_jsonl(
        month / "base.jsonl",
        [_rec(n, "2026-09-30") for n in range(5)] + [_rec(90, "2026-03-30")] * 2,
    )
    _write_jsonl(month / "days" / "01" / "added.jsonl", [])
    _write_jsonl(month / "days" / "01" / "removed.jsonl", [])
    _write_meta_without_crawl_date(month / "days" / "01", "2026-10-01")
    _write_jsonl(
        month / "days" / "02" / "added.jsonl", [_rec(n, "2026-10-01") for n in range(5)]
    )
    _write_jsonl(month / "days" / "02" / "removed.jsonl", [])
    _write_meta_without_crawl_date(month / "days" / "02", "2026-10-02")

    assert _entry(tmp_path, "2026-10-01")["date"] == "2026-09-30"
    assert _entry(tmp_path, "2026-10-02")["date"] == "2026-10-01"


def test_legacy_full_archives_are_labelled_by_crawl_date(tmp_path: Path):
    _write_jsonl(
        tmp_path / "archive" / "2026" / "04" / "07" / "urls.jsonl",
        [_rec(n, "2026-04-06") for n in range(3)],
    )

    entry = _entry(tmp_path, "2026-04-07")

    assert entry["date"] == "2026-04-06"
    assert entry["format"] == "legacy-full"


def test_records_without_dates_fall_back_to_the_archive_date(tmp_path: Path):
    _write_jsonl(
        tmp_path / "archive" / "2026" / "04" / "07" / "urls.jsonl",
        [{"source": "s", "url": "https://example.test/x.pdf"}],
    )

    assert _entry(tmp_path, "2026-04-07")["date"] == "2026-04-07"


def test_entries_sort_newest_crawl_first(tmp_path: Path):
    _publish_latest(tmp_path, "2026-09-30", rows=2)
    archive_previous_latest(tmp_path, run_date="2026-10-01")
    _publish_latest(tmp_path, "2026-10-01", rows=2)
    archive_previous_latest(tmp_path, run_date="2026-10-02")

    dates = [e["date"] for e in build_index(tmp_path)["archives"]]

    assert dates == ["2026-10-01", "2026-09-30"]
