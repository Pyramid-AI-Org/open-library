"""The previous-run snapshot must come from latest/urls.jsonl, not the archive.

On 2026-09-02 a full run published 85,154 rows at 11:16 UTC. Two single-crawler
dispatches later that day rebuilt latest/urls.jsonl from the archive_v2
reconstruction (month base + that day's delta). archive_previous_latest returns
early when the day already has a delta, so the delta reflected only the first
run of the day; the reconstruction was missing 7,578 rows that the 11:16 run had
added, and the snapshot fell to 78,535. Every one of the 25 new sections
vanished. The guardrail did not fire because it only compares crawlers that ran.

README calls latest/urls.jsonl "the canonical full snapshot for the latest run".
It has to be what a run starts from.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import _detect_total_row_regression, _load_previous_records_by_source


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _rec(source: str, n: int) -> dict:
    return {"source": source, "url": f"https://example.test/{source}/{n}.pdf"}


def test_latest_snapshot_wins_over_a_stale_archive_reconstruction(tmp_path: Path):
    out_root = tmp_path

    # The first run of the day archived this: base + an empty day delta.
    base = [_rec("old_section", i) for i in range(3)]
    _write_jsonl(out_root / "archive_v2" / "2026" / "09" / "base.jsonl", base)
    day = out_root / "archive_v2" / "2026" / "09" / "days" / "02"
    _write_jsonl(day / "added.jsonl", [])
    _write_jsonl(day / "removed.jsonl", [])
    (day / "meta.json").write_text(
        json.dumps(
            {
                "base_path": "archive_v2/2026/09/base.jsonl",
                "added_path": "archive_v2/2026/09/days/02/added.jsonl",
                "removed_path": "archive_v2/2026/09/days/02/removed.jsonl",
            }
        ),
        encoding="utf-8",
    )

    # A later run the same day published a fuller latest that the archive
    # never saw, because same-day archiving is skipped.
    latest = base + [_rec("new_section", i) for i in range(5)]
    _write_jsonl(out_root / "latest" / "urls.jsonl", latest)

    previous = _load_previous_records_by_source(out_root)

    assert "new_section" in previous, (
        "the newer latest/urls.jsonl was ignored in favour of the stale archive"
    )
    assert len(previous["new_section"]) == 5
    assert len(previous["old_section"]) == 3


def test_archive_reconstruction_is_used_only_when_there_is_no_latest(tmp_path: Path):
    out_root = tmp_path
    base = [_rec("archived_only", i) for i in range(4)]
    _write_jsonl(out_root / "archive_v2" / "2026" / "09" / "base.jsonl", base)
    day = out_root / "archive_v2" / "2026" / "09" / "days" / "02"
    _write_jsonl(day / "added.jsonl", [_rec("archived_only", 99)])
    _write_jsonl(day / "removed.jsonl", [])
    (day / "meta.json").write_text(
        json.dumps(
            {
                "base_path": "archive_v2/2026/09/base.jsonl",
                "added_path": "archive_v2/2026/09/days/02/added.jsonl",
                "removed_path": "archive_v2/2026/09/days/02/removed.jsonl",
            }
        ),
        encoding="utf-8",
    )

    previous = _load_previous_records_by_source(out_root)

    assert len(previous["archived_only"]) == 5


def test_total_row_drop_is_a_regression():
    # 85,154 -> 78,535 is what actually happened; well past any tolerance.
    result = _detect_total_row_regression(previous_rows=85154, current_rows=78535)
    assert result is not None
    assert result["previous"] == 85154
    assert result["current"] == 78535


def test_small_shrink_and_growth_are_not_regressions():
    assert _detect_total_row_regression(previous_rows=1000, current_rows=990) is None
    assert _detect_total_row_regression(previous_rows=1000, current_rows=1200) is None


def test_first_ever_run_has_nothing_to_compare():
    assert _detect_total_row_regression(previous_rows=0, current_rows=500) is None
