"""A sweep record whose URL another crawler lists is dropped (PAI-1384).

The department sweeps re-read the index pages their dedicated crawlers own, so
2,492 URLs were listed twice — DMUA 2026 under `codes_design_manuals_and_guidelines`
and `bd_document_sweep` — and ingestion stored those files twice.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import _drop_sweep_duplicates

DMUA = "https://www.bd.gov.hk/doc/en/resources/codes-and-references/code-and-design-manuals/DMUA2026e.pdf"


def _rec(source: str, url: str) -> dict:
    return {"source": source, "url": url}


def test_the_dedicated_crawler_keeps_a_url_the_sweep_also_found():
    records = [
        _rec("bd_document_sweep", DMUA),
        _rec("codes_design_manuals_and_guidelines", DMUA),
        _rec("bd_document_sweep", "https://example.test/only-the-sweep.pdf"),
    ]

    kept, dropped = _drop_sweep_duplicates(records)

    assert dropped == 1
    assert kept == [
        _rec("codes_design_manuals_and_guidelines", DMUA),
        _rec("bd_document_sweep", "https://example.test/only-the-sweep.pdf"),
    ]


def test_two_dedicated_crawlers_both_keep_their_record():
    records = [
        _rec("joint_practice_notes", "https://example.test/x.pdf"),
        _rec("lao_practice_notes", "https://example.test/x.pdf"),
    ]

    kept, dropped = _drop_sweep_duplicates(records)

    assert dropped == 0
    assert kept == records


def test_a_url_found_only_by_two_sweeps_is_left_alone():
    records = [
        _rec("bd_document_sweep", "https://example.test/y.pdf"),
        _rec("lands_document_sweep", "https://example.test/y.pdf"),
    ]

    kept, dropped = _drop_sweep_duplicates(records)

    assert dropped == 0
    assert kept == records
