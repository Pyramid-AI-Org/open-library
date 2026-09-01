"""Tests for the silent-failure guardrail in main.py.

A crawler that fetches nothing and exits 0 is indistinguishable from one whose
source genuinely has no documents. That ambiguity hid three separate outages:
LegCo's seven pages (TLS chain), the FSD circular letters and the EMSD gas
portal all reported successful runs for months while collecting nothing.
_detect_regressions closes that gap by comparing each run against the previous
snapshot.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import _detect_regressions


def _prior(*counts_by_source):
    """Build the previous-run shape: source -> url -> record."""
    return {
        source: {f"https://example.test/{i}": {"url": f"https://example.test/{i}"} for i in range(n)}
        for source, n in counts_by_source
    }


def _records(n):
    return [{"url": f"https://example.test/{i}"} for i in range(n)]


def test_reports_crawler_that_collected_nothing():
    zeroed, dropped = _detect_regressions(
        _prior(("legco.finance_meetings", 1178)),
        {"legco.finance_meetings": []},
    )

    assert zeroed == [{"crawler": "legco.finance_meetings", "previous": 1178}]
    assert dropped == []


def test_ignores_a_new_crawler_with_no_history():
    """First run of a crawler yields nothing to compare against, so it is silent."""
    zeroed, dropped = _detect_regressions({}, {"fsd.licensing_food_premises": []})

    assert zeroed == []
    assert dropped == []


def test_warns_when_the_count_halves_but_does_not_zero():
    zeroed, dropped = _detect_regressions(
        _prior(("devb.devb_works_technical_circulars_um", 342)),
        {"devb.devb_works_technical_circulars_um": _records(71)},
    )

    assert zeroed == []
    assert dropped == [
        {
            "crawler": "devb.devb_works_technical_circulars_um",
            "previous": 342,
            "current": 71,
        }
    ]


def test_small_fluctuations_are_not_reported():
    zeroed, dropped = _detect_regressions(
        _prior(("bd.practice_notes_and_circular_letters", 467)),
        {"bd.practice_notes_and_circular_letters": _records(465)},
    )

    assert zeroed == []
    assert dropped == []


def test_skipped_crawlers_are_not_reported():
    """A crawler that did not run this cycle carries its records forward."""
    zeroed, dropped = _detect_regressions(
        _prior(("cedd.cedd_geo_publications", 1235)),
        {},
    )

    assert zeroed == []
    assert dropped == []


def test_prior_snapshot_is_matched_by_the_bare_crawler_name():
    """Results are keyed by module_path; the snapshot is keyed by `source`.

    Records carry the bare crawler name in their `source` field, so a lookup by
    module_path alone finds nothing and every regression goes unreported. This
    is the same keying mismatch that the scheduler state lookup already guards
    against.
    """
    zeroed, _ = _detect_regressions(
        _prior(("practice_notes", 11)),
        {"pland.practice_notes": []},
    )

    assert zeroed == [{"crawler": "pland.practice_notes", "previous": 11}]


def test_results_are_sorted_and_independent():
    zeroed, dropped = _detect_regressions(
        _prior(
            ("wsd.circular_letters", 35),
            ("fsd.circular_letters", 108),
            ("emsd.gas_safety_portal", 16),
            ("lands.lao_practice_notes", 101),
        ),
        {
            "wsd.circular_letters": _records(35),
            "fsd.circular_letters": [],
            "emsd.gas_safety_portal": [],
            "lands.lao_practice_notes": _records(40),
        },
    )

    assert [entry["crawler"] for entry in zeroed] == [
        "emsd.gas_safety_portal",
        "fsd.circular_letters",
    ]
    assert [entry["crawler"] for entry in dropped] == ["lands.lao_practice_notes"]
