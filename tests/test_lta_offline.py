"""Parser and wiring tests for the LTA source and the spider engine.

The spider is the risky engine in the repository: nothing about it is bounded
by a sitemap, so the tests are mostly about the bounds. They stub the HTTP
layer and run the real crawlers through the real config/settings.sg.yaml, so a
settings key that never reaches the code fails here rather than in production.

    python tests/test_lta_offline.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

import crawlers.common.spider as spider
from crawlers.base import RunContext

BASE = "https://www.lta.gov.sg"
SEC = "/content/ltagov/en/industry_innovations/industry_matters/development_construction_specifications_resources"
DAM = "/content/dam/ltagov/industry_innovations/industry_matters/development_construction_resources"


def page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html><html><head><title>{title} | LTA</title></head><body>
<header><a href="/content/ltagov/en/newsroom/press-release.html">Newsroom</a>
<a href="/content/ltagov/en/getting_around/public_transport.html">Getting around</a>
<a href="https://www.facebook.com/lta">Facebook</a></header>
<main><h1>{title}</h1>{body}</main>
<footer><a href="/content/ltagov/en/who_we_are.html">Who we are</a></footer></body></html>"""


PAGES = {
    f"{BASE}{SEC}/street_works/requirements_for_street_work_proposals.html": page(
        "Requirements for Street Work Proposals",
        f'''<h2>Codes of Practice</h2>
        <a href="{DAM}/Street_Work_Proposals/codes_of_practice/Code_of_Practice_for_Works_on_Public_Streets_Apr_2026.pdf">
           Code of Practice for Works on Public Streets</a>
        <a href="{DAM}/Street_Work_Proposals/codes_of_practice/RT-COP_V2.0_April_2019.pdf">Download</a>
        <h2>Forms</h2>
        <a href="{DAM}/Street_Work_Proposals/forms_and_checklists/Road_Data_Form.doc">Road Data Form</a>
        <a href="{SEC}/street_works/requirements_for_street_work_proposals/gis_data_hub_collection.html">
           GIS Data Hub Data Collection Specifications</a>
        <a href="{SEC}/vehicle_parking/requirements_for_vehicle_parking_proposals.html">Vehicle parking</a>
        <a href="https://app.sla.gov.sg/inlis/#/home">INLIS Portal</a>'''
    ),
    f"{BASE}{SEC}/street_works/requirements_for_street_work_proposals/gis_data_hub_collection.html": page(
        "GIS Data Hub Data Collection Specifications",
        f'<a href="{DAM}/Street_Work_Proposals/Guidelines/gis_spec.pdf">GIS Specification v3</a>',
    ),
    f"{BASE}{SEC}/active-mobility/Active_Mobility_Proposals_and_Requirements.html": page(
        "Active Mobility Proposals and Requirements",
        f'''<a href="{DAM}/pdf/ActiveMobilityProposalsandRequirements/LTA Active Mobility Design Guide (Version 1.3).pdf">
              Active Mobility Design Guide</a>
        <a href="https://info.corenet.gov.sg/docs/default-source/lta-circulars/active-mobility-plan.pdf?sfvrsn=c15c3c12_0">
              Active Mobility Plan for Public Infrastructure Circular</a>''',
    ),
    f"{BASE}{SEC}/vehicle_parking/requirements_for_vehicle_parking_proposals.html": page(
        "Requirements for Vehicle Parking Proposals",
        f'<a href="{DAM}/vehicle_parking/pdf/cop_on_vehicle_parking.pdf">Code of Practice on Vehicle Parking</a>',
    ),
}

FETCHED: list[str] = []


def _fake_get(session, url, **kwargs):
    FETCHED.append(url)
    if url not in PAGES:
        raise RuntimeError(f"unexpected fetch: {url}")
    return SimpleNamespace(text=PAGES[url], status_code=200, headers={"Content-Type": "text/html"})


spider.get_with_retries = _fake_get
spider.sleep_seconds = lambda _s: None


def _ctx() -> RunContext:
    return RunContext(
        run_date_utc="2026-08-21",
        started_at_utc="2026-08-21T00:00:00+00:00",
        settings=yaml.safe_load(Path("config/settings.sg.yaml").read_text()),
        source_id="lta",
        source_label="Land Transport Authority",
        debug=False,
    )


def _run(module_name: str):
    import importlib

    return importlib.import_module(f"crawlers.lta.{module_name}").Crawler().crawl(_ctx())


def test_the_spider_stays_inside_its_section() -> None:
    records = _run("street_works")
    urls = [r.url for r in records]

    assert not any("newsroom" in u for u in urls), "header link followed out of section"
    assert not any("getting_around" in u for u in urls)
    assert not any("facebook" in u for u in urls)
    assert not any("who_we_are" in u for u in urls), "footer link followed out of section"
    # A sibling section is in the same site but belongs to a different row.
    assert not any("vehicle_parking" in u for u in urls), "crossed into a sibling section"
    for url in FETCHED:
        assert "/street_works/" in url, f"fetched outside the allowlist: {url}"


def test_it_follows_pages_and_collects_their_documents() -> None:
    records = _run("street_works")
    urls = {r.url for r in records}
    assert any(u.endswith("gis_data_hub_collection.html") for u in urls), "sub-page not followed"
    assert any(u.endswith("gis_spec.pdf") for u in urls), "document behind a sub-page missed"


def test_documents_are_identified_by_the_asset_path_not_the_extension() -> None:
    records = _run("street_works")
    docs = [r for r in records if r.meta["record_kind"] == "document"]
    assert docs, "no documents recorded"
    for record in docs:
        assert "/content/dam/ltagov/" in record.url, record.url
    # .doc counts as a document: LTA forms are part of the requirement.
    assert any(r.url.endswith("Road_Data_Form.doc") for r in docs)


def test_a_weak_link_label_falls_back_rather_than_naming_a_document_download() -> None:
    records = _run("street_works")
    cop = next(r for r in records if r.url.endswith("RT-COP_V2.0_April_2019.pdf"))
    assert cop.name != "Download"
    assert "RT COP" in cop.name or "V2.0" in cop.name, cop.name


def test_corenet_circulars_are_kept_and_marked_external() -> None:
    records = _run("active_mobility")
    circular = next(r for r in records if "info.corenet.gov.sg" in r.url)
    assert circular.meta["record_kind"] == "external_reference"
    # The cache-busting parameter must not be part of the record identity, or
    # the circular looks new on every run and the archive diff fills with noise.
    assert "sfvrsn" not in circular.url
    assert "sfvrsn" in circular.meta["fetch_url"], "the fetchable url should be kept"


def test_records_carry_source_identity() -> None:
    for record in _run("vehicle_parking"):
        assert record.source_id == "lta"
        assert record.source == "lta.vehicle_parking"
        assert record.source_label == "Land Transport Authority"


def test_the_page_budget_is_announced_when_it_bites(capsys=None) -> None:
    # A crawl that stops at its cap silently is indistinguishable from one that
    # covered the section, and the coverage report would score it as a pass.
    import io
    import contextlib

    settings = yaml.safe_load(Path("config/settings.sg.yaml").read_text())
    settings["crawlers"]["lta"]["pages"]["street_works"]["max_pages"] = 1
    ctx = RunContext(
        run_date_utc="2026-08-21",
        started_at_utc="2026-08-21T00:00:00+00:00",
        settings=settings,
        source_id="lta",
        source_label="Land Transport Authority",
    )
    import importlib

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        importlib.import_module("crawlers.lta.street_works").Crawler().crawl(ctx)
    assert "INCOMPLETE" in buf.getvalue(), "a truncated crawl said nothing"


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        FETCHED.clear()
        try:
            fn()
            print(f"PASS {name}")
        except AssertionError as exc:
            failures += 1
            print(f"FAIL {name}: {exc}")
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(f"ERROR {name}: {type(exc).__name__}: {exc}")
    print(f"\n{failures} failure(s)")
    raise SystemExit(1 if failures else 0)
