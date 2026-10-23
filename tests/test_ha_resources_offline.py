"""PAI-1453: real HA settings, captured publisher pages and runner contracts."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from crawlers.base import RunContext
from crawlers.common import spider
from crawlers.common.popup_links import unwrap_popup_link
from main import _get_all_crawlers_from_settings, _merge_records_for_latest, _run_one

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/ha"
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text())
EXPECTED = json.loads((FIXTURES / "expected.json").read_text())
SETTINGS = yaml.safe_load((ROOT / "config/settings.yaml").read_text())
BASE = "https://www.housingauthority.gov.hk"
HUB = BASE + "/en/business-partnerships/resources/index.html"
BIM = HUB.replace("index.html", "building-information-modelling/index.html")
SPEC = HUB.replace("index.html", "general-specification-for-building-works/index.html")


@pytest.fixture
def pages(monkeypatch):
    pages = {row["url"]: (FIXTURES / row["file"]).read_text() for row in MANIFEST}

    def get(_session, url, **_kwargs):
        if url not in pages:
            raise RuntimeError(f"Unexpected fetch: {url}")
        return SimpleNamespace(text=pages[url], headers={"Content-Type": "text/html"})

    monkeypatch.setattr(spider, "get_with_retries", get)
    monkeypatch.setattr(spider, "sleep_seconds", lambda _: None)
    return pages


def run(settings=None, section=None):
    settings = settings or deepcopy(SETTINGS)
    records = []
    for sid, name, module in _get_all_crawlers_from_settings(settings):
        if sid == "ha" and (section is None or name == section):
            records.extend(
                _run_one(
                    sid,
                    name,
                    module,
                    settings,
                    "2026-10-23",
                    "2026-10-23T00:00:00Z",
                    False,
                )
            )
    return records


def test_complete_publisher_inventory_and_stable_existing_identity(pages):
    records = run()
    assert len(records) == 27
    assert {r["url"] for r in records} == set(EXPECTED["included"])
    assert not set(EXPECTED["excluded"]) & {r["url"] for r in records}
    assert len({r["url"] for r in records}) == 27
    for r in records:
        assert r["source_id"] == "ha"
        assert r["meta"]["record_kind"] == "document"
        assert r["meta"]["file_ext"] in {"pdf", "doc", "docx"}
        assert r["name"]
        if r["url"] in EXPECTED["existing_names"]:
            assert r["name"] == EXPECTED["existing_names"][r["url"]]
        else:
            assert r["name"].startswith("Housing Authority — ")
        if "/building-information-modelling/" in r["url"]:
            assert r["source"] == "ha.building_information_modelling"
        elif "/general-specification-for-building-works/" in r["url"]:
            assert r["source"] == "ha.general_specification_for_building_works"
        else:
            assert r["source"] == "ha.resources"
    assert records == run()


def test_captured_fixture_integrity():
    assert len(MANIFEST) == 24
    for row in MANIFEST:
        assert (
            hashlib.sha256((FIXTURES / row["file"]).read_bytes()).hexdigest()
            == row["sha256"]
        )


def test_future_section_link_and_removed_document_change_the_snapshot(pages):
    new_page = HUB.replace("index.html", "new-technical-section/index.html")
    new_doc = (
        BASE
        + "/common/pdf/business-partnerships/resources/new-technical-section/new.pdf"
    )
    pages[HUB] += f'<a href="{new_page}">New technical section</a>'
    pages[new_page] = (
        f'<title>New technical section</title><a href="{new_doc}">New guide</a>'
    )
    old = run()
    assert new_doc in {r["url"] for r in old}
    pages[new_page] = "<title>New technical section</title>"
    new = run()
    merged = _merge_records_for_latest(
        {
            source: {r["url"]: r for r in old if r["source"] == source}
            for source in {r["source"] for r in old}
        },
        {
            source: [r for r in new if r["source"] == source]
            for source in {r["source"] for r in new}
        },
    )
    assert new_doc not in {r["url"] for r in merged}


def test_replacement_editions_do_not_depend_on_four_permanent_urls(pages):
    pages[BIM] = pages[BIM].replace("HABIMSG v5.0-", "HABIMSG v6.0-")
    pages[SPEC] = pages[SPEC].replace(
        "Works 2024 Edition.pdf", "Works 2028 Edition.pdf"
    )
    urls = {r["url"] for r in run()}
    assert not set(EXPECTED["existing_names"]) & urls
    assert sum("v6.0-" in u for u in urls) == 3
    assert any("Works%202028%20Edition.pdf" in u for u in urls)


@pytest.mark.parametrize(
    "suffix",
    [
        '<a href="https://evil.test/x.pdf">Wrong host</a>',
        '<a href="http://www.housingauthority.gov.hk/common/pdf/business-partnerships/resources/x.pdf">HTTP</a>',
        '<a href="ftp://www.housingauthority.gov.hk/common/pdf/business-partnerships/resources/x.pdf">FTP</a>',
        '<a href="ftp://www.housingauthority.gov.hk/en/business-partnerships/resources/evil.html">FTP page</a>',
        '<a href="/common/pdf/unrelated/secret.pdf">Wrong asset path</a>',
        '<a href="/common/pdf/business-partnerships/resources/x.zip">Archive</a>',
        '<a href="/en/public-housing/other.html">Outside page scope</a>',
        "<a href=\"javascript:popup800600Scroll('https://evil.test/x.pdf')\">External popup</a>",
        "<a href=\"javascript:popup800600Scroll('file:///x.pdf')\">Local file</a>",
    ],
)
def test_host_path_extension_and_script_bounds(pages, suffix):
    pages[HUB] += suffix
    assert {r["url"] for r in run()} == set(EXPECTED["included"])


@pytest.mark.parametrize(
    "kind", ["fetch", "page_budget", "link_budget", "record_budget", "floor", "popup"]
)
def test_incomplete_crawl_raises_instead_of_publishing_partial_feed(pages, kind):
    settings = deepcopy(SETTINGS)
    cfg = settings["crawlers"]["ha"]["pages"]["resources"]
    if kind == "fetch":
        del pages[
            HUB.replace("index.html", "performance-assessment/building-pass/index.html")
        ]
    elif kind == "page_budget":
        cfg["max_pages"] = 1
    elif kind == "link_budget":
        cfg["max_out_links_per_page"] = 1
    elif kind == "record_budget":
        cfg["max_total_records"] = 1
    elif kind == "floor":
        cfg["min_records"] = 1000
    elif kind == "popup":
        pages[
            HUB
        ] += "<a href=\"javascript:popup800600Scroll('x.pdf'+window.location)\">Bad popup</a>"
    with pytest.raises((RuntimeError, ValueError)):
        run(settings)


def test_literal_popup_parser_never_accepts_script_expressions():
    assert (
        unwrap_popup_link("javascript:popup800600Scroll('/x.pdf')", "popup800600Scroll")
        == "/x.pdf"
    )
    assert (
        unwrap_popup_link("javascript:popupSingleWindow('/x.pdf')", "popup800600Scroll")
        is None
    )
    assert unwrap_popup_link("javascript:void(0)", "popup800600Scroll") is None
    with pytest.raises(ValueError):
        unwrap_popup_link(
            "javascript:popup800600Scroll('/x.pdf');steal()", "popup800600Scroll"
        )


def test_legacy_spider_defaults_keep_page_records_and_tolerate_fetch_failures(pages):
    settings = {
        "crawlers": {
            "test": {
                "label": "Test",
                "pages": {
                    "section_spider": {
                        "start_urls": [HUB],
                        "max_depth": 0,
                    }
                },
            }
        }
    }
    ctx = RunContext("2026-10-23", "2026-10-23T00:00:00Z", settings, "test", "Test")
    assert any(
        r.meta["record_kind"] == "page"
        for r in spider.SectionSpiderCrawler().crawl(ctx)
    )
    del pages[HUB]
    assert spider.SectionSpiderCrawler().crawl(ctx) == []
