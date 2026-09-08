"""DSD Sewage Services Charging Scheme (PAI-1037).

Runs the real crawler through the real config/settings.yaml with the HTTP layer
stubbed, so a settings key that never reaches the code fails here rather than
in production.

Two things are worth pinning. First the bounds: the scheme's pages must be
followed across DSD's two spellings of the section path, while the Sewage
Services Operating Accounts tree — 46 annual financial reports linked from a
neighbouring page — must stay out. Second the titles: every page on this site
puts "Drainage Services Department" in its <h1> and the page's real name only
in <title> and og:title, so without `page_title_source: og_title` six of the
ten pages are indexed under the department name and none of them can be found
by what they are about.

    python -m pytest tests/test_dsd_sewage_charging_offline.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
import yaml

import crawlers.common.spider as spider
from crawlers.base import RunContext

BASE = "https://www.dsd.gov.hk"
LANDING = f"{BASE}/EN/SewageServicesChargingScheme/index.html"
SEC = "/EN/Sewage_Services_Charging_Scheme"
UPLOADS = "/uploads/page/SewageServicesChargingScheme"


def page(og_title: str, body: str) -> str:
    """DSD's shape: agency name in <h1>, real name in <title> and og:title."""
    return f"""<!DOCTYPE html><html><head>
<title>Drainage Services Department - {og_title}</title>
<meta property="og:title" content="{og_title} " />
</head><body>
<header><h1>Drainage Services Department</h1>
<a href="/EN/Home/index.html">Home</a>
<a href="/TC/Sewage_Services_Charging_Scheme/Billing/index.html">Chinese</a></header>
<main>{body}</main></body></html>"""


PAGES = {
    LANDING: page(
        "Sewage Services Charging Scheme",
        f'<a href="{SEC}/Sewage_Services_Charges/index.html">Sewage Services Charges</a>'
        f'<a href="{SEC}/Ordinance___Regulations/index.html">Ordinance &amp; Regulations</a>'
        f'<a href="/EN/Publicity_and_Publications/Publicity/Sewage_Services_Operating_Accounts/index.html">'
        f"Operating Accounts</a>",
    ),
    f"{BASE}{SEC}/Sewage_Services_Charges/index.html": page(
        "Sewage Services Charges",
        f'<a href="{UPLOADS}/Appendix/Appendix_V_e.pdf">Appendix V</a>'
        f'<a href="{UPLOADS}/Forms/DSD_TES1_e.pdf">DSD/TES 1(e)</a>'
        # A financial report from the neighbouring tree — must not be recorded.
        f'<a href="/EN/Files/publications_publicity/ssoa_reports/SSOA09-10e.pdf">Accounts 2009-10</a>',
    ),
    f"{BASE}{SEC}/Ordinance___Regulations/index.html": page(
        "Ordinance &amp; Regulations",
        f'<a href="{UPLOADS}/Forms/TM_2007_Eng.pdf">Technical Memorandum</a>',
    ),
}

@pytest.fixture(autouse=True)
def fetched(monkeypatch):
    """Stub the HTTP layer for this module only, and record what was fetched.

    Deliberately a fixture rather than a module-level assignment: patching
    `spider.get_with_retries` at import time leaks into every other test module
    pytest loads in the same session, which is what makes the LTA and BCA suites
    fail when the whole directory runs. monkeypatch undoes itself per test.
    """
    urls: list[str] = []

    def fake_get(session, url, **kwargs):
        urls.append(url)
        if url not in PAGES:
            raise RuntimeError(f"unexpected fetch: {url}")
        return SimpleNamespace(
            text=PAGES[url], status_code=200, headers={"Content-Type": "text/html"}
        )

    monkeypatch.setattr(spider, "get_with_retries", fake_get)
    monkeypatch.setattr(spider, "sleep_seconds", lambda _s: None)
    return urls


def _run():
    import importlib

    module = importlib.import_module("crawlers.dsd.sewage_services_charging_scheme")
    ctx = RunContext(
        run_date_utc="2026-09-08",
        started_at_utc="2026-09-08T00:00:00+00:00",
        settings=yaml.safe_load(Path("config/settings.yaml").read_text(encoding="utf-8")),
        source_id="dsd",
        source_label="Drainage Services Department",
        debug=False,
    )
    return module.Crawler().crawl(ctx)


def _kinds(records):
    return {
        kind: [r for r in records if r.meta.get("record_kind") == kind]
        for kind in ("page", "document")
    }


def test_it_follows_both_spellings_of_the_section_path():
    # The landing page is /EN/SewageServicesChargingScheme/ but every child
    # lives under /EN/Sewage_Services_Charging_Scheme/. Allowing only one
    # prefix silently reduces the scheme to a single navigation page.
    records = _run()
    pages = _kinds(records)["page"]
    urls = {r.url for r in pages}
    assert LANDING in urls
    assert f"{BASE}{SEC}/Sewage_Services_Charges/index.html" in urls
    assert f"{BASE}{SEC}/Ordinance___Regulations/index.html" in urls


def test_page_titles_come_from_og_title_not_the_agency_banner():
    # Every <h1> on this site reads "Drainage Services Department". Falling back
    # to it would index six of the ten pages under the department name.
    records = _run()
    titles = {r.name for r in _kinds(records)["page"]}
    assert "Drainage Services Department" not in titles
    assert "Sewage Services Charges" in titles
    assert "Sewage Services Charging Scheme" in titles


def test_entity_encoded_titles_are_decoded():
    records = _run()
    titles = {r.name for r in _kinds(records)["page"]}
    assert "Ordinance & Regulations" in titles
    assert "Ordinance &amp; Regulations" not in titles


def test_the_scheme_documents_are_collected():
    records = _run()
    docs = {r.url.split("/")[-1] for r in _kinds(records)["document"]}
    assert "Appendix_V_e.pdf" in docs
    assert "DSD_TES1_e.pdf" in docs
    assert "TM_2007_Eng.pdf" in docs


def test_the_operating_accounts_reports_are_left_out():
    # 46 annual financial reports hang off a neighbouring page. They are company
    # accounts, not charging rules, and document_path_prefixes keeps them out.
    records = _run()
    assert [r.url for r in records if "ssoa_reports" in r.url] == []


def test_the_chinese_mirror_is_not_followed(fetched):
    records = _run()
    assert [r.url for r in records if "/TC/" in r.url] == []
    # And it was never even requested — the allowlist drops it before the fetch.
    assert [u for u in fetched if "/TC/" in u] == []
    assert len(fetched) == len(PAGES)


def test_every_record_is_attributed_to_this_section():
    records = _run()
    assert {r.source for r in records} == {"dsd.sewage_services_charging_scheme"}
