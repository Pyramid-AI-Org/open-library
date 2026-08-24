"""End-to-end wiring test for the BCA source, with HTTP stubbed out.

The parser tests prove the pieces work. This proves the assembled thing works:
that config/settings.sg.yaml resolves, that path prefixes and excludes are
applied, that host filtering separates BCA documents from Statutes Online
references, and that each crawler module loads the way main.py loads it.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

import crawlers.common.isomer as isomer
from crawlers.base import RunContext

BASE = "https://www1.bca.gov.sg"

SITEMAP = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>{BASE}/</loc><lastmod>2026-08-05T06:54:31.504Z</lastmod></url>
<url><loc>{BASE}/safety-and-standards/building-control-act/</loc><lastmod>2026-07-30T00:00:00.000Z</lastmod></url>
<url><loc>{BASE}/safety-and-standards/enforcement-actions/case-1/</loc></url>
<url><loc>{BASE}/resources/newsroom/press-release/</loc></url>
<url><loc>{BASE}/sustainability/legislation-on-environmental-sustainability-for-buildings/existing-buildings/</loc></url>
<url><loc>{BASE}/sustainability/greenmark/awards/</loc></url>
</urlset>"""

ACT_PAGE = """<html><head><title>Building Control Act | BCA</title></head><body>
<header><a href="/about-us/">About</a></header>
<main id="main-content"><h1>Building Control Act</h1>
<a aria-label="Approved Document [PDF, 5.1 MB] (opens in new tab)"
   href="https://isomer-user-content.by.gov.sg/338/abc/approved-document-v7-07.pdf">Approved Document</a>
<a aria-label="Building Control Regulations (opens in new tab)"
   href="https://sso.agc.gov.sg/SL/BCA1989-S666-2003">Regulations</a>
<a href="https://www.facebook.com/BCASingapore">Facebook</a>
<a href="/safety-and-standards/accessibility/">Accessibility</a>
</main></body></html>"""

ESB_PAGE = """<html><body><main id="main-content"><h1>Existing Buildings</h1>
<a aria-label="Code on Mandatory Energy Improvement [PDF, 3.8 MB] (opens in new tab)"
   href="https://isomer-user-content.by.gov.sg/338/mei/mei-code_1st-edition.pdf">MEI Code</a>
</main></body></html>"""

CIRCULARS = (
    '<html><body><main id="main-content"><h1>Circulars</h1>'
    '<a href="https://isomer-user-content.by.gov.sg/338/aaa/Circular A.pdf">3 August 2026</a>'
    "</main>"
    '<script>self.__next_f.push([1,"'
    '{\\"referenceLinkHref\\":\\"https://isomer-user-content.by.gov.sg/338/aaa/Circular A.pdf\\",'
    '\\"itemTitle\\":\\"CIRCULAR ON FEEDBACK CHANNEL [PDF, 1.2 MB]\\",'
    '\\"formattedDate\\":\\"3 August 2026\\"},'
    '{\\"referenceLinkHref\\":\\"https://isomer-user-content.by.gov.sg/338/bbb/Circular B.pdf\\",'
    '\\"itemTitle\\":\\"ESCALATOR MAINTENANCE [PDF, 33 KB]\\",'
    '\\"formattedDate\\":\\"18 February 2018\\"},'
    '{\\"referenceLinkHref\\":\\"/resources/circulars/web-only-circular/\\",'
    '\\"itemTitle\\":\\"WEB ONLY CIRCULAR\\",'
    '\\"formattedDate\\":\\"1 January 2020\\"}'
    '"])</script></body></html>'
)

PAGES = {
    f"{BASE}/sitemap.xml": SITEMAP,
    f"{BASE}/safety-and-standards/building-control-act/": ACT_PAGE,
    f"{BASE}/safety-and-standards/enforcement-actions/case-1/": "<html></html>",
    f"{BASE}/sustainability/legislation-on-environmental-sustainability-for-buildings/existing-buildings/": ESB_PAGE,
    f"{BASE}/resources/circulars/": CIRCULARS,
}

FETCHED: list[str] = []


def _fake_get(session, url, **kwargs):
    FETCHED.append(url)
    if url not in PAGES:
        raise RuntimeError(f"unexpected fetch: {url}")
    return SimpleNamespace(text=PAGES[url], status_code=200)


isomer.get_with_retries = _fake_get
isomer.sleep_seconds = lambda _s: None


def _ctx() -> RunContext:
    settings = yaml.safe_load(Path("config/settings.sg.yaml").read_text())
    return RunContext(
        run_date_utc="2026-08-21",
        started_at_utc="2026-08-21T00:00:00+00:00",
        settings=settings,
        source_id="bca",
        source_label="Building and Construction Authority",
        debug=False,
    )


def _run(module_name: str):
    import importlib

    module = importlib.import_module(f"crawlers.bca.{module_name}")
    return module.Crawler().crawl(_ctx())


def test_safety_and_standards_scope_and_classification() -> None:
    records = _run("safety_and_standards")
    urls = [r.url for r in records]

    assert f"{BASE}/safety-and-standards/enforcement-actions/case-1/" not in FETCHED, (
        "excluded path was fetched"
    )
    assert not any("newsroom" in u for u in urls), "out-of-scope section leaked in"
    assert not any("facebook" in u for u in urls), "footer link leaked in"
    assert not any(u.endswith("/safety-and-standards/accessibility/") for u in urls), (
        "a sibling page link was recorded as a document"
    )

    by_url = {r.url: r for r in records}
    page = by_url[f"{BASE}/safety-and-standards/building-control-act"]
    assert page.meta["record_kind"] == "page"
    assert page.name == "Building Control Act"
    assert page.publish_date == "2026-07-30", "sitemap lastmod should date the page"

    doc = by_url["https://isomer-user-content.by.gov.sg/338/abc/approved-document-v7-07.pdf"]
    assert doc.meta["record_kind"] == "document"
    assert doc.name == "Approved Document"
    assert doc.meta["declared_file_size"] == "5.1 MB"
    assert doc.meta["discovered_from"].endswith("/building-control-act/")

    law = by_url["https://sso.agc.gov.sg/SL/BCA1989-S666-2003"]
    assert law.meta["record_kind"] == "external_reference"
    assert "file_ext" not in law.meta


def test_sustainability_excludes_greenmark() -> None:
    records = _run("sustainability_legislation")
    assert not any("greenmark" in r.url for r in records)
    assert any(r.url.endswith("mei-code_1st-edition.pdf") for r in records)


def test_circulars_reads_the_whole_collection() -> None:
    records = _run("circulars")
    docs = [r for r in records if r.meta.get("record_kind") == "document"]
    assert len(docs) == 2, "only the rendered page was read"

    escalator = next(r for r in docs if r.url.endswith("Circular%20B.pdf"))
    assert escalator.name == "ESCALATOR MAINTENANCE"
    assert escalator.publish_date == "2018-02-18", "formatted date should normalise"

    web_only = next(r for r in records if r.url.endswith("/web-only-circular"))
    assert web_only.meta["record_kind"] == "page", (
        "a circular published as a web page is still a circular"
    )

    listing = next(r for r in records if r.url.endswith("/resources/circulars"))
    assert listing.meta["record_kind"] == "index_page"


def test_records_carry_source_identity() -> None:
    for record in _run("safety_and_standards"):
        assert record.source_id == "bca"
        assert record.source == "safety_and_standards"
        assert record.source_label == "Building and Construction Authority"


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
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
