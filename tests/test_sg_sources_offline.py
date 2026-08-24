"""Wiring tests for the Singapore sources, with HTTP stubbed out.

These run the real crawler modules through the real config/settings.sg.yaml, so a
settings key that never reaches the code fails here rather than in production.
They concentrate on what is genuinely new in this batch: Sitecore and
Sitefinity asset-path filtering, cache-buster stripping, sitemap indexes, and
a source that collects pages rather than documents.

    python tests/test_sg_sources_offline.py
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

import crawlers.common.isomer as isomer
import crawlers.common.spider as spider
from crawlers.base import RunContext

SETTINGS = yaml.safe_load(Path("config/settings.sg.yaml").read_text())

PAGES: dict[str, str] = {}
FETCHED: list[str] = []


def _fake_get(session, url, **kwargs):
    FETCHED.append(url)
    if url not in PAGES:
        raise RuntimeError(f"unexpected fetch: {url}")
    return SimpleNamespace(
        text=PAGES[url], status_code=200, headers={"Content-Type": "text/html"}
    )


for module in (isomer, spider):
    module.get_with_retries = _fake_get
    module.sleep_seconds = lambda _s: None


def ctx(source_id: str, label: str = "test") -> RunContext:
    return RunContext(
        run_date_utc="2026-08-21",
        started_at_utc="2026-08-21T00:00:00+00:00",
        settings=SETTINGS,
        source_id=source_id,
        source_label=label,
    )


def run(source_id: str, module_name: str):
    mod = importlib.import_module(f"crawlers.{source_id}.{module_name}")
    return mod.Crawler().crawl(ctx(source_id))


def page(title: str, body: str, host_nav: str = "/about-us") -> str:
    return (
        f"<html><head><title>{title}</title></head><body>"
        f'<header><a href="{host_nav}">About</a>'
        f'<a href="https://www.facebook.com/x">Facebook</a></header>'
        f"<main><h1>{title}</h1>{body}</main>"
        f'<footer><a href="/newsroom">Newsroom</a></footer></body></html>'
    )


# ---------------------------------------------------------------------------
# Sitefinity: SCDF. Documents under /docs/default-source/ with ?sfvrsn=.
# ---------------------------------------------------------------------------

SCDF = "https://www.scdf.gov.sg"
PAGES[f"{SCDF}/fire-safety-services-listing/downloads/acts-codes-and-regulations"] = page(
    "Acts, Codes and Regulations",
    f'<h2>Fire Code</h2>'
    f'<a href="{SCDF}/docs/default-source/firecode-whole-document-with-annotations/'
    f'firecode_full_latest.pdf?sfvrsn=a794fc8b_0">Fire Code 2023</a>'
    f'<h2>Circular</h2>'
    f'<a href="{SCDF}/docs/default-source/fire-safety-docs/circular-amendments.pdf">'
    f"Amendments to Fire Code 2023</a>"
    # An asset outside the document tree: a stylesheet-ish PDF must not count.
    f'<a href="{SCDF}/assets/brochure.pdf">Corporate brochure</a>'
    # And a CORENET circular: not SCDF's host, but SCDF's circular.
    f'<a href="https://info.corenet.gov.sg/docs/default-source/scdf-circulars/x.pdf'
    f'?sfvrsn=1_1">SCDF circular on CORENET</a>',
)


def test_sitefinity_documents_are_filtered_by_asset_path() -> None:
    records = run("scdf", "acts_codes_and_regulations")
    docs = [r for r in records if r.meta["record_kind"] == "document"]
    assert docs, "no documents recorded"
    for record in docs:
        assert "/docs/default-source/" in record.url, record.url
    assert not any("brochure.pdf" in r.url for r in records), (
        "a PDF outside the document tree was recorded"
    )
    assert not any("facebook" in r.url for r in records)
    assert not any("/newsroom" in r.url for r in records)


def test_cache_buster_is_stripped_from_identity_but_kept_for_fetching() -> None:
    records = run("scdf", "acts_codes_and_regulations")
    code = next(r for r in records if "firecode_full_latest" in r.url)
    # Sitefinity re-stamps sfvrsn whenever an asset is republished. Left in the
    # identity, the same document reappears as new on every run.
    assert "sfvrsn" not in code.url
    assert "sfvrsn" in code.meta["fetch_url"]


def test_a_regulators_circular_on_a_shared_portal_is_kept_as_external() -> None:
    records = run("scdf", "acts_codes_and_regulations")
    corenet = next(r for r in records if "corenet" in r.url)
    assert corenet.meta["record_kind"] == "external_reference"
    assert "sfvrsn" not in corenet.url


# ---------------------------------------------------------------------------
# Sitecore + sitemap index: MOM.
# ---------------------------------------------------------------------------

MOM = "https://www.mom.gov.sg"
PAGES[f"{MOM}/sitemap.xml"] = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<sitemap><loc>https://www.mom.gov.sg/workplace-safety-and-health.xml</loc></sitemap>
</sitemapindex>"""
PAGES[f"{MOM}/workplace-safety-and-health.xml"] = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>{MOM}/workplace-safety-and-health/wsh-circulars</loc>
<lastmod>2026-03-16T00:00:00Z</lastmod></url>
<url><loc>{MOM}/newsroom/press-release</loc></url>
</urlset>"""
PAGES[f"{MOM}/workplace-safety-and-health/wsh-circulars"] = page(
    "WSH Circulars",
    f'<h2>2026</h2><a href="{MOM}/-/media/mom/documents/safety-health/circulars/2026/'
    f'circular-20260316-vertical-lifters.pdf">Vertical lifters in conveyor systems</a>'
    f'<a href="{MOM}/-/media/other/brochure.pdf">Brochure outside the document tree</a>',
)


def test_a_sitemap_index_is_followed_to_its_children() -> None:
    records = run("mom", "wsh_circulars")
    assert records, (
        "nothing collected - a sitemap index read as a urlset yields zero pages "
        "and looks like a healthy crawler finding nothing"
    )
    assert f"{MOM}/workplace-safety-and-health.xml" in FETCHED
    circular = next(r for r in records if "vertical-lifters" in r.url)
    assert circular.meta["record_kind"] == "document"
    assert circular.name == "Vertical lifters in conveyor systems"


def test_sitemap_pages_outside_the_prefix_are_not_fetched() -> None:
    run("mom", "wsh_circulars")
    assert not any("/newsroom/" in u for u in FETCHED), "crawled outside the scope prefix"


def test_sitecore_documents_are_filtered_by_asset_path() -> None:
    records = run("mom", "wsh_circulars")
    assert not any("/-/media/other/" in r.url for r in records), (
        "a file outside /-/media/mom/documents/ was recorded"
    )


def test_the_page_record_carries_the_sitemap_date() -> None:
    records = run("mom", "wsh_circulars")
    page_record = next(r for r in records if r.meta["record_kind"] == "page")
    assert page_record.publish_date == "2026-03-16"


# ---------------------------------------------------------------------------
# Isomer collection: SLA circulars, the 332-item listing.
# ---------------------------------------------------------------------------

SLA = "https://www.sla.gov.sg"
PAGES[f"{SLA}/news/circulars/"] = (
    '<html><body><main id="main-content"><h1>Circulars</h1>'
    '<a href="https://isomer-user-content.by.gov.sg/73/aaa/CS%20Circular%204-2025.pdf">'
    "30 Dec 2025</a></main>"
    '<script>self.__next_f.push([1,"'
    '{\\"referenceLinkHref\\":\\"https://isomer-user-content.by.gov.sg/73/aaa/'
    'CS Circular 4-2025.pdf\\",'
    '\\"itemTitle\\":\\"CS Circular 4-2025 - Determining Strata Area\\",'
    '\\"formattedDate\\":\\"30 December 2025\\"},'
    '{\\"referenceLinkHref\\":\\"https://isomer-user-content.by.gov.sg/73/bbb/'
    'notes03.pdf\\",'
    '\\"itemTitle\\":\\"Consolidated Practice Circulars 2003\\",'
    '\\"formattedDate\\":\\"1 January 2003\\"}'
    '"])</script></body></html>'
)


def test_the_whole_circular_back_catalogue_comes_out_of_one_response() -> None:
    records = run("sla", "circulars")
    docs = [r for r in records if r.meta["record_kind"] == "document"]
    assert len(docs) == 2, "only the rendered page was read"
    old = next(r for r in docs if "notes03" in r.url)
    assert old.publish_date == "2003-01-01", "a 2003 circular should date to 2003"
    assert old.name == "Consolidated Practice Circulars 2003"


# ---------------------------------------------------------------------------
# A source that collects pages, not documents: SGDI.
# ---------------------------------------------------------------------------

SGDI = "https://www.sgdi.gov.sg"
PAGES[f"{SGDI}/ministries"] = page(
    "Ministries",
    f'<a href="{SGDI}/ministries/mccy">Ministry of Culture, Community and Youth</a>'
    f'<a href="{SGDI}/statutory-boards">Statutory Boards</a>',
    host_nav="/about",
)
PAGES[f"{SGDI}/ministries/mccy"] = page(
    "Ministry of Culture, Community and Youth",
    f'<a href="{SGDI}/ministries/mccy/departments/ahd">Arts House Department</a>',
    host_nav="/about",
)
PAGES[f"{SGDI}/ministries/mccy/departments/ahd"] = page("Arts House Department", "<p>Contacts</p>")


def test_a_directory_source_collects_pages_and_stays_in_its_branch() -> None:
    records = run("sgdi", "ministries")
    kinds = {r.meta["record_kind"] for r in records}
    assert kinds == {"page"}, f"expected only page records, got {kinds}"
    urls = {r.url for r in records}
    assert f"{SGDI}/ministries/mccy" in urls
    assert f"{SGDI}/ministries/mccy/departments/ahd" in urls, "did not descend to the unit page"
    # /statutory-boards is a sibling branch with its own crawler row.
    assert not any("/statutory-boards" in u for u in urls), "crossed into a sibling row"


def test_every_sg_source_resolves_its_settings() -> None:
    # A settings key that never reaches the code is the commonest silent
    # failure in this repository, and it looks exactly like an empty section.
    sources = [
        "pub", "nea", "scdf", "mom", "ura", "sla", "hdb",
        "jtc", "ema", "nparks", "parliament", "sgdi", "mnd",
    ]
    for source_id in sources:
        pages = SETTINGS["crawlers"][source_id]["pages"]
        assert pages, f"{source_id} has no pages configured"
        for crawler_name in pages:
            module = importlib.import_module(f"crawlers.{source_id}.{crawler_name}")
            crawler = module.Crawler()
            assert crawler.name == crawler_name, (
                f"{source_id}.{crawler_name}: Crawler.name is {crawler.name!r}; "
                "it must match the settings key or the config resolves empty"
            )
            cfg = ctx(source_id).get_crawler_config(crawler_name)
            target_keys = (
                "listing_urls",      # index-page crawlers
                "start_urls",        # section spiders
                "start_url",         # payload crawlers - one entry point
                "path_prefixes",     # sitemap-driven rows
                "allowed_path_prefixes",
                "endpoints",         # api-index crawlers
            )
            assert any(cfg.get(k) for k in target_keys), (
                f"{source_id}.{crawler_name} has no target configured"
            )
            assert cfg.get("document_hosts"), f"{source_id}.{crawler_name} inherits no document hosts"


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
