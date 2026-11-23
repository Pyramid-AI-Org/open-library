"""Settings-to-feed contract for PAI-1741 department legislation."""
from collections import Counter
from pathlib import Path
import re
import sys
from urllib.parse import urlparse

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from crawlers.base import RunContext
from main import _get_all_crawlers_from_settings, _load_crawler_module

SETTINGS = yaml.safe_load(
    Path(__file__).resolve().parents[1].joinpath("config/settings.yaml").read_text()
)

# (source_id, page) -> (English chapters, chapters that must be present)
PAGES = {
    ("bd", "legislation"): (1, {"121"}),
    ("fsd", "fire_safety_legislation"): (3, {"502", "572", "636"}),
    ("fsd", "legislation"): (14, {"95", "95B", "132", "295", "295G"}),
    ("emsd", "legislation"): (32, {"51", "406", "406E", "449", "598", "610", "618", "470", "624", "211"}),
    ("labour", "legislation"): (53, {"56", "57", "59", "59I", "282", "360", "469", "509", "608"}),
    ("epd", "legislation"): (131, {"311", "354", "358", "400", "403", "466", "499", "603", "595B", "611A", "640", "312"}),
    ("wsd", "legislation"): (2, {"102", "102A"}),
    ("devb", "legislation"): (7, {"583", "587", "652"}),
    ("fehd", "legislation"): (61, {"172", "132X", "132CO"}),
    ("had", "legislation"): (12, {"344", "349", "626", "7"}),
    ("lands", "legislation"): (2, {"28"}),
    ("pland", "legislation"): (4, {"131"}),
}
EDITION = re.compile(r"/hk/cap(\w+?)!(en|zh-Hant-HK)\.pdf")


def crawl(source_id, page):
    ctx = RunContext(
        "2026-11-23",
        "2026-11-23T00:00:00Z",
        SETTINGS,
        source_id,
        SETTINGS["crawlers"][source_id]["label"],
    )
    return _load_crawler_module(f"{source_id}.{page}").Crawler().crawl(ctx)


def edition(url):
    return EDITION.fullmatch(urlparse(url).path).groups()


@pytest.mark.parametrize(("source_id", "page"), sorted(PAGES))
def test_legislation_page_emits_english_and_chinese_pdfs(source_id, page):
    definitions = _get_all_crawlers_from_settings(SETTINGS)
    assert (source_id, page, f"{source_id}.{page}") in definitions

    count, required = PAGES[(source_id, page)]
    records = crawl(source_id, page)
    editions = Counter(edition(r.url)[1] for r in records)
    english = {edition(r.url)[0] for r in records if edition(r.url)[1] == "en"}
    chinese = {edition(r.url)[0] for r in records if edition(r.url)[1] == "zh-Hant-HK"}
    assert editions["en"] == len(english) == count
    assert required <= english
    # Cap 123's subsidiaries are bilingual PDFs already; its principal is English only.
    assert chinese == english | ({"123"} if source_id == "bd" else set())
    for record in records:
        assert record.source_id == source_id
        assert record.source == f"{source_id}.{page}"
        assert record.meta["file_ext"] == "pdf"
        assert record.meta["ticket"] == "PAI-1741"
        assert urlparse(record.url).hostname == "www.elegislation.gov.hk"
        assert record.publish_date is None
        assert record.name.endswith(f"(Cap {edition(record.url)[0]})")


def test_each_edition_is_crawled_once():
    urls = [r.url for key in PAGES for r in crawl(*key)]
    cap123 = [r.url for r in crawl("bd", "buildings_ordinance")]
    assert len(urls) == len(set(urls)) == 322 + 323
    assert not set(urls) & set(cap123)


def test_emsd_ordinance_guidance_list():
    records = crawl("emsd", "ordinance_guidance")
    assert len(records) == 22
    assert {r.source for r in records} == {"emsd.ordinance_guidance"}
    assert all(urlparse(r.url).hostname == "www.emsd.gov.hk" for r in records)
    assert sum("Regulatory Regime" in r.name for r in records) == 14
    assert any(r.name.endswith("(2009 Edition, superseded)") for r in records)


def test_signed_duplicate_rule_keeps_words_ending_in_se():
    sweeps = [
        pc
        for cfg in SETTINGS["crawlers"].values()
        for name, pc in (cfg.get("pages") or {}).items()
        if name.endswith("_document_sweep")
    ]
    assert sweeps
    for sweep in sweeps:
        patterns = [re.compile(p) for p in sweep.get("exclude_url_patterns", [])]
        assert any(p.search("https://x/APP070se.pdf") for p in patterns)
        for kept in ("FCO_Leaflet_Japanese.pdf", "SuspectedCasesofPrescribedOccupationalDisease.pdf"):
            assert not any(p.search(f"https://x/{kept}") for p in patterns)


@pytest.mark.parametrize(
    ("source_id", "page_url"),
    [
        ("fsd", "https://www.hkfsd.gov.hk/eng/fire_protection/fs_industrial_building/"),
        ("fsd", "https://www.hkfsd.gov.hk/eng/fire_protection/cert/"),
        ("fsd", "https://www.hkfsd.gov.hk/eng/fire_protection/mini_storages/"),
        ("fsd", "https://es.hkfsd.gov.hk/dg/en/"),
        ("emsd", "https://www.emsd.gov.hk/en/other_regulatory_services/cooling_towers/index.html"),
        ("emsd", "https://www.emsd.gov.hk/en/energy_efficiency/DCS_in_HK/guidelines_forms/index.html"),
        ("bd", "https://www.bd.gov.hk/en/resources/legal-matters/index.html"),
    ],
)
def test_ordinance_guidance_pages_are_swept(source_id, page_url):
    sweep = SETTINGS["crawlers"][source_id]["pages"][f"{source_id}_document_sweep"]
    assert page_url in sweep["page_urls"]
    assert len(sweep["page_urls"]) == len(set(sweep["page_urls"]))
