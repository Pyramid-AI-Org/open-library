"""Settings-to-feed contract for PAI-1741 department legislation."""
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

# (source_id, page) -> (document count, chapters that must be present)
PAGES = {
    ("bd", "legislation"): (1, {"121"}),
    ("fsd", "fire_safety_legislation"): (3, {"502", "572", "636"}),
    ("fsd", "legislation"): (14, {"95", "95B", "132", "295", "295G"}),
    ("emsd", "legislation"): (29, {"51", "406", "406E", "449", "598", "610", "618", "470", "624"}),
    ("labour", "legislation"): (53, {"56", "57", "59", "59I", "282", "360", "469", "509", "608"}),
    ("epd", "legislation"): (129, {"311", "354", "358", "400", "403", "466", "499", "603", "595B", "611A", "640"}),
    ("wsd", "legislation"): (2, {"102", "102A"}),
}


def crawl(source_id, page):
    ctx = RunContext(
        "2026-11-23",
        "2026-11-23T00:00:00Z",
        SETTINGS,
        source_id,
        SETTINGS["crawlers"][source_id]["label"],
    )
    return _load_crawler_module(f"{source_id}.{page}").Crawler().crawl(ctx)


def chapter(url):
    return re.fullmatch(r"/hk/cap(\w+)!en\.pdf", urlparse(url).path).group(1)


@pytest.mark.parametrize(("source_id", "page"), sorted(PAGES))
def test_legislation_page_is_registered_and_emits_english_pdfs(source_id, page):
    definitions = _get_all_crawlers_from_settings(SETTINGS)
    assert (source_id, page, f"{source_id}.{page}") in definitions

    count, required = PAGES[(source_id, page)]
    records = crawl(source_id, page)
    assert len(records) == count
    chapters = {chapter(r.url) for r in records}
    assert len(chapters) == count
    assert required <= chapters
    for record in records:
        assert record.source_id == source_id
        assert record.source == f"{source_id}.{page}"
        assert record.meta["file_ext"] == "pdf"
        assert record.meta["ticket"] == "PAI-1741"
        assert urlparse(record.url).hostname == "www.elegislation.gov.hk"
        assert record.publish_date is None
        assert record.name.endswith(f"(Cap {chapter(record.url)})")


def test_each_chapter_is_crawled_once():
    urls = [r.url for key in PAGES for r in crawl(*key)]
    urls += [r.url for r in crawl("bd", "buildings_ordinance")]
    assert len(urls) == len(set(urls)) == 231 + 18
    assert not any("cap123" in r.url for key in PAGES for r in crawl(*key))


@pytest.mark.parametrize(
    ("source_id", "page_url"),
    [
        ("fsd", "https://www.hkfsd.gov.hk/eng/fire_protection/fs_industrial_building/"),
        ("fsd", "https://www.hkfsd.gov.hk/eng/fire_protection/cert/"),
        ("fsd", "https://es.hkfsd.gov.hk/dg/en/"),
        ("emsd", "https://www.emsd.gov.hk/en/other_regulatory_services/cooling_towers/index.html"),
    ],
)
def test_ordinance_guidance_pages_are_swept(source_id, page_url):
    sweep = SETTINGS["crawlers"][source_id]["pages"][f"{source_id}_document_sweep"]
    assert page_url in sweep["page_urls"]
    assert len(sweep["page_urls"]) == len(set(sweep["page_urls"]))
