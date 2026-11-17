"""Settings-to-feed contract for PAI-1696 Cap 123 / Minor Works."""
from copy import deepcopy
from pathlib import Path
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

EXPECTED_CAPS = {
    "cap123!en.pdf",
    "cap123A!en-zh-Hant-HK.pdf",
    "cap123B!en-zh-Hant-HK.pdf",
    "cap123C!en-zh-Hant-HK.pdf",
    "cap123D!en-zh-Hant-HK.pdf",
    "cap123E!en-zh-Hant-HK.pdf",
    "cap123F!en-zh-Hant-HK.pdf",
    "cap123G!en-zh-Hant-HK.pdf",
    "cap123H!en-zh-Hant-HK.pdf",
    "cap123I!en-zh-Hant-HK.pdf",
    "cap123J!en-zh-Hant-HK.pdf",
    "cap123K!en-zh-Hant-HK.pdf",
    "cap123L!en-zh-Hant-HK.pdf",
    "cap123M!en-zh-Hant-HK.pdf",
    "cap123N!en-zh-Hant-HK.pdf",
    "cap123O!en-zh-Hant-HK.pdf",
    "cap123P!en-zh-Hant-HK.pdf",
    "cap123Q!en-zh-Hant-HK.pdf",
}


def context():
    return RunContext(
        "2026-11-17",
        "2026-11-17T00:00:00Z",
        SETTINGS,
        "bd",
        SETTINGS["crawlers"]["bd"]["label"],
    )


def test_buildings_ordinance_inventory_is_registered():
    definitions = _get_all_crawlers_from_settings(SETTINGS)
    assert ("bd", "buildings_ordinance", "bd.buildings_ordinance") in definitions
    assert ("bd", "minor_works", "bd.minor_works") in definitions

    records = _load_crawler_module("bd.buildings_ordinance").Crawler().crawl(context())
    assert len(records) == 18
    endings = {urlparse(r.url).path.rsplit("/", 1)[-1] for r in records}
    assert endings == EXPECTED_CAPS
    for record in records:
        assert record.source_id == "bd"
        assert record.source == "bd.buildings_ordinance"
        assert record.meta["file_ext"] == "pdf"
        assert record.meta["ticket"] == "PAI-1696"
        assert urlparse(record.url).hostname == "www.elegislation.gov.hk"
        assert record.publish_date is None
    assert any("Cap 123N" in r.name for r in records)
    assert any("Cap 123I" in r.name for r in records)
    assert any("Cap 123C" in r.name for r in records)


def test_minor_works_spider_covers_item_family_and_indexes():
    cfg = SETTINGS["crawlers"]["bd"]["pages"]["minor_works"]
    assert cfg["emit_page_records"] is True
    assert cfg["max_pages"] >= 200
    assert "/en/building-works/minor-works/" in cfg["allowed_path_prefixes"]
    assert "/en/resources/legal-matters/" in cfg["allowed_path_prefixes"]
    starts = cfg["start_urls"]
    assert any(u.endswith("types-of-minor-works/index.html") for u in starts)
    assert any(u.endswith("minor-works-items/index.html") for u in starts)
    assert any("introduction-to-minor-works-control-system" in u for u in starts)
    assert SETTINGS["crawlers"]["bd"]["schedule"]["interval_days"] == 1


def test_document_list_ticket_is_configurable():
    records = _load_crawler_module("bd.buildings_ordinance").Crawler().crawl(context())
    assert all(r.meta["ticket"] == "PAI-1696" for r in records)


def test_rejects_wrong_host_or_non_pdf_instead_of_publishing_bad_records():
    for bad_url in [
        "https://example.com/cap123.pdf",
        "https://www.elegislation.gov.hk/hk/cap123!en.html",
    ]:
        settings = deepcopy(SETTINGS)
        settings["crawlers"]["bd"]["pages"]["buildings_ordinance"]["documents"][0]["url"] = bad_url
        with pytest.raises(ValueError):
            _load_crawler_module("bd.buildings_ordinance").Crawler().crawl(
                RunContext(
                    "2026-11-17",
                    "2026-11-17T00:00:00Z",
                    settings,
                    "bd",
                    settings["crawlers"]["bd"]["label"],
                )
            )
