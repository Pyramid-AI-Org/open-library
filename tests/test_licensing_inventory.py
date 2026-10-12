"""Settings-to-feed contract for PAI-1457, with no network or paid parsing."""
from copy import deepcopy
from pathlib import Path
import sys
from urllib.parse import urlparse

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from main import _get_all_crawlers_from_settings, _load_crawler_module, _merge_records_for_latest
from crawlers.base import RunContext

SETTINGS = yaml.safe_load(Path(__file__).resolve().parents[1].joinpath("config/settings.yaml").read_text())
EXPECTED = {"fehd": 5, "swd": 4, "had": 5, "edb": 1, "police": 1}


def context(department, settings=SETTINGS):
    return RunContext("2026-10-12", "2026-10-12T00:00:00Z", settings,
                      department, settings["crawlers"][department]["label"])


def test_reviewed_inventory_loads_through_the_real_runner():
    all_urls = set()
    definitions = _get_all_crawlers_from_settings(SETTINGS)
    for dept, count in EXPECTED.items():
        assert (dept, "licensing_guides", f"{dept}.licensing_guides") in definitions
        crawler = _load_crawler_module(f"{dept}.licensing_guides").Crawler()
        records = crawler.crawl(context(dept))
        assert records == crawler.crawl(context(dept))
        assert len(records) == count
        for record in records:
            assert record.url not in all_urls
            all_urls.add(record.url)
            assert record.source_id == dept
            assert record.source == f"{dept}.licensing_guides"
            # The file processor stores file_ext verbatim as file_type;
            # citation PDFViewer requires 'pdf', not '.pdf'.
            assert record.meta["file_ext"] == "pdf"
            assert record.name
            assert record.publish_date is None
            assert urlparse(record.url).hostname in SETTINGS["crawlers"][dept]["pages"]["licensing_guides"]["document_hosts"]
    assert len(all_urls) == 16


def test_school_document_is_described_as_extension_not_new_registration():
    record = _load_crawler_module("edb.licensing_guides").Crawler().crawl(context("edb"))[0]
    assert "Extension of School Premises" in record.name


def test_catalog_refresh_replaces_the_section_instead_of_retaining_retired_urls():
    from dataclasses import asdict
    crawler = _load_crawler_module("fehd.licensing_guides").Crawler()
    records = [asdict(record) for record in crawler.crawl(context("fehd"))]
    retired = {**records[0], "url": "https://www.fehd.gov.hk/retired.pdf"}
    previous = {"fehd.licensing_guides": {retired["url"]: retired}}
    merged = _merge_records_for_latest(previous, {"fehd.licensing_guides": records})
    assert len(merged) == 5
    assert retired["url"] not in {record["url"] for record in merged}


def test_rejects_wrong_host_or_non_pdf_instead_of_publishing_bad_records():
    for bad_url in ["https://example.com/a.pdf", "https://www.fehd.gov.hk/a.html"]:
        settings = deepcopy(SETTINGS)
        settings["crawlers"]["fehd"]["pages"]["licensing_guides"]["documents"][0]["url"] = bad_url
        with pytest.raises(ValueError):
            _load_crawler_module("fehd.licensing_guides").Crawler().crawl(context("fehd", settings))
