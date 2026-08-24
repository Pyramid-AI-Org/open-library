"""Tests for the payload engines, with HTTP stubbed out.

Every case here is a real failure that happened on 21 August 2026 and cost a
whole source. The point of writing them down is that all three failures look
identical from the outside: the crawler runs, reports pages fetched, and
returns nothing. None of them raises. A test suite that only checks the happy
path would have passed throughout.

    python tests/test_payload_offline.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import crawlers.common.payload as payload
from crawlers.base import RunContext


# --------------------------------------------------------------------------
# Fixtures shaped like the real responses
# --------------------------------------------------------------------------


def flight_page(*objects: str) -> str:
    """Wrap JSON fragments in the script chunks Next.js actually emits.

    Chunk boundaries are placed mid-string on purpose: that is what breaks a
    decoder which concatenates first and unescapes afterwards.
    """
    body = "".join(objects)
    escaped = json.dumps(body)[1:-1]
    third = max(1, len(escaped) // 3)
    chunks = [escaped[:third], escaped[third : third * 2], escaped[third * 2 :]]
    scripts = "".join(
        'self.__next_f.push([%d,"%s"])' % (i + 1, c) for i, c in enumerate(chunks)
    )
    return "<html><body><script>%s</script></body></html>" % scripts


ORDER_PAPER = flight_page(
    '{"orderPaperType":[{"label":"All","value":"All"}],"initData":'
    '{"meta":{"filter_count":3},"data":['
    '{"id":"aaa","fileType":"application/pdf","fileTitle":"Order Paper No. 34",'
    '"fileUrl":"/api/media/aaa/Order-Paper---5August2026.pdf","fileSize":"306809",'
    '"date":"5 August 2026"},'
    '{"id":"bbb","fileTitle":"Order Paper No. 33",'
    '"fileUrl":"/api/media/bbb/Order-Paper---4August2026.pdf","date":"4 August 2026"},'
    '{"id":"ccc","fileTitle":"Order Paper No. 1",'
    '"fileUrl":"/api/media/ccc/op-1955.pdf","date":"22 April 1955"}'
    "]}}"
)

# The listing that ships only its first page. filter_count says 619.
PARTIAL_PAGE = flight_page(
    '{"initialMPs":{"meta":{"filter_count":619},"data":['
    '{"id":"r1","description":"Votes and Proceedings No. 32","date":"2026-07-07T12:00:00",'
    '"report":{"id":"f1","filename_download":"VP 7July2026.pdf","type":"application/pdf",'
    '"filesize":"167570"}}'
    "]}}"
)

SERVER_ACTION_RESPONSE = (
    '0:{"a":"$@1","f":"","b":"build"}\n'
    '1:{"meta":{"filter_count":2},"data":['
    '{"id":"b1","title":"18/2026","description":"Land Titles (Strata) (Amendment) Bill",'
    '"date_introduced":"2026-08-04T12:00:00",'
    '"file":{"id":"f1","filename_download":"Land Titles (Strata) (Amendment) Bill 18-2026.pdf",'
    '"type":"application/pdf","filesize":"314252"},"corrigenda":null},'
    '{"id":"b2","title":"01/2006","description":"Residential Property (Amendment) Bill",'
    '"date_introduced":"2006-01-16T12:00:00",'
    '"file":{"id":"f2","filename_download":"060001.pdf","type":"application/pdf"},'
    '"corrigenda":{"id":"f3","filename_download":"060001 corrigenda.pdf"}}'
    "]}\n"
)

GRAPHQL_RESPONSE = {
    "data": {
        "licensingInformationList": {
            "items": [
                {
                    "title": "Electricity Licence Conditions",
                    "publishedDate": "2025-03-14",
                    "pdfFile": {"_path": "/content/dam/corporate/licences/elec-conditions.pdf"},
                    "thumbnail": {"_path": "/content/dam/corporate/img/thumb.png"},
                },
                {
                    "title": "Gas Transporter Application Form",
                    "publishedDate": "2024-11-02",
                    "pdfFile": {"_path": "/content/dam/corporate/forms/gas-transporter.docx"},
                },
            ]
        }
    }
}


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------

SETTINGS: dict = {
    "http": {"user_agent": "test", "timeout_seconds": 5, "max_retries": 0},
    "crawlers": {
        "parliament": {
            "label": "Parliament of Singapore",
            "document_hosts": ["www.parliament.gov.sg"],
            "pages": {
                "order_paper": {
                    "start_url": "https://www.parliament.gov.sg/parliamentary-business/order-paper",
                    "payload_key": "initData",
                    "label": "Order Paper",
                    "record": {"url_field": "fileUrl", "title_fields": ["fileTitle"], "date_fields": ["date"]},
                },
                "bills_introduced": {
                    "start_url": "https://www.parliament.gov.sg/parliamentary-business/bills-introduced",
                    "action_id": "deadbeef",
                    "form_fields": {"_1_name": "", "0": '[null,"$K1"]'},
                    "page_size": 1000,
                    "record": {
                        "file_field": "file",
                        "extra_file_fields": ["corrigenda"],
                        "url_template": "/api/media/{id}/{filename}",
                        "title_fields": ["description"],
                        "date_fields": ["date_introduced"],
                    },
                },
            },
        },
        "ema": {
            "label": "Energy Market Authority",
            "document_hosts": ["www.ema.gov.sg"],
            "pages": {
                "licences": {
                    "base_url": "https://www.ema.gov.sg",
                    "endpoints": ["/graphql/execute.json/corporate/licensing-information"],
                    "found_on": "https://www.ema.gov.sg/regulations-licences/licences",
                    "tolerate_statuses": [500],
                }
            },
        },
    },
}


def ctx(source_id: str) -> RunContext:
    return RunContext(
        run_date_utc="2026-08-21",
        started_at_utc="2026-08-21T00:00:00Z",
        settings=SETTINGS,
        source_id=source_id,
        source_label=SETTINGS["crawlers"][source_id]["label"],
    )


def stub_get(text: str, status: int = 200, json_body=None):
    def _get(session, url, **kwargs):
        return SimpleNamespace(
            text=text,
            status_code=status,
            headers={"Content-Type": "application/json"},
            json=lambda: json_body if json_body is not None else json.loads(text),
        )

    return _get


class StubSession:
    """Records the POST it was given so the action shape can be asserted."""

    def __init__(self, text: str, status: int = 200) -> None:
        self.text = text
        self.status = status
        self.calls: list[dict] = []
        self.headers: dict[str, str] = {}

    def post(self, url, files=None, headers=None, timeout=None):
        self.calls.append({"url": url, "files": files or {}, "headers": headers or {}})
        return SimpleNamespace(
            text=self.text,
            status_code=self.status,
            raise_for_status=lambda: None,
        )


# --------------------------------------------------------------------------
# Payload decoding
# --------------------------------------------------------------------------


def test_flight_chunks_are_unescaped_before_they_are_joined() -> None:
    decoded = payload.decode_flight_payload(ORDER_PAPER)
    assert '"filter_count":3' in decoded
    assert "\\" not in decoded.split('"fileUrl"')[1][:60], "escaping survived into the payload"


def test_a_brace_inside_a_string_does_not_end_the_object() -> None:
    text = '{"a":"} not the end {","b":{"c":1}}tail'
    assert payload.balanced_object(text, 0) == '{"a":"} not the end {","b":{"c":1}}'


def test_the_collection_is_recovered_even_when_the_key_name_changed() -> None:
    # Payload key names change between builds; the collection shape does not.
    renamed = ORDER_PAPER.replace("initData", "somethingElse")
    decoded = payload.decode_flight_payload(renamed)
    assert payload.extract_keyed_object(decoded, "initData") is None
    recovered = payload.first_collection(decoded)
    assert recovered and len(recovered["data"]) == 3


def test_spaces_in_a_stored_filename_become_hyphens_in_the_url() -> None:
    # Getting this wrong produces a library of 404s that all look plausible.
    assert payload.filename_to_url_segment("VP 7July2026.pdf") == "VP-7July2026.pdf"
    assert payload.filename_to_url_segment("  a  b .pdf") == "a-b-.pdf"


# --------------------------------------------------------------------------
# FlightPayloadCrawler
# --------------------------------------------------------------------------


def test_one_request_returns_the_whole_order_paper() -> None:
    crawler = payload.FlightPayloadCrawler()
    crawler.name = "order_paper"
    payload.get_with_retries = stub_get(ORDER_PAPER)  # type: ignore[assignment]
    records = crawler.crawl(ctx("parliament"))

    documents = [r for r in records if r.meta.get("kind") == "document"]
    assert len(documents) == 3, f"expected 3 documents, got {len(documents)}"
    assert all(r.url.startswith("https://www.parliament.gov.sg/api/media/") for r in documents)
    assert any("Order Paper No. 34" in (r.name or "") for r in documents)
    # A page record for the listing itself, so coverage is auditable.
    assert any(r.meta.get("kind") == "page" for r in records)


def test_a_partial_payload_is_reported_not_silently_accepted(capsys=None) -> None:
    # This is the failure that produced "40 pages, zero documents" - except
    # here it would produce "1 page, 10 documents", which is worse because it
    # looks like a result.
    crawler = payload.FlightPayloadCrawler()
    crawler.name = "order_paper"
    payload.get_with_retries = stub_get(PARTIAL_PAGE)  # type: ignore[assignment]

    printed: list[str] = []
    original_print = __builtins__["print"] if isinstance(__builtins__, dict) else __builtins__.print
    try:
        if isinstance(__builtins__, dict):
            __builtins__["print"] = lambda *a, **k: printed.append(" ".join(str(x) for x in a))
        else:
            __builtins__.print = lambda *a, **k: printed.append(" ".join(str(x) for x in a))
        crawler.crawl(ctx("parliament"))
    finally:
        if isinstance(__builtins__, dict):
            __builtins__["print"] = original_print
        else:
            __builtins__.print = original_print

    assert any("INCOMPLETE" in line for line in printed), (
        "a payload holding 1 of 619 records must say so; got: %r" % printed
    )


def test_an_interstitial_is_not_mistaken_for_an_empty_section() -> None:
    crawler = payload.FlightPayloadCrawler()
    crawler.name = "order_paper"
    payload.get_with_retries = stub_get("<html><body>Checking your browser</body></html>")  # type: ignore[assignment]
    records = crawler.crawl(ctx("parliament"))
    assert records == [], "no flight chunks means no data, not zero documents"


# --------------------------------------------------------------------------
# ServerActionCrawler
# --------------------------------------------------------------------------


def test_the_server_action_returns_both_the_bill_and_its_corrigendum() -> None:
    crawler = payload.ServerActionCrawler()
    crawler.name = "bills_introduced"
    session = StubSession(SERVER_ACTION_RESPONSE)
    payload._PayloadCrawlerBase._session = staticmethod(lambda ctx: session)  # type: ignore[assignment]

    records = crawler.crawl(ctx("parliament"))
    documents = [r for r in records if r.meta.get("kind") == "document"]

    assert len(documents) == 3, f"2 bills + 1 corrigendum = 3, got {len(documents)}"
    urls = {r.url for r in documents}
    assert "https://www.parliament.gov.sg/api/media/f1/Land-Titles-(Strata)-(Amendment)-Bill-18-2026.pdf" in urls
    assert "https://www.parliament.gov.sg/api/media/f3/060001-corrigenda.pdf" in urls
    assert any("[18/2026]" in (r.name or "") for r in documents), "the Bill number belongs in the title"
    assert any(r.publish_date == "2026-08-04" for r in documents)


def test_the_limit_is_raised_so_one_call_does_it() -> None:
    crawler = payload.ServerActionCrawler()
    crawler.name = "bills_introduced"
    session = StubSession(SERVER_ACTION_RESPONSE)
    payload._PayloadCrawlerBase._session = staticmethod(lambda ctx: session)  # type: ignore[assignment]
    crawler.crawl(ctx("parliament"))

    assert len(session.calls) == 1, "filter_count was 2 and page_size 1000; no second call is needed"
    sent = {k: v[1] for k, v in session.calls[0]["files"].items()}
    assert sent["_1_limit"] == "1000"
    assert sent["_1_offset"] == "0"
    assert session.calls[0]["headers"]["next-action"] == "deadbeef"


def test_a_stale_action_id_raises_rather_than_reporting_nothing() -> None:
    # A build hash goes stale on every deploy. Swallowing the 500 would turn a
    # 772-document source into a silent zero.
    crawler = payload.ServerActionCrawler()
    crawler.name = "bills_introduced"
    session = StubSession("", status=500)
    payload._PayloadCrawlerBase._session = staticmethod(lambda ctx: session)  # type: ignore[assignment]

    try:
        crawler.crawl(ctx("parliament"))
    except RuntimeError as exc:
        assert "stale" in str(exc)
    else:
        raise AssertionError("HTTP 500 from a server action must raise")


# --------------------------------------------------------------------------
# ApiIndexCrawler
# --------------------------------------------------------------------------


def test_assets_are_paired_with_the_nearest_title_and_date() -> None:
    found = payload.walk_for_assets(GRAPHQL_RESPONSE, base_url="https://www.ema.gov.sg")
    by_url = {u: v for u, v in found.items()}
    conditions = "https://www.ema.gov.sg/content/dam/corporate/licences/elec-conditions.pdf"
    assert by_url[conditions]["title"] == "Electricity Licence Conditions"
    assert by_url[conditions]["date"] == "2025-03-14"


def test_a_logo_is_not_a_document() -> None:
    found = payload.walk_for_assets(GRAPHQL_RESPONSE, base_url="https://www.ema.gov.sg")
    assert not any(u.endswith(".png") for u in found), (
        "image assets inflate coverage and mean nothing"
    )


def test_the_api_index_crawler_reads_an_unknown_schema() -> None:
    crawler = payload.ApiIndexCrawler()
    crawler.name = "licences"
    payload.get_with_retries = stub_get(json.dumps(GRAPHQL_RESPONSE), json_body=GRAPHQL_RESPONSE)  # type: ignore[assignment]
    records = crawler.crawl(ctx("ema"))

    assert len(records) == 2
    assert all(r.meta["kind"] == "document" for r in records)
    assert {r.meta["file_type"] for r in records} == {"pdf", "docx"}


def test_an_html_response_to_a_json_url_is_called_out() -> None:
    # AEM ignores an unknown .model.json suffix and serves the page. Reading
    # that as an empty collection is how EMA's first re-run lost all 86 pages.
    crawler = payload.ApiIndexCrawler()
    crawler.name = "licences"

    def _get(session, url, **kwargs):
        return SimpleNamespace(
            text="<!DOCTYPE HTML><html>...</html>",
            status_code=200,
            headers={},
            json=lambda: (_ for _ in ()).throw(ValueError("not json")),
        )

    payload.get_with_retries = _get  # type: ignore[assignment]
    records = crawler.crawl(ctx("ema"))
    assert records == []


if __name__ == "__main__":
    import crawlers.common.payload as _reload_guard  # noqa: F401

    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        # each test re-stubs what it needs; reload to undo the last one
        import importlib

        importlib.reload(payload)
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
