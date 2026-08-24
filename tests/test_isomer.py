"""Parser tests for crawlers/common/isomer.py.

These use fixtures shaped like the real BCA markup rather than live requests,
so they run in CI without network access and keep passing when the site is
down. They cover the four things that actually break when an Isomer site is
restyled: content scoping, aria-label titles, sitemap parsing and the flight
payload decode.

Run from the repository root:

    python -m pytest tests/test_isomer.py -q
    # or, with no pytest available:
    python tests/test_isomer.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crawlers.common.isomer import (  # noqa: E402
    PageLink,
    assign_titles,
    decode_flight_payload,
    is_weak_label,
    extract_collection_items,
    extract_page_links,
    extract_page_title,
    parse_sitemap,
    split_label,
    strip_query,
)


PAGE_HTML = """<!DOCTYPE html><html lang="en" data-theme="isomer-next"><head>
<title>Building Control Act | Building and Construction Authority</title></head><body>
<header><a href="/about-us/">About us</a><a href="/e-services/">e-Services</a></header>
<main id="main-content" class="focus-visible:outline-none">
  <h1>Building Control Act</h1>
  <a target="_blank" rel="noopener nofollow"
     aria-label="Approved Document [PDF, 5.1 MB] (opens in new tab)"
     href="https://isomer-user-content.by.gov.sg/338/abc/approved-document-v7-07.pdf">
     <span>Approved Document</span></a>
  <a aria-label="Building Control Act (opens in new tab)"
     href="https://sso.agc.gov.sg/Act/BCA1989">Building Control Act</a>
  <a href="/safety-and-standards/accessibility/">Accessibility</a>
  <img src="/x.png"/>
  <a aria-label="Code on Accessibility in the Built Environment 2025 [PDF, 332 KB] (opens in new tab)"
     href="https://isomer-user-content.by.gov.sg/338/def/accessibility-2025.pdf">Code 2025</a>
</main>
<footer><a href="https://www.facebook.com/BCASingapore">Facebook</a></footer>
</body></html>"""


SITEMAP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://www1.bca.gov.sg/</loc><lastmod>2026-08-05T06:54:31.504Z</lastmod></url>
<url><loc>https://www1.bca.gov.sg/safety-and-standards/building-control-act/</loc>
<lastmod>2026-07-30T00:00:00.000Z</lastmod></url>
<url><loc>https://www1.bca.gov.sg/resources/newsroom/some-release/</loc></url>
</urlset>"""


# One real-shaped flight chunk. The doubled backslashes here are Python source
# escaping; the file on the wire contains \" exactly as Next.js emits it.
COLLECTION_HTML = (
    '<!DOCTYPE html><html><body><main id="main-content"><h1>Circulars</h1></main>'
    '<script>self.__next_f.push([1,"'
    '{\\"id\\":\\"item-1\\",\\"date\\":\\"$D2026-08-03T00:00:00.000Z\\",'
    '\\"title\\":\\"CIRCULAR ON NEW FEEDBACK CHANNEL [PDF, 1.2 MB]\\",'
    '\\"referenceLinkHref\\":\\"https://isomer-user-content.by.gov.sg/338/aaa/Circular A.pdf\\",'
    '\\"itemTitle\\":\\"CIRCULAR ON NEW FEEDBACK CHANNEL [PDF, 1.2 MB]\\",'
    '\\"formattedDate\\":\\"3 August 2026\\"},'
    '{\\"id\\":\\"item-2\\",'
    '\\"referenceLinkHref\\":\\"https://isomer-user-content.by.gov.sg/338/bbb/Circular B.pdf\\",'
    '\\"itemTitle\\":\\"MORE VIGILANCE REQUIRED IN ESCALATOR MAINTENANCE [PDF, 33 KB]\\",'
    '\\"formattedDate\\":\\"18 February 2018\\"}'
    '"])</script></body></html>'
)


def test_links_are_scoped_to_main_content() -> None:
    links = extract_page_links(
        PAGE_HTML, base_url="https://www1.bca.gov.sg/safety-and-standards/building-control-act/"
    )
    hrefs = [link.href for link in links]
    assert not any("facebook.com" in h for h in hrefs), "footer leaked into results"
    assert not any("/about-us/" in h for h in hrefs), "header leaked into results"
    assert "https://isomer-user-content.by.gov.sg/338/abc/approved-document-v7-07.pdf" in hrefs
    assert "https://sso.agc.gov.sg/Act/BCA1989" in hrefs
    # A self-closing <img> inside the scope must not close the scope early.
    assert any("accessibility-2025.pdf" in h for h in hrefs), "scope closed too early"


def test_relative_links_resolve_against_the_page() -> None:
    links = extract_page_links(
        PAGE_HTML, base_url="https://www1.bca.gov.sg/safety-and-standards/building-control-act/"
    )
    assert "https://www1.bca.gov.sg/safety-and-standards/accessibility/" in [
        link.href for link in links
    ]


def test_scoping_falls_back_when_the_id_is_absent() -> None:
    html = '<html><body><a href="/a.pdf">A</a></body></html>'
    links = extract_page_links(html, base_url="https://x.test/", element_id="main-content")
    assert [link.href for link in links] == ["https://x.test/a.pdf"]


def test_aria_label_beats_anchor_text() -> None:
    links = extract_page_links(PAGE_HTML, base_url="https://www1.bca.gov.sg/")
    doc = next(link for link in links if link.href.endswith("approved-document-v7-07.pdf"))
    title, hints = split_label(doc.best_label)
    assert title == "Approved Document"
    assert hints == {"declared_file_type": "pdf", "declared_file_size": "5.1 MB"}


def test_weak_labels_fall_back_to_the_heading() -> None:
    # Anchor text like "Download" or "this circular" is common on government
    # pages and useless as a record name; the heading above it is the real one.
    for junk in ("Download", "this circular", "click here", "refer to this guide"):
        assert is_weak_label(junk), junk
    for real in ("Approved Document", "Code of Practice on Buildability 2022"):
        assert not is_weak_label(real), real

    link = PageLink(
        href="https://x.test/accessibilitycode2019.pdf",
        text="Download",
        aria_label="",
        heading="Code on Accessibility in the Built Environment 2019",
    )
    assert link.best_label == "Code on Accessibility in the Built Environment 2019"


def test_a_strong_label_still_keeps_its_file_hint() -> None:
    # best_label must return the label intact so split_label can lift the size
    # into metadata; stripping it early would lose the hint.
    link = PageLink(
        href="https://x.test/a.pdf",
        text="Download",
        aria_label="Approved Document [PDF, 5.1 MB] (opens in new tab)",
        heading="Irrelevant",
    )
    title, hints = split_label(link.best_label)
    assert title == "Approved Document"
    assert hints["declared_file_size"] == "5.1 MB"


def test_file_hint_mid_label_ends_the_title() -> None:
    # Card layouts wrap the title, the size hint and a description in one
    # anchor. The hint marks where the title stops; the rest is not a title.
    title, hints = split_label(
        "Guide to Universal Design index (UDi) 2022 [PDF, 6.4 MB]"
        "Designed for architects, designers and developers."
    )
    assert title == "Guide to Universal Design index (UDi) 2022"
    assert hints["declared_file_size"] == "6.4 MB"

    # And the same hint with its opening bracket lost to markup.
    title, hints = split_label("Addendum to Certification Standard (GM 3.0), Sep 2014 PDF, 81 KB]")
    assert title == "Addendum to Certification Standard (GM 3.0), Sep 2014"
    assert hints["declared_file_size"] == "81 KB"

    # A genuine bracketed prefix is part of the title and must survive.
    title, _ = split_label("[Updated] Circular on Streamlining of COVID-19 Requirements")
    assert title == "[Updated] Circular on Streamlining of COVID-19 Requirements"


def test_a_shared_heading_names_nobody() -> None:
    # A "Codes of Practice" heading over several links names the group, not
    # any member of it. Using it would produce records that cannot be told
    # apart - worse than filenames, which at least differ per document.
    links = [
        PageLink(href="https://x.test/a/RT-COP_V2.0_April_2019.pdf", text="Download",
                 aria_label="", heading="Codes of Practice"),
        PageLink(href="https://x.test/a/works_on_public_streets.pdf",
                 text="Code of Practice for Works on Public Streets",
                 aria_label="", heading="Codes of Practice"),
    ]
    titles = {link.href: title for link, title, _ in assign_titles(links)}
    assert titles["https://x.test/a/RT-COP_V2.0_April_2019.pdf"] != "Codes of Practice"
    assert "RT COP" in titles["https://x.test/a/RT-COP_V2.0_April_2019.pdf"]
    # The well-labelled sibling is untouched.
    assert titles["https://x.test/a/works_on_public_streets.pdf"] == (
        "Code of Practice for Works on Public Streets"
    )

    # A heading covering exactly one link does identify it, and is used.
    solo = [
        PageLink(href="https://x.test/accessibilitycode2019.pdf", text="Download",
                 aria_label="", heading="Code on Accessibility in the Built Environment 2019"),
        PageLink(href="https://x.test/other.pdf", text="Approved Document",
                 aria_label="", heading="Approved Document"),
    ]
    got = {link.href: title for link, title, _ in assign_titles(solo)}
    assert got["https://x.test/accessibilitycode2019.pdf"] == (
        "Code on Accessibility in the Built Environment 2019"
    )


def test_page_title_prefers_h1() -> None:
    assert extract_page_title(PAGE_HTML) == "Building Control Act"


def test_sitemap_parsing_keeps_lastmod() -> None:
    entries, nested = parse_sitemap(SITEMAP_XML)
    assert len(entries) == 3
    assert nested == []
    act = next(e for e in entries if e.url.endswith("/building-control-act/"))
    assert act.lastmod == "2026-07-30T00:00:00.000Z"
    assert entries[-1].lastmod is None


def test_a_sitemap_index_is_reported_separately() -> None:
    # Several agencies publish an index rather than a urlset. A parser that
    # only understands urlsets returns nothing and looks healthy doing it, so
    # the two shapes are kept apart and the caller is made to follow children.
    index = """<?xml version="1.0" encoding="UTF-8"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <sitemap><loc>https://mom.gov.sg/workplace-safety-and-health.xml</loc></sitemap>
    <sitemap><loc>https://mom.gov.sg/faq.xml</loc></sitemap>
    </sitemapindex>"""
    entries, nested = parse_sitemap(index)
    assert entries == []
    assert nested == [
        "https://mom.gov.sg/workplace-safety-and-health.xml",
        "https://mom.gov.sg/faq.xml",
    ]


def test_query_strings_are_stripped_from_record_identity() -> None:
    # Sitefinity/Sitecore re-stamp ?sfvrsn= whenever an asset is republished.
    # Left in the identity, the same document reappears as new every run.
    assert strip_query("https://x.test/docs/a.pdf?sfvrsn=6904a05f_1") == (
        "https://x.test/docs/a.pdf"
    )
    assert strip_query("https://x.test/-/media/b.pdf?la=en&hash=ABC#p2") == (
        "https://x.test/-/media/b.pdf"
    )
    assert strip_query("https://x.test/c.pdf") == "https://x.test/c.pdf"


def test_flight_payload_round_trips() -> None:
    payload = decode_flight_payload(COLLECTION_HTML)
    assert '"referenceLinkHref":"https://isomer-user-content' in payload
    assert "\\" not in payload.split('"formattedDate"')[0][-40:]


def test_collection_yields_every_item_not_just_the_rendered_page() -> None:
    items = extract_collection_items(COLLECTION_HTML)
    assert len(items) == 2
    first, second = items
    assert first.formatted_date == "3 August 2026"
    assert second.href.endswith("Circular B.pdf")
    title, hints = split_label(second.title)
    assert title == "MORE VIGILANCE REQUIRED IN ESCALATOR MAINTENANCE"
    assert hints["declared_file_size"] == "33 KB"


def test_collection_extraction_is_silent_on_ordinary_pages() -> None:
    assert extract_collection_items(PAGE_HTML) == []


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
    print(f"\n{failures} failure(s)")
    raise SystemExit(1 if failures else 0)
