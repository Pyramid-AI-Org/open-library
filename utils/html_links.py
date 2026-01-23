from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin


@dataclass(frozen=True)
class HtmlLink:
    href: str
    text: str


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_a = False
        self._current_href: str | None = None
        self._current_text_parts: list[str] = []
        self.links: list[HtmlLink] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return

        href = None
        for k, v in attrs:
            if k.lower() == "href" and v:
                href = v
                break

        self._in_a = True
        self._current_href = href
        self._current_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a":
            return

        if self._in_a and self._current_href:
            text = "".join(self._current_text_parts).strip()
            self.links.append(HtmlLink(href=self._current_href, text=text))

        self._in_a = False
        self._current_href = None
        self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_a:
            self._current_text_parts.append(data)


class _ScopedAnchorParser(HTMLParser):
    def __init__(self, *, element_id: str) -> None:
        super().__init__()
        self._target_id = element_id
        self._target_depth = 0

        self._in_a = False
        self._current_href: str | None = None
        self._current_text_parts: list[str] = []
        self.links: list[HtmlLink] = []

    def _attrs_to_dict(self, attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = self._attrs_to_dict(attrs)

        if self._target_depth == 0 and attrs_map.get("id") == self._target_id:
            self._target_depth = 1
        elif self._target_depth > 0:
            self._target_depth += 1

        if self._target_depth <= 0:
            return

        if tag.lower() != "a":
            return

        href = attrs_map.get("href")
        self._in_a = True
        self._current_href = href
        self._current_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        if self._target_depth > 0:
            self._target_depth -= 1
            if self._target_depth == 0:
                self._in_a = False
                self._current_href = None
                self._current_text_parts = []
                return

        if self._target_depth <= 0:
            return

        if tag.lower() != "a":
            return

        if self._in_a and self._current_href:
            text = "".join(self._current_text_parts).strip()
            self.links.append(HtmlLink(href=self._current_href, text=text))

        self._in_a = False
        self._current_href = None
        self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._target_depth > 0 and self._in_a:
            self._current_text_parts.append(data)


def extract_links(html: str, base_url: str) -> list[HtmlLink]:
    parser = _AnchorParser()
    parser.feed(html)

    normalized: list[HtmlLink] = []
    for link in parser.links:
        href = urljoin(base_url, link.href)
        normalized.append(HtmlLink(href=href, text=link.text))

    return normalized


def extract_links_in_element(
    html: str, *, base_url: str, element_id: str
) -> list[HtmlLink]:
    parser = _ScopedAnchorParser(element_id=element_id)
    parser.feed(html)

    normalized: list[HtmlLink] = []
    for link in parser.links:
        href = urljoin(base_url, link.href)
        normalized.append(HtmlLink(href=href, text=link.text))

    return normalized


def filter_links(
    links: Iterable[HtmlLink],
    *,
    text_contains: str | None = None,
    href_contains: str | None = None,
) -> list[HtmlLink]:
    out: list[HtmlLink] = []
    for l in links:
        if text_contains and text_contains.lower() not in (l.text or "").lower():
            continue
        if href_contains and href_contains.lower() not in (l.href or "").lower():
            continue
        out.append(l)
    return out
