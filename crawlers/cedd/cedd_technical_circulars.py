from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests

from crawlers.base import RunContext, UrlRecord, clean_text, get_with_retries, sleep_seconds


_clean_text = clean_text
_sleep_seconds = sleep_seconds
_get_with_retries = get_with_retries


def _is_pdf_url(url: str) -> bool:
    path = (urlparse(url).path or "").lower()
    return path.endswith(".pdf")


@dataclass(frozen=True)
class _Row:
    circular_no: str | None
    title: str | None
    href: str | None


class _CeddTechnicalCircularsParser(HTMLParser):
    """Parse CEDD technical circulars table.

    Expected table shape:
      Circular No. | Title
    """

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[_Row] = []

        self._in_table = False
        self._table_depth = 0

        self._in_tr = False
        self._in_th = False
        self._td_index = -1

        self._current_circular_parts: list[str] = []
        self._current_title_parts: list[str] = []
        self._current_href: str | None = None

        self._in_a = False
        self._accessibility_depth = 0

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    @staticmethod
    def _class_set(attrs_map: dict[str, str]) -> set[str]:
        return {c.strip() for c in attrs_map.get("class", "").split() if c.strip()}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if not self._in_table and t == "table":
            classes = self._class_set(attrs_map)
            if "colortable" in {c.lower() for c in classes} and "tendertbl" in {
                c.lower() for c in classes
            }:
                self._in_table = True
                self._table_depth = 1
                return

        if self._in_table:
            self._table_depth += 1
        else:
            return

        if t == "tr":
            self._in_tr = True
            self._in_th = False
            self._td_index = -1
            self._current_circular_parts = []
            self._current_title_parts = []
            self._current_href = None
            self._in_a = False
            self._accessibility_depth = 0
            return

        if not self._in_tr:
            return

        if t == "th":
            self._in_th = True
            return

        if t == "td":
            self._td_index += 1
            return

        if t == "a" and not self._in_th and self._td_index == 1:
            self._in_a = True
            if self._current_href is None:
                href = attrs_map.get("href")
                if href:
                    self._current_href = href
            return

        if t == "span" and self._in_a:
            classes = self._class_set(attrs_map)
            if "accessibility" in {c.lower() for c in classes}:
                self._accessibility_depth += 1

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_table:
            return

        self._table_depth -= 1
        if self._table_depth == 0:
            self._in_table = False
            self._in_tr = False
            self._in_th = False
            self._in_a = False
            self._accessibility_depth = 0
            return

        if not self._in_tr:
            return

        if t == "span" and self._in_a and self._accessibility_depth > 0:
            self._accessibility_depth -= 1
            return

        if t == "a" and self._in_a:
            self._in_a = False
            self._accessibility_depth = 0
            return

        if t == "th":
            self._in_th = False
            return

        if t == "tr":
            self._in_tr = False
            if self._current_href:
                circular_no = _clean_text("".join(self._current_circular_parts)) or None
                title = _clean_text("".join(self._current_title_parts)) or None
                self.rows.append(
                    _Row(
                        circular_no=circular_no,
                        title=title,
                        href=self._current_href,
                    )
                )

            self._td_index = -1
            self._in_a = False
            self._accessibility_depth = 0

    def handle_data(self, data: str) -> None:
        if not self._in_table or not self._in_tr or self._in_th:
            return

        if self._td_index == 0:
            self._current_circular_parts.append(data)
            return

        if self._td_index == 1 and self._in_a and self._accessibility_depth == 0:
            self._current_title_parts.append(data)


class Crawler:
    """Crawl CEDD Technical Circulars listing page and emit PDF records."""

    name = "cedd_technical_circulars"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.cedd.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get(
                "page_url",
                f"{base_url}/eng/publications/technical-circulars/index.html",
            )
        ).strip()

        max_total_records = int(cfg.get("max_total_records", 50000))
        backoff_base_seconds = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter_seconds = float(cfg.get("backoff_jitter_seconds", 0.25))

        http_cfg = ctx.settings.get("http", {})
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if ctx.debug:
            print(f"[{self.name}] Fetch -> {page_url}")

        resp = _get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )

        parser = _CeddTechnicalCircularsParser()
        parser.feed(resp.text)

        out: list[UrlRecord] = []
        seen: set[str] = set()

        for row in parser.rows:
            if not row.href:
                continue

            abs_url = urljoin(page_url, row.href)
            if not abs_url.startswith(base_url + "/"):
                continue
            if not _is_pdf_url(abs_url):
                continue
            if abs_url in seen:
                continue
            seen.add(abs_url)

            name_parts: list[str] = []
            if row.circular_no:
                name_parts.append(row.circular_no)
            if row.title:
                name_parts.append(row.title)

            out.append(
                UrlRecord(
                    url=abs_url,
                    name=" - ".join(name_parts) or None,
                    discovered_at_utc=ctx.started_at_utc,
                    source=self.name,
                    meta={
                        "circular_no": row.circular_no,
                        "title": row.title,
                        "discovered_from": page_url,
                    },
                )
            )

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url, (r.meta.get("circular_no") or "")))
        return out
