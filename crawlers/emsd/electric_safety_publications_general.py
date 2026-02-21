from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import random
import re
from urllib.parse import urljoin

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    infer_name_from_link,
    path_ext,
    sleep_seconds,
)


_ALLOWED_DOC_EXTS = {".pdf"}
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", flags=re.DOTALL)


@dataclass(frozen=True)
class _Anchor:
    href: str
    text: str
    lang: str


class _ScopedAnchorParser(HTMLParser):
    def __init__(self, *, element_id: str | None) -> None:
        super().__init__()
        self._target_id = element_id
        # If no target ID, start at depth 1 (active)
        self._target_depth = 1 if not element_id else 0

        self._in_a = False
        self._current_href: str | None = None
        self._current_lang: str = ""
        self._current_text_parts: list[str] = []
        self.links: list[_Anchor] = []

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

        self._in_a = True
        self._current_href = attrs_map.get("href")
        self._current_lang = attrs_map.get("lang", "")
        self._current_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        if self._target_depth > 0:
            self._target_depth -= 1
            if self._target_depth == 0:
                self._in_a = False
                self._current_href = None
                self._current_lang = ""
                self._current_text_parts = []
                return

        if self._target_depth <= 0:
            return

        if tag.lower() != "a":
            return

        if self._in_a and self._current_href:
            self.links.append(
                _Anchor(
                    href=self._current_href,
                    text="".join(self._current_text_parts).strip(),
                    lang=self._current_lang,
                )
            )

        self._in_a = False
        self._current_href = None
        self._current_lang = ""
        self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._target_depth > 0 and self._in_a:
            self._current_text_parts.append(data)


def _strip_html_comments(html: str) -> str:
    if not html:
        return ""
    return _HTML_COMMENT_RE.sub("", html)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


def _is_english_link(lang_attr: str) -> bool:
    s = clean_text(lang_attr)
    if not s:
        # Most English links on this page have no lang attribute.
        return True

    tokens: list[str] = []
    for chunk in s.split(","):
        tok = chunk.strip().lower()
        if tok:
            tokens.append(tok)

    if not tokens:
        return True

    return any(tok == "en" or tok.startswith("en-") for tok in tokens)


_sleep_seconds = sleep_seconds
_get_with_retries = get_with_retries
_path_ext = path_ext
_infer_name = infer_name_from_link


class Crawler:
    """EMSD Electricity Safety publications (general) crawler.

    Emits unique English PDF links from the configured page.
    """

    name = "electric_safety_publications_general"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        page_url = str(
            cfg.get(
                "page_url",
                "https://www.emsd.gov.hk/en/electricity_safety/publications/general/index.html#eps",
            )
        ).strip()
        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )

        request_delay_seconds = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter_seconds = float(cfg.get("request_jitter_seconds", 0.10))
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

        if request_delay_seconds > 0:
            _sleep_seconds(
                request_delay_seconds + random.uniform(0.0, max(0.0, request_jitter_seconds))
            )

        try:
            if ctx.debug:
                print(f"[{self.name}] Fetching {page_url}")

            resp = _get_with_retries(
                session,
                page_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base_seconds,
                backoff_jitter_seconds=backoff_jitter_seconds,
            )
        except Exception as e:
            if ctx.debug:
                print(f"[{self.name}] Error fetching page: {e}")
            return []

        parser = _ScopedAnchorParser(element_id=content_element_id)
        parser.feed(_strip_html_comments(resp.text or ""))

        links = parser.links
        if not links:
            # Fallback to all links if content element not found
            parser = _ScopedAnchorParser(element_id="")
            parser.feed(_strip_html_comments(resp.text or ""))
            links = parser.links

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for link in links:
            if not _is_english_link(link.lang):
                continue

            can = _canonicalize(urljoin(page_url, link.href))
            if not can:
                continue

            if _path_ext(can) not in _ALLOWED_DOC_EXTS:
                continue

            if can in seen_urls:
                continue
            seen_urls.add(can)

            out.append(
                UrlRecord(
                    url=can,
                    name=_infer_name(link.text or "", can),
                    discovered_at_utc=ctx.started_at_utc,
                    source=self.name,
                    meta={
                        "discovered_from": page_url,
                        "file_ext": "pdf",
                    },
                )
            )

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url or ""))
        return out
