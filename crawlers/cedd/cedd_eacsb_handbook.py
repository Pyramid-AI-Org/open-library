from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    clean_text,
    get_with_retries,
    path_ext,
)


_TARGET_TITLE_PREFIX = 'all "pdf" files of eacsb handbook revision no'


@dataclass(frozen=True)
class _Candidate:
    title: str | None
    href: str


class _EacsbParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.candidates: list[_Candidate] = []

        self._in_p = False
        self._p_depth = 0
        self._before_first_link_parts: list[str] = []

        self._in_a = False
        self._current_href: str | None = None
        self._current_link_classes: set[str] = set()

        self._in_accessibility_span = False

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in attrs:
            if v is None:
                continue
            out[k.lower()] = v
        return out

    @staticmethod
    def _class_set(value: str | None) -> set[str]:
        return {c.strip().lower() for c in (value or "").split() if c.strip()}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attrs_map = self._attrs_to_dict(attrs)

        if t == "p":
            if not self._in_p:
                self._in_p = True
                self._p_depth = 1
                self._before_first_link_parts = []
            else:
                self._p_depth += 1
            return

        if not self._in_p:
            return

        if t == "a":
            self._in_a = True
            self._current_href = attrs_map.get("href")
            self._current_link_classes = self._class_set(attrs_map.get("class"))
            return

        if t == "span" and self._in_a:
            classes = self._class_set(attrs_map.get("class"))
            if "accessibility" in classes:
                self._in_accessibility_span = True

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if not self._in_p:
            return

        if t == "span" and self._in_accessibility_span:
            self._in_accessibility_span = False
            return

        if t == "a" and self._in_a:
            href = (self._current_href or "").strip()
            is_pdf = path_ext(href) == ".pdf" or "pdf" in self._current_link_classes

            if href and is_pdf:
                title = clean_text("".join(self._before_first_link_parts)) or None
                normalized_title = (title or "").lower()
                if normalized_title.startswith(_TARGET_TITLE_PREFIX):
                    self.candidates.append(_Candidate(title=title, href=href))

            self._in_a = False
            self._current_href = None
            self._current_link_classes = set()
            self._in_accessibility_span = False
            return

        if t == "p":
            self._p_depth -= 1
            if self._p_depth <= 0:
                self._in_p = False
                self._p_depth = 0
                self._before_first_link_parts = []

    def handle_data(self, data: str) -> None:
        if not self._in_p:
            return
        if self._in_a or self._in_accessibility_span:
            return
        self._before_first_link_parts.append(data)


class Crawler:
    """Discover EACSB Handbook PDF from its landing page."""

    name = "cedd_eacsb_handbook"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        base_url = str(cfg.get("base_url", "https://www.cedd.gov.hk")).rstrip("/")
        page_url = str(
            cfg.get(
                "page_url",
                f"{base_url}/eng/publications/eacsb-handbook/index.html",
            )
        ).strip()
        max_total_records = int(cfg.get("max_total_records", 1))
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

        resp = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_jitter_seconds=backoff_jitter_seconds,
        )

        parser = _EacsbParser()
        parser.feed(resp.text)

        normalized: list[_Candidate] = []
        for c in parser.candidates:
            abs_url = canonicalize_url(urljoin(page_url, c.href), encode_spaces=True)
            if not abs_url:
                continue
            if not abs_url.startswith(base_url + "/"):
                continue
            if path_ext(abs_url) != ".pdf":
                continue
            normalized.append(_Candidate(title=c.title, href=abs_url))

        # Only keep the single handbook-level PDF row:
        # All "pdf" files of EACSB Handbook Revision No. X
        picked: list[_Candidate] = []
        for c in normalized:
            if (c.title or "").lower().startswith(_TARGET_TITLE_PREFIX):
                picked = [c]
                break

        out: list[UrlRecord] = []
        seen: set[str] = set()
        for c in picked:
            if c.href in seen:
                continue
            seen.add(c.href)

            out.append(
                UrlRecord(
                    url=c.href,
                    name=c.title,
                    discovered_at_utc=ctx.started_at_utc,
                    source=self.name,
                    meta={
                        "discovered_from": page_url,
                        "title": c.title,
                    },
                )
            )

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: r.url)
        return out
