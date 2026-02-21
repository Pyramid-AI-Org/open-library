from __future__ import annotations

import re
import random

import requests

from crawlers.base import (
    RunContext,
    UrlRecord,
    canonicalize_url,
    get_with_retries,
    infer_name_from_link,
    path_ext,
    sleep_seconds,
)
from utils.html_links import extract_links, extract_links_in_element


_ALLOWED_DOC_EXTS = {".pdf"}


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


_sleep_seconds = sleep_seconds
_get_with_retries = get_with_retries
_path_ext = path_ext
_infer_name = infer_name_from_link


_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", flags=re.DOTALL)


def _strip_html_comments(html: str) -> str:
    if not html:
        return ""
    return _HTML_COMMENT_RE.sub("", html)


def _extract_links_in_content(html: str, *, page_url: str, content_element_id: str):
    scoped = extract_links_in_element(
        html,
        base_url=page_url,
        element_id=content_element_id,
    )
    if scoped:
        return scoped
    return extract_links(html, base_url=page_url)


class Crawler:
    """EMSD Electricity Safety - New Edition of Code of Practice.

    Flow:
    - Fetch landing page.
    - Discover sub-pages under the same section.
    - For each sub-page, extract all unique PDF links.

    Emits PDFs only.

    Config: crawlers.electric_safety_cop
      - landing_url: landing page URL
      - subpage_urls: optional list of subpage URLs (overrides discovery)
      - content_element_id: element id to scope link extraction (default: content)
      - request_delay_seconds / request_jitter_seconds
      - max_total_records
      - backoff_base_seconds / backoff_jitter_seconds
    """

    name = "electric_safety_cop"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        landing_url = str(
            cfg.get(
                "landing_url",
                "https://www.emsd.gov.hk/en/electricity_safety/new_edition_cop/index.html",
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

        def _fetch_text(url: str) -> str:
            if request_delay_seconds > 0:
                _sleep_seconds(
                    request_delay_seconds
                    + random.uniform(0.0, max(0.0, request_jitter_seconds))
                )

            if ctx.debug:
                print(f"[{self.name}] Fetching {url}")

            resp = _get_with_retries(
                session,
                url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base_seconds,
                backoff_jitter_seconds=backoff_jitter_seconds,
            )
            return _strip_html_comments(resp.text or "")

        landing_html: str
        try:
            landing_html = _fetch_text(landing_url)
        except Exception as e:
            if ctx.debug:
                print(f"[{self.name}] Error fetching landing page: {e}")
            return []

        landing_can = _canonicalize(landing_url) or landing_url

        subpage_urls_cfg = cfg.get("subpage_urls")
        subpage_urls: list[str] = []
        if isinstance(subpage_urls_cfg, list) and subpage_urls_cfg:
            subpage_urls = [str(v).strip() for v in subpage_urls_cfg if str(v).strip()]
        else:
            # Discover sub-pages from the landing page. We intentionally keep the
            # HTML order for stable "first seen" semantics.
            links = _extract_links_in_content(
                landing_html,
                page_url=landing_url,
                content_element_id=content_element_id,
            )

            seen_subpages: set[str] = set()
            for link in links:
                can = _canonicalize(link.href)
                if not can:
                    continue

                if can == landing_can:
                    continue

                if _path_ext(can) in _ALLOWED_DOC_EXTS:
                    continue

                if "/en/electricity_safety/new_edition_cop/" not in can:
                    continue

                if can in seen_subpages:
                    continue
                seen_subpages.add(can)
                subpage_urls.append(can)

        out: list[UrlRecord] = []
        seen_pdf_urls: set[str] = set()

        for sub_url in subpage_urls:
            if len(out) >= max_total_records:
                break

            try:
                html = _fetch_text(sub_url)
            except Exception as e:
                if ctx.debug:
                    print(f"[{self.name}] Error fetching subpage {sub_url}: {e}")
                continue

            links = _extract_links_in_content(
                html,
                page_url=sub_url,
                content_element_id=content_element_id,
            )

            for link in links:
                can = _canonicalize(link.href)
                if not can:
                    continue

                if _path_ext(can) not in _ALLOWED_DOC_EXTS:
                    continue

                # Keep only the first discovery of a PDF across sub-pages.
                if can in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(can)

                out.append(
                    UrlRecord(
                        url=can,
                        name=_infer_name(link.text or "", can),
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={
                            "discovered_from": sub_url,
                            "file_ext": "pdf",
                        },
                    )
                )

                if len(out) >= max_total_records:
                    break

        out.sort(key=lambda r: (r.url or ""))
        return out
