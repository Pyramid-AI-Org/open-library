from __future__ import annotations

import random

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
from utils.html_links import extract_links, extract_links_in_element


_ALLOWED_DOC_EXTS = {".pdf"}


def _canonicalize_url(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


_clean_text = clean_text
_sleep_seconds = sleep_seconds
_get_with_retries = get_with_retries
_path_ext = path_ext
_infer_name = infer_name_from_link


class Crawler:
    """BD Basic Pages crawler.

    Handles miscellaneous single pages where we want to:
    1. Emit the page itself (HTML).
    2. Emit all PDF links found on the page.

    Configuration is list-driven via `targets`.
    """

    name = "bd_basic_pages"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        targets = cfg.get("targets", [])
        if not targets:
            return []

        # Common settings
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

        out: list[UrlRecord] = []
        seen_urls: set[str] = set()

        for target in targets:
            page_url = str(target.get("url", "")).strip()
            if not page_url:
                continue

            page_name_label = str(target.get("title", "Index Page")).strip()
            # If not provided, we won't infer from HTML <title> to keep it simple,
            # but rely on config.

            content_element_id = str(
                target.get("content_element_id", "content")
            ).strip()

            if request_delay_seconds > 0:
                _sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
                )

            try:
                resp = _get_with_retries(
                    session,
                    page_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                    backoff_jitter_seconds=backoff_jitter_seconds,
                )
            except Exception as e:
                # Log error in real app?
                if ctx.debug:
                    print(f"[{self.name}] Error fetching {page_url}: {e}")
                continue

            # 1. Emit the page itself
            can_page = _canonicalize_url(page_url)
            if can_page and can_page not in seen_urls:
                seen_urls.add(can_page)
                out.append(
                    UrlRecord(
                        url=can_page,
                        name=page_name_label,
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={
                            "is_index_page": True,
                            "file_ext": "html",
                        },
                    )
                )

            # 2. Extract PDF links
            links = extract_links_in_element(
                resp.text or "",
                base_url=page_url,
                element_id=content_element_id,
            )
            if not links:
                links = extract_links(resp.text or "", base_url=page_url)

            for link in links:
                can = _canonicalize_url(link.href)
                if not can:
                    continue

                if _path_ext(can) not in _ALLOWED_DOC_EXTS:
                    continue

                if can in seen_urls:
                    continue
                seen_urls.add(can)

                name = _infer_name(link.text or "", can)

                out.append(
                    UrlRecord(
                        url=can,
                        name=name,
                        discovered_at_utc=ctx.started_at_utc,
                        source=self.name,
                        meta={
                            "from_page_url": page_url,
                            "file_ext": "pdf",
                        },
                    )
                )

                if len(out) >= max_total_records:
                    break

            if len(out) >= max_total_records:
                break

        out.sort(key=lambda r: (r.url or ""))
        return out
