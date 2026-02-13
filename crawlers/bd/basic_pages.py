from __future__ import annotations

import random
import time
from urllib.parse import unquote, urlparse, urlunparse

import requests

from crawlers.base import RunContext, UrlRecord
from utils.html_links import extract_links, extract_links_in_element


_ALLOWED_DOC_EXTS = {".pdf"}


def _clean_text(value: str) -> str:
    return " ".join((value or "").strip().split())


def _sleep_seconds(seconds: float) -> None:
    if seconds <= 0:
        return
    time.sleep(seconds)


def _compute_backoff_seconds(attempt: int, *, base: float, jitter: float) -> float:
    exp = base * (2**attempt)
    exp = min(exp, 30.0)
    if jitter > 0:
        exp += random.uniform(0.0, jitter)
    return exp


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    backoff_jitter_seconds: float,
) -> requests.Response:
    last_err: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout_seconds)
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    resp.raise_for_status()

                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        _sleep_seconds(float(retry_after))
                    except ValueError:
                        pass

                _sleep_seconds(
                    _compute_backoff_seconds(
                        attempt,
                        base=backoff_base_seconds,
                        jitter=backoff_jitter_seconds,
                    )
                )
                continue

            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_err = exc
            if attempt >= max_retries:
                raise

            _sleep_seconds(
                _compute_backoff_seconds(
                    attempt,
                    base=backoff_base_seconds,
                    jitter=backoff_jitter_seconds,
                )
            )

    assert last_err is not None
    raise last_err


def _canonicalize_url(url: str) -> str | None:
    s = (url or "").strip()
    if not s:
        return None

    # Keep produced URLs usable if source href has spaces.
    s = s.replace(" ", "%20")

    lower = s.lower()
    if lower.startswith("javascript:"):
        return None
    if lower.startswith("mailto:"):
        return None
    if lower.startswith("tel:"):
        return None

    parsed = urlparse(s)
    if not parsed.scheme or not parsed.netloc:
        return None

    parsed = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment="",
    )

    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    parsed = parsed._replace(path=path)

    return urlunparse(parsed)


def _path_ext(url: str) -> str:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if "." not in path:
        return ""
    return "." + path.rsplit(".", 1)[-1]


def _infer_name(link_text: str, url: str) -> str | None:
    text = _clean_text(link_text)
    if text:
        return text

    parsed = urlparse(url)
    tail = (parsed.path or "").rstrip("/").rsplit("/", 1)[-1]
    if not tail:
        return None
    tail = unquote(tail)
    if "." in tail:
        tail = tail.rsplit(".", 1)[0]
    tail = tail.replace("_", " ").replace("-", " ")
    tail = _clean_text(tail)
    return tail or None


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
