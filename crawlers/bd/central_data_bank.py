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
    """BD Central Data Bank crawler.

    Collects all PDF links from the 4 CDB sub-pages and the ADM-20 PDF link
    from the CDB landing page.
    """

    name = "central_data_bank"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.settings.get("crawlers", {}).get(self.name, {})

        main_page_url = str(
            cfg.get(
                "main_page_url",
                "https://www.bd.gov.hk/en/resources/codes-and-references/central-data-bank/CDB.html",
            )
        ).strip()
        content_element_id = (
            str(cfg.get("content_element_id", "content")).strip() or "content"
        )

        subpage_urls_cfg = cfg.get("subpage_urls")
        subpage_urls: list[str]
        if isinstance(subpage_urls_cfg, list) and subpage_urls_cfg:
            subpage_urls = [str(v).strip() for v in subpage_urls_cfg if str(v).strip()]
        else:
            subpage_urls = [
                "https://www.bd.gov.hk/en/resources/codes-and-references/central-data-bank/CDBBuildMaterial.html",
                "https://www.bd.gov.hk/en/resources/codes-and-references/central-data-bank/CDBBuildComp.html",
                "https://www.bd.gov.hk/en/resources/codes-and-references/central-data-bank/CDBConstructSys.html",
                "https://www.bd.gov.hk/en/resources/codes-and-references/central-data-bank/CDBFSImprovementWorks.html",
            ]

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

        target_pages: list[tuple[str, str]] = [("CDB Main", main_page_url)]
        target_pages.extend(("CDB Subpage", u) for u in subpage_urls)

        for page_kind, page_url in target_pages:
            if request_delay_seconds > 0:
                _sleep_seconds(
                    request_delay_seconds + random.uniform(0.0, request_jitter_seconds)
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
                    print(f"[{self.name}] Error fetching {page_url}: {e}")
                continue

            links = _extract_links_in_content(
                resp.text or "",
                page_url=page_url,
                content_element_id=content_element_id,
            )

            for link in links:
                can = _canonicalize_url(link.href)
                if not can:
                    continue
                if _path_ext(can) not in _ALLOWED_DOC_EXTS:
                    continue

                # On the main page keep only ADM-20 (user requested explicitly).
                if page_kind == "CDB Main":
                    txt = _clean_text(link.text or "")
                    if "adm-20" not in txt.lower() and "adm020.pdf" not in can.lower():
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
                            "page_url": page_url,
                            "page_type": page_kind,
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
