from __future__ import annotations

import random
import re
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
    normalize_publish_date,
    path_ext,
    sleep_seconds,
)

_DEFAULT_PAGE_URL = "https://www.landsd.gov.hk/en/resources/practice-notes/laco.html"
_ALLOWED_EXTENSIONS = {".pdf", ".doc", ".docx"}
_TABLE_ID_PATTERN = re.compile(r"^laco_pn_\d+$", flags=re.IGNORECASE)


@dataclass(frozen=True)
class _RowData:
    section_name: str
    laco_cm_no: str
    download_href: str | None
    subject: str
    issue_date: str


class _LacoDesktopTableParser(HTMLParser):
    """Extract rows from LACO desktop tables only."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._in_target_table = False
        self._table_depth = 0
        self._active_section_name = ""

        self._in_mobile_container = False
        self._mobile_container_depth = 0
        self._in_mobile_table = False
        self._mobile_table_depth = 0

        self._in_mobile_row = False
        self._mobile_in_th = False
        self._mobile_in_td = False
        self._mobile_current_label = ""
        self._mobile_th_text: list[str] = []
        self._mobile_td_text: list[str] = []
        self._mobile_in_subject_anchor = False
        self._mobile_subject_anchor_text: list[str] = []

        self._mobile_cm_no = ""
        self._mobile_download_href: str | None = None
        self._mobile_subject = ""
        self._mobile_issue_date = ""

        self._in_heading = False
        self._current_heading_text: list[str] = []
        self._last_heading_text = ""

        self._in_row = False
        self._in_td = False
        self._td_index = 0
        self._current_td_text: list[str] = []
        self._in_subject_anchor = False
        self._subject_anchor_text: list[str] = []

        self._row_cm_no = ""
        self._row_download_href: str | None = None
        self._row_subject = ""
        self._row_issue_date = ""
        self._row_section_name = ""

        self.rows: list[_RowData] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = dict(attrs)

        if tag == "div":
            div_id = clean_text(str(attrs_map.get("id") or ""))
            if not self._in_mobile_container and div_id.startswith("laco_mobile_"):
                self._in_mobile_container = True
                self._mobile_container_depth = 1
            elif self._in_mobile_container:
                self._mobile_container_depth += 1

        if tag == "table" and self._in_mobile_container and not self._in_target_table:
            if not self._in_mobile_table:
                self._in_mobile_table = True
                self._mobile_table_depth = 1
                self._active_section_name = self._last_heading_text
                self._mobile_cm_no = ""
                self._mobile_download_href = None
                self._mobile_subject = ""
                self._mobile_issue_date = ""
                return

            self._mobile_table_depth += 1
            return

        if self._in_mobile_table:
            if tag == "tr":
                self._in_mobile_row = True
                self._mobile_in_th = False
                self._mobile_in_td = False
                self._mobile_current_label = ""
                self._mobile_th_text = []
                self._mobile_td_text = []
                self._mobile_in_subject_anchor = False
                self._mobile_subject_anchor_text = []
                return

            if not self._in_mobile_row:
                return

            if tag == "th":
                self._mobile_in_th = True
                self._mobile_th_text = []
                return

            if tag == "td":
                self._mobile_in_td = True
                self._mobile_td_text = []
                self._mobile_in_subject_anchor = False
                self._mobile_subject_anchor_text = []
                return

            if tag == "a" and self._mobile_in_td:
                href = clean_text(str(attrs_map.get("href") or ""))
                label = self._mobile_current_label.lower()
                if (
                    "click button to download" in label
                    and self._mobile_download_href is None
                    and href
                ):
                    self._mobile_download_href = href

                if "subject" in label and not self._mobile_subject:
                    self._mobile_in_subject_anchor = True
                    self._mobile_subject_anchor_text = []
            return

        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._in_heading = True
            self._current_heading_text = []
            return

        if tag == "table":
            table_id = clean_text(str(attrs_map.get("id") or ""))
            is_target = (
                bool(_TABLE_ID_PATTERN.fullmatch(table_id)) or table_id == "laco_list"
            )

            if not self._in_target_table and is_target:
                self._in_target_table = True
                self._table_depth = 1
                self._active_section_name = self._last_heading_text
                return

            if self._in_target_table:
                self._table_depth += 1
                return

        if not self._in_target_table:
            return

        if tag == "tr":
            self._in_row = True
            self._in_td = False
            self._td_index = 0
            self._current_td_text = []
            self._in_subject_anchor = False
            self._subject_anchor_text = []

            self._row_cm_no = ""
            self._row_download_href = None
            self._row_subject = ""
            self._row_issue_date = ""
            self._row_section_name = self._active_section_name
            return

        if not self._in_row:
            return

        if tag == "td":
            self._in_td = True
            self._td_index += 1
            self._current_td_text = []
            self._in_subject_anchor = False
            self._subject_anchor_text = []
            return

        if (
            tag == "a"
            and self._in_td
            and self._td_index == 2
            and self._row_download_href is None
        ):
            href = clean_text(str(attrs_map.get("href") or ""))
            if href:
                self._row_download_href = href

        if tag == "a" and self._in_td and self._td_index == 3 and not self._row_subject:
            self._in_subject_anchor = True
            self._subject_anchor_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self._in_mobile_container:
            self._mobile_container_depth -= 1
            if self._mobile_container_depth <= 0:
                self._in_mobile_container = False
                self._mobile_container_depth = 0
            return

        if self._in_mobile_table:
            if tag == "a" and self._mobile_in_subject_anchor:
                candidate = clean_text("".join(self._mobile_subject_anchor_text))
                if candidate and not self._mobile_subject:
                    self._mobile_subject = candidate
                self._mobile_in_subject_anchor = False
                self._mobile_subject_anchor_text = []
                return

            if tag == "th" and self._mobile_in_th:
                self._mobile_current_label = clean_text("".join(self._mobile_th_text))
                self._mobile_in_th = False
                self._mobile_th_text = []
                return

            if tag == "td" and self._mobile_in_td:
                label = self._mobile_current_label.lower()
                value_text = clean_text("".join(self._mobile_td_text))

                if "laco cm no" in label:
                    self._mobile_cm_no = value_text
                elif "subject" in label:
                    self._mobile_subject = self._mobile_subject or value_text
                elif "date of issue" in label:
                    self._mobile_issue_date = value_text

                self._mobile_in_td = False
                self._mobile_td_text = []
                self._mobile_in_subject_anchor = False
                self._mobile_subject_anchor_text = []
                return

            if tag == "tr" and self._in_mobile_row:
                self._in_mobile_row = False
                self._mobile_in_th = False
                self._mobile_in_td = False
                self._mobile_current_label = ""
                self._mobile_th_text = []
                self._mobile_td_text = []
                self._mobile_in_subject_anchor = False
                self._mobile_subject_anchor_text = []
                return

            if tag == "table":
                self._mobile_table_depth -= 1
                if self._mobile_table_depth <= 0:
                    cm_no = clean_text(self._mobile_cm_no)
                    subject = clean_text(self._mobile_subject)
                    issue_date = clean_text(self._mobile_issue_date)
                    section_name = clean_text(self._active_section_name)
                    if cm_no and subject and self._mobile_download_href:
                        self.rows.append(
                            _RowData(
                                section_name=section_name,
                                laco_cm_no=cm_no,
                                download_href=self._mobile_download_href,
                                subject=subject,
                                issue_date=issue_date,
                            )
                        )

                    self._in_mobile_table = False
                    self._mobile_table_depth = 0
                    self._in_mobile_row = False
                    self._mobile_in_th = False
                    self._mobile_in_td = False
                    self._mobile_current_label = ""
                    self._mobile_th_text = []
                    self._mobile_td_text = []
                    self._mobile_in_subject_anchor = False
                    self._mobile_subject_anchor_text = []
                    self._mobile_cm_no = ""
                    self._mobile_download_href = None
                    self._mobile_subject = ""
                    self._mobile_issue_date = ""
                    self._active_section_name = ""
                return

        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"} and self._in_heading:
            heading_text = clean_text("".join(self._current_heading_text))
            if heading_text:
                self._last_heading_text = heading_text
            self._in_heading = False
            self._current_heading_text = []
            return

        if tag == "table" and self._in_target_table:
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_target_table = False
                self._table_depth = 0
                self._active_section_name = ""
            return

        if not self._in_target_table:
            return

        if tag == "a" and self._in_subject_anchor:
            candidate = clean_text("".join(self._subject_anchor_text))
            if candidate and not self._row_subject:
                self._row_subject = candidate
            self._in_subject_anchor = False
            self._subject_anchor_text = []
            return

        if tag == "td" and self._in_td:
            text = clean_text("".join(self._current_td_text))
            if self._td_index == 1:
                self._row_cm_no = text
            elif self._td_index == 3:
                if not self._row_subject:
                    self._row_subject = text
            elif self._td_index == 4:
                self._row_issue_date = text

            self._in_td = False
            self._current_td_text = []
            return

        if tag == "tr" and self._in_row:
            cm_no = clean_text(self._row_cm_no)
            subject = clean_text(self._row_subject)
            issue_date = clean_text(self._row_issue_date)
            section_name = clean_text(self._row_section_name)

            if cm_no and subject and self._row_download_href:
                self.rows.append(
                    _RowData(
                        section_name=section_name,
                        laco_cm_no=cm_no,
                        download_href=self._row_download_href,
                        subject=subject,
                        issue_date=issue_date,
                    )
                )

            self._in_row = False
            self._in_td = False
            self._td_index = 0
            self._current_td_text = []
            self._in_subject_anchor = False
            self._subject_anchor_text = []

            self._row_cm_no = ""
            self._row_download_href = None
            self._row_subject = ""
            self._row_issue_date = ""
            self._row_section_name = ""

    def handle_data(self, data: str) -> None:
        if self._in_heading:
            self._current_heading_text.append(data)

        if self._in_mobile_table:
            if self._mobile_in_th:
                self._mobile_th_text.append(data)
            if self._mobile_in_td:
                self._mobile_td_text.append(data)
                if self._mobile_in_subject_anchor:
                    self._mobile_subject_anchor_text.append(data)

        if self._in_row and self._in_td:
            self._current_td_text.append(data)
            if self._td_index == 3 and self._in_subject_anchor:
                self._subject_anchor_text.append(data)


def _canonicalize(url: str) -> str | None:
    return canonicalize_url(url, encode_spaces=True)


class Crawler:
    name = "laco_circular_memorandum"

    def crawl(self, ctx: RunContext) -> list[UrlRecord]:
        cfg = ctx.get_crawler_config(self.name)

        page_url = clean_text(str(cfg.get("page_url") or _DEFAULT_PAGE_URL))

        request_delay = float(cfg.get("request_delay_seconds", 0.25))
        request_jitter = float(cfg.get("request_jitter_seconds", 0.10))
        backoff_base = float(cfg.get("backoff_base_seconds", 0.5))
        backoff_jitter = float(cfg.get("backoff_jitter_seconds", 0.25))
        max_total_records = int(cfg.get("max_total_records", 50000))

        http_cfg = ctx.get_http_config()
        timeout_seconds = int(http_cfg.get("timeout_seconds", 30))
        user_agent = str(http_cfg.get("user_agent", "")).strip()
        max_retries = int(http_cfg.get("max_retries", 3))

        session = requests.Session()
        if user_agent:
            session.headers.update({"User-Agent": user_agent})

        if request_delay > 0:
            sleep_seconds(request_delay + random.uniform(0.0, max(0.0, request_jitter)))

        resp = get_with_retries(
            session,
            page_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
            backoff_jitter_seconds=backoff_jitter,
        )
        resp.encoding = "utf-8"

        parser = _LacoDesktopTableParser()
        parser.feed(resp.text or "")

        out: list[UrlRecord] = []

        for row in parser.rows:
            if len(out) >= max_total_records:
                break

            raw_href = clean_text(str(row.download_href or ""))
            if not raw_href:
                continue

            candidate_url = _canonicalize(urljoin(page_url, raw_href))
            if not candidate_url:
                continue
            if path_ext(candidate_url) not in _ALLOWED_EXTENSIONS:
                continue

            publish_date = normalize_publish_date(row.issue_date)

            out.append(
                ctx.make_record(
                    url=candidate_url,
                    name=row.subject,
                    discovered_at_utc=ctx.run_date_utc,
                    source=self.name,
                    publish_date=publish_date,
                    meta={
                        "discovered_from": page_url,
                        "section_name": row.section_name,
                        "ref_no": row.laco_cm_no,
                    },
                )
            )

        out.sort(
            key=lambda r: (
                str(r.meta.get("section_name") or ""),
                str(r.meta.get("ref_no") or ""),
                r.url,
                str(r.name or ""),
                str(r.publish_date or ""),
                str(r.meta.get("discovered_from") or ""),
            )
        )
        return out
