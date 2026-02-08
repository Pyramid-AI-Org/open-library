from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse


CONSTRUCTION_SITE_SAFETY_MANUAL_PREFIX = (
	"/en/publications_and_press_releases/publications/construction_site_safety_manual/"
)


_ALLOWED_DOC_EXTS = {".pdf", ".doc", ".docx"}


def _normalize_ws(value: str | None) -> str | None:
	s = " ".join((value or "").split()).strip()
	return s or None


def _strip_query_fragment(url: str) -> str:
	s = url or ""
	for sep in ("#", "?"):
		if sep in s:
			s = s.split(sep, 1)[0]
	return s


def _doc_ext(url: str) -> str:
	path = urlparse(_strip_query_fragment(url)).path.lower()
	if "." not in path:
		return ""
	return "." + path.rsplit(".", 1)[-1]


def _is_allowed_doc_url(url: str) -> bool:
	return _doc_ext(url) in _ALLOWED_DOC_EXTS


def _looks_like_html_page(url: str) -> bool:
	u = (url or "").lower()
	if u.endswith("/"):
		return True
	if u.endswith(".html") or u.endswith(".htm") or u.endswith(".php"):
		return True
	if "?" in u:
		return True
	return False


def _filter_allowed_docs(hrefs: list[str]) -> list[str]:
	return [h for h in hrefs if _is_allowed_doc_url(h)]


@dataclass(frozen=True)
class ConstructionSiteSafetyManualHit:
	url: str
	title: str | None
	issue_date_raw: str | None
	meta: dict[str, str]


@dataclass(frozen=True)
class _Cell:
	text: str | None
	hrefs: tuple[str, ...]
	colspan: int


class _ArticleListTableParser(HTMLParser):
	def __init__(self, *, base_url: str, element_id: str) -> None:
		super().__init__()
		self._base_url = base_url
		self._element_id = element_id

		self.doc_hits: list[ConstructionSiteSafetyManualHit] = []
		self.page_links: set[str] = set()

		# If element_id is falsy, parse the entire HTML document.
		self._target_depth = 1 if not self._element_id else 0

		self._in_table = False
		self._table_depth = 0

		self._in_tr = False
		self._in_th = False
		self._in_td = False

		self._current_row_cells: list[_Cell] = []

		self._current_text_parts: list[str] = []
		self._current_hrefs: list[str] = []
		self._current_colspan = 1

	def _attrs_to_dict(self, attrs: list[tuple[str, str | None]]) -> dict[str, str]:
		out: dict[str, str] = {}
		for k, v in attrs:
			if v is None:
				continue
			out[k.lower()] = v
		return out

	def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
		t = tag.lower()
		attrs_map = self._attrs_to_dict(attrs)

		if (
			self._target_depth == 0
			and self._element_id
			and attrs_map.get("id") == self._element_id
		):
			self._target_depth = 1
		elif self._target_depth > 0:
			self._target_depth += 1

		if self._target_depth <= 0:
			return

		if not self._in_table and t == "table":
			cls = attrs_map.get("class", "")
			classes = set(cls.split())
			if "articlelistpage" in classes:
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
			self._current_row_cells = []
			return

		if not self._in_tr:
			return

		if t == "th":
			self._in_th = True
			return

		if t == "td":
			self._in_td = True
			self._current_text_parts = []
			self._current_hrefs = []
			try:
				self._current_colspan = int(attrs_map.get("colspan", "1") or "1")
			except ValueError:
				self._current_colspan = 1
			return

		if t == "a" and self._in_td:
			href = attrs_map.get("href")
			if href:
				self._current_hrefs.append(urljoin(self._base_url, href))
			return

		if t == "br" and self._in_td:
			self._current_text_parts.append(" ")

	def handle_endtag(self, tag: str) -> None:
		t = tag.lower()

		if self._target_depth > 0:
			self._target_depth -= 1
			if self._target_depth == 0:
				self._reset_all()
				return

		if not self._in_table:
			return

		self._table_depth -= 1
		if self._table_depth == 0:
			self._reset_all()
			return

		if t == "th":
			self._in_th = False
			return

		if not self._in_tr:
			return

		if t == "td" and not self._in_th:
			self._in_td = False
			text = _normalize_ws("".join(self._current_text_parts))
			hrefs = tuple(self._current_hrefs)
			colspan = self._current_colspan if self._current_colspan > 0 else 1

			self._current_row_cells.append(_Cell(text=text, hrefs=hrefs, colspan=colspan))

			self._current_text_parts = []
			self._current_hrefs = []
			self._current_colspan = 1
			return

		if t == "tr":
			self._in_tr = False
			if not self._current_row_cells:
				return
			self._process_row(self._current_row_cells)
			self._current_row_cells = []

	def _reset_all(self) -> None:
		self._in_table = False
		self._table_depth = 0
		self._in_tr = False
		self._in_th = False
		self._in_td = False
		self._current_row_cells = []

	def handle_data(self, data: str) -> None:
		if self._in_td and self._target_depth > 0:
			self._current_text_parts.append(data)

	def _emit_docs(
		self,
		*,
		title: str | None,
		hrefs: list[str],
		issue_date_raw: str | None,
		meta: dict[str, str],
	) -> None:
		if not title:
			return
		for h in _filter_allowed_docs(hrefs):
			self.doc_hits.append(
				ConstructionSiteSafetyManualHit(
					url=h,
					title=title,
					issue_date_raw=issue_date_raw,
					meta=meta,
				)
			)

	def _process_row(self, cells: list[_Cell]) -> None:
		# Collect in-scope page links (if any).
		for c in cells:
			for href in c.hrefs:
				if (
					CONSTRUCTION_SITE_SAFETY_MANUAL_PREFIX in href
					and _looks_like_html_page(href)
					and not _is_allowed_doc_url(href)
				):
					self.page_links.add(href)

		# Manual main table:
		# - title | (clean/current links) | highlighted links | revision/date
		# Ignore highlighted column entirely.
		if len(cells) >= 4:
			title = cells[0].text
			clean_hrefs = list(cells[1].hrefs)
			issue_date_raw = cells[-1].text
			self._emit_docs(
				title=title,
				hrefs=clean_hrefs,
				issue_date_raw=issue_date_raw,
				meta={"kind": "manual", "variant": "clean"},
			)
			return

		# Manual contents rows: title | current issue cell (often colspan=3)
		if len(cells) == 2 and cells[1].colspan >= 2:
			title = cells[0].text
			self._emit_docs(
				title=title,
				hrefs=list(cells[1].hrefs),
				issue_date_raw=None,
				meta={"kind": "manual", "variant": "current"},
			)
			return

		# Two-column tables:
		# - title (often contains the document link) | issue date
		if len(cells) == 2:
			title = cells[0].text
			issue_date_raw = cells[1].text

			hrefs = list(cells[0].hrefs)
			if not hrefs:
				hrefs = list(cells[1].hrefs)

			self._emit_docs(
				title=title,
				hrefs=hrefs,
				issue_date_raw=issue_date_raw,
				meta={"kind": "table", "variant": "single"},
			)
			return


def parse_construction_site_safety_manual_page(
	html: str, *, base_url: str, content_element_id: str = "content"
) -> tuple[list[ConstructionSiteSafetyManualHit], list[str]]:
	"""Parse DEVb Construction Site Safety Manual pages.

	Rules:
	  - Title comes from the left column (Manual Contents / Chapter / circular title).
	  - Ignore the "Highlighted" / track-change column.
	  - Capture raw issue date / revision string when present.
	  - Filters to .pdf/.doc/.docx only.
	  - Does not canonicalize or dedupe; caller should do that.
	"""

	parser = _ArticleListTableParser(base_url=base_url, element_id=content_element_id)
	parser.feed(html or "")

	if not parser.doc_hits and not parser.page_links:
		# Fallback for unexpected layouts / local fixtures.
		parser = _ArticleListTableParser(base_url=base_url, element_id="")
		parser.feed(html or "")

	docs: list[ConstructionSiteSafetyManualHit] = []
	for h in parser.doc_hits:
		if _is_allowed_doc_url(h.url):
			docs.append(h)

	pages = [p for p in parser.page_links if CONSTRUCTION_SITE_SAFETY_MANUAL_PREFIX in p]
	return docs, pages