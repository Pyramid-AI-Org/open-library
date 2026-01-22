from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class UrlRecord:
    url: str
    name: str | None
    discovered_at_utc: str  # ISO-8601 string
    source: str
    meta: dict[str, Any]


@dataclass(frozen=True)
class RunContext:
    run_date_utc: str
    started_at_utc: str
    settings: dict[str, Any]
    debug: bool = False


class BaseCrawler(Protocol):
    name: str

    def crawl(self, ctx: RunContext) -> list[UrlRecord]: ...
