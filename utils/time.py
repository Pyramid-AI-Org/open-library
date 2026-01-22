from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_date_yyyymmdd(dt: datetime | None = None) -> str:
    if dt is None:
        dt = utc_now()
    return dt.strftime("%Y-%m-%d")
