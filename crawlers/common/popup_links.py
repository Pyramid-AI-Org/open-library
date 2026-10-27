"""Literal-only handling for an explicitly configured publisher popup wrapper."""

from __future__ import annotations

import re

_POPUP = re.compile(
    r"javascript:\s*popup800600Scroll\(\s*'([^'\\\r\n]*)'\s*\)\s*;?\s*", re.IGNORECASE
)


def unwrap_popup_link(href: str, wrapper: str) -> str | None:
    """Return a literal target, never evaluate script or accept concatenation."""
    if wrapper != "popup800600Scroll":
        raise ValueError(f"Unsupported popup wrapper: {wrapper}")
    if not href.lower().startswith("javascript:"):
        return href
    match = _POPUP.fullmatch(href)
    if match:
        return match.group(1)
    if re.match(
        r"javascript:\s*popup800600Scroll\b", href, re.IGNORECASE
    ) and re.search(r"\.(?:pdf|docx?)\b", href, re.IGNORECASE):
        raise ValueError("Unparseable document popup link")
    return None
