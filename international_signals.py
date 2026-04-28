"""Helpers for detecting international-student eligibility signals."""

from __future__ import annotations

import re
from typing import Any

_INTERNATIONAL_STRONG_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\binternational students?\b", re.I),
    re.compile(r"\binternational applicants?\b", re.I),
    re.compile(r"\bforeign students?\b", re.I),
    re.compile(r"\bforeign nationals?\b", re.I),
    re.compile(r"\bnon[-\s]?u\.?s\.?\s*citizens?\b", re.I),
    re.compile(r"\bnonresident(?:\s+alien)?s?\b", re.I),
    re.compile(r"\bf[-\s]?1 visa\b", re.I),
    re.compile(r"\bstudy permit\b", re.I),
)

_INTERNATIONAL_WEAK_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\binternational\b", re.I),
    re.compile(r"\bstudy abroad\b", re.I),
    re.compile(r"\boverseas\b", re.I),
    re.compile(r"\bglobal applicants?\b", re.I),
)


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        return " ".join(_flatten_text(v) for v in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten_text(v) for v in value)
    return str(value)


def detect_international_signal(*values: Any) -> str | None:
    """Return 'strong', 'weak', or None for international-related signals."""
    blob = " ".join(_flatten_text(v) for v in values).lower()
    if not blob.strip():
        return None
    if any(p.search(blob) for p in _INTERNATIONAL_STRONG_PATTERNS):
        return "strong"
    if any(p.search(blob) for p in _INTERNATIONAL_WEAK_PATTERNS):
        return "weak"
    return None
