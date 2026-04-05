"""Shared taxonomy derivation helpers for scholarship eligibility and education levels."""

from __future__ import annotations

import html
import re
from typing import Any


def _strip_html_to_text(value: Any) -> str:
    if not value:
        return ""
    raw = str(value)
    raw = re.sub(r"</(p|div|li|tr|h[1-6]|br)>", " ", raw, flags=re.I)
    raw = re.sub(r"<[^>]+>", " ", raw)
    return " ".join(html.unescape(raw).split())


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " ".join(_to_text(v) for v in value if v is not None)
    if isinstance(value, dict):
        return " ".join(_to_text(v) for v in value.values() if v is not None)
    return " ".join(str(value).split())


def build_taxonomy_blob(record: dict[str, Any]) -> str:
    """Build a normalized lowercase text blob from scholarship content fields."""
    parts: list[str] = []
    for key in (
        "title",
        "description",
        "summary_short",
        "summary",
        "summary_long",
        "requirements_text",
        "eligibility_text",
        "who_can_apply",
        "institutions_text",
        "study_levels",
        "field_of_study",
        "raw_data",
    ):
        value = record.get(key)
        if not value:
            continue
        parts.append(_to_text(value))

    for key in ("description_html", "eligibility_html", "requirements_html", "full_content_html"):
        value = record.get(key)
        if value:
            parts.append(_strip_html_to_text(value))

    return " ".join(parts).lower()


ELIGIBILITY_PATTERNS: list[tuple[str, list[str]]] = [
    ("women", [r"\bwomen\b", r"\bfemale\b", r"\bgirl(s)?\b"]),
    ("minority", [r"\bminority\b", r"underrepresented (group|student|population)"]),
    ("african_american", [r"african\s*american", r"\bblack students?\b"]),
    ("disability", [r"\bdisabilit(y|ies)\b", r"special needs", r"ada"]),
    ("lgbtq", [r"\blgbtq?\b", r"lesbian", r"gay", r"bisexual", r"transgender", r"queer"]),
    ("foster_youth", [r"foster youth", r"foster care", r"former foster"]),
    ("low_income", [r"low\s*income", r"limited income", r"economically disadvantaged"]),
    ("international_students", [r"international students?", r"non[-\s]?u\.?s\.? citizen", r"f[- ]?1 visa", r"study permit"]),
    ("hispanic", [r"\bhispanic\b", r"latinx?", r"latino/a?"]),
    ("first_generation", [r"first[-\s]?generation", r"first in (their|the) family"]),
    ("veterans", [r"\bveteran(s)?\b", r"active duty", r"military service", r"service member"]),
    ("single_parent", [r"single parent", r"single mother", r"single father"]),
    ("native_american", [r"native american", r"american indian", r"alaska native", r"tribal (member|nation)"]),
    ("financial_need", [r"financial need", r"need[-\s]?based", r"demonstrate need", r"fafsa", r"pell grant"]),
]

EDUCATION_LEVEL_PATTERNS: list[tuple[str, list[str]]] = [
    ("high_school_senior", [r"high school senior", r"12th grade", r"senior year"]),
    ("high_school", [r"\bhigh school\b", r"secondary school", r"k[- ]?12"]),
    ("undergraduate", [r"undergraduate", r"\bcollege student\b", r"bachelor'?s", r"freshman", r"sophomore", r"junior", r"senior student"]),
    ("graduate", [r"graduate student", r"master'?s", r"postgraduate", r"grad school"]),
    ("phd", [r"\bph\.?d\b", r"doctoral", r"doctorate"]),
    ("community_college", [r"community college", r"two[-\s]?year college", r"associate degree"]),
    ("trade_school", [r"trade school", r"vocational", r"technical school", r"certificate program", r"apprenticeship"]),
]


def _match_patterns(blob: str, mapping: list[tuple[str, list[str]]]) -> list[str]:
    found: list[str] = []
    for tag, patterns in mapping:
        if any(re.search(p, blob, re.I) for p in patterns):
            found.append(tag)
    return found


def derive_eligibility_tags(record: dict[str, Any], blob: str | None = None) -> list[str]:
    text = blob if blob is not None else build_taxonomy_blob(record)
    return _match_patterns(text, ELIGIBILITY_PATTERNS)


def derive_catalog_education_levels(record: dict[str, Any], blob: str | None = None) -> list[str]:
    text = blob if blob is not None else build_taxonomy_blob(record)
    levels = _match_patterns(text, EDUCATION_LEVEL_PATTERNS)
    if "phd" in levels and "graduate" not in levels:
        levels.append("graduate")
    if "high_school_senior" in levels and "high_school" not in levels:
        levels.append("high_school")
    return levels
