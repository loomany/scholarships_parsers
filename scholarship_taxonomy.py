"""Shared normalization helpers for scholarship taxonomy/filter fields."""

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


_TEXT_FIELDS_FOR_DERIVATION: tuple[str, ...] = (
    "title",
    "description",
    "eligibility_text",
    "requirements_text",
    "awards_text",
    "notification_text",
    "payment_details",
    "institutions_text",
    "field_of_study",
    "summary_short",
    "summary_long",
    "who_can_apply",
    "state_territory_text",
    "raw_data",
)


US_STATE_CANONICAL_BY_TOKEN: dict[str, str] = {
    "alabama": "AL",
    "al": "AL",
    "alaska": "AK",
    "ak": "AK",
    "arizona": "AZ",
    "az": "AZ",
    "arkansas": "AR",
    "ar": "AR",
    "california": "CA",
    "ca": "CA",
    "colorado": "CO",
    "co": "CO",
    "connecticut": "CT",
    "ct": "CT",
    "delaware": "DE",
    "de": "DE",
    "florida": "FL",
    "fl": "FL",
    "georgia": "GA",
    "ga": "GA",
    "hawaii": "HI",
    "hi": "HI",
    "idaho": "ID",
    "id": "ID",
    "illinois": "IL",
    "il": "IL",
    "indiana": "IN",
    "in": "IN",
    "iowa": "IA",
    "ia": "IA",
    "kansas": "KS",
    "ks": "KS",
    "kentucky": "KY",
    "ky": "KY",
    "louisiana": "LA",
    "la": "LA",
    "maine": "ME",
    "me": "ME",
    "maryland": "MD",
    "md": "MD",
    "massachusetts": "MA",
    "ma": "MA",
    "michigan": "MI",
    "mi": "MI",
    "minnesota": "MN",
    "mn": "MN",
    "mississippi": "MS",
    "ms": "MS",
    "missouri": "MO",
    "mo": "MO",
    "montana": "MT",
    "mt": "MT",
    "nebraska": "NE",
    "ne": "NE",
    "nevada": "NV",
    "nv": "NV",
    "new hampshire": "NH",
    "nh": "NH",
    "new jersey": "NJ",
    "nj": "NJ",
    "new mexico": "NM",
    "nm": "NM",
    "new york": "NY",
    "ny": "NY",
    "north carolina": "NC",
    "nc": "NC",
    "north dakota": "ND",
    "nd": "ND",
    "ohio": "OH",
    "oh": "OH",
    "oklahoma": "OK",
    "ok": "OK",
    "oregon": "OR",
    "or": "OR",
    "pennsylvania": "PA",
    "pa": "PA",
    "rhode island": "RI",
    "ri": "RI",
    "south carolina": "SC",
    "sc": "SC",
    "south dakota": "SD",
    "sd": "SD",
    "tennessee": "TN",
    "tn": "TN",
    "texas": "TX",
    "tx": "TX",
    "utah": "UT",
    "ut": "UT",
    "vermont": "VT",
    "vt": "VT",
    "virginia": "VA",
    "va": "VA",
    "washington": "WA",
    "wa": "WA",
    "west virginia": "WV",
    "wv": "WV",
    "wisconsin": "WI",
    "wi": "WI",
    "wyoming": "WY",
    "wy": "WY",
    "district of columbia": "DC",
    "dc": "DC",
}

GPA_BUCKETS: tuple[str, ...] = (
    "no_gpa_requirement",
    "gpa_2_0_plus",
    "gpa_2_5_plus",
    "gpa_3_0_plus",
    "gpa_3_5_plus",
)

EASY_APPLY_FLAGS: tuple[str, ...] = (
    "no_essay",
    "easy_apply",
    "quick_apply",
    "few_requirements",
)

LISTING_COMPLETENESS_BUCKETS: tuple[str, ...] = (
    "basic_info",
    "standard_detail",
    "detailed_listing",
    "verified_listing",
)


def _build_derivation_blob(record: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in _TEXT_FIELDS_FOR_DERIVATION:
        value = record.get(key)
        if value:
            parts.append(_to_text(value))
    return " ".join(parts).lower()


_GPA_MIN_RE = re.compile(
    r"(?:minimum|min\.?|at\s+least|must\s+have|maintain|require(?:s|d)?|"
    r"gpa(?:\s+of)?|grade point average(?:\s+of)?)"
    r"[^0-9]{0,16}(?P<gpa>[1-3](?:\.[0-9]{1,2})?|4(?:\.0{1,2})?)",
    re.I,
)
_GPA_NAKED_RE = re.compile(
    r"(?P<gpa>[1-3](?:\.[0-9]{1,2})?|4(?:\.0{1,2})?)\s*(?:gpa|grade point average)",
    re.I,
)
_NO_GPA_RE = re.compile(
    r"\b(no|not|without)\s+(minimum\s+)?gpa\b|"
    r"\bno\s+gpa\s+requirement\b|"
    r"\bgpa\s+(?:not\s+required|is\s+not\s+required)\b",
    re.I,
)


def derive_gpa_fields(record: dict[str, Any], blob: str | None = None) -> tuple[float | None, str | None]:
    text = blob if blob is not None else _build_derivation_blob(record)
    best: float | None = None

    for match in list(_GPA_MIN_RE.finditer(text)) + list(_GPA_NAKED_RE.finditer(text)):
        try:
            gpa = float(match.group("gpa"))
        except (TypeError, ValueError):
            continue
        if gpa < 1.0 or gpa > 4.0:
            continue
        if best is None or gpa > best:
            best = gpa

    if best is not None:
        if best >= 3.5:
            return best, "gpa_3_5_plus"
        if best >= 3.0:
            return best, "gpa_3_0_plus"
        if best >= 2.5:
            return best, "gpa_2_5_plus"
        return best, "gpa_2_0_plus"

    if _NO_GPA_RE.search(text):
        return None, "no_gpa_requirement"
    return None, None


def derive_location_tags(record: dict[str, Any], blob: str | None = None) -> list[str]:
    text = blob if blob is not None else _build_derivation_blob(record)
    found: list[str] = []
    seen: set[str] = set()

    def add(code: str) -> None:
        if code not in seen:
            seen.add(code)
            found.append(code)

    direct_state_codes = record.get("state_codes")
    if isinstance(direct_state_codes, list):
        for token in direct_state_codes:
            if isinstance(token, str):
                mapped = US_STATE_CANONICAL_BY_TOKEN.get(token.strip().lower())
                if mapped:
                    add(mapped)
    state_territory_text = record.get("state_territory_text")
    if isinstance(state_territory_text, str):
        for token in re.findall(
            r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|"
            r"MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b",
            state_territory_text.upper(),
        ):
            add(token)

    normalized_text = f" {text} "
    for token, canonical in US_STATE_CANONICAL_BY_TOKEN.items():
        if len(token) == 2:
            continue
        if re.search(rf"\b{re.escape(token)}\b", normalized_text, re.I):
            add(canonical)
    return found


def derive_easy_apply_flags(record: dict[str, Any], blob: str | None = None) -> list[str]:
    base_text = blob if blob is not None else _build_derivation_blob(record)
    apply_context = " ".join(
        _to_text(record.get(key))
        for key in (
            "apply_button_text",
            "application_status_text",
            "requirements_text",
            "notification_text",
            "apply_url",
            "provider_name",
            "raw_data",
        )
        if record.get(key)
    )
    text = f"{base_text} {apply_context}".strip().lower()
    out: list[str] = []

    def add(flag: str) -> None:
        if flag not in out:
            out.append(flag)

    no_essay_explicit = bool(
        re.search(
            r"\b("
            r"no\s+essay(?:\s+required)?|"
            r"essay\s+not\s+required|"
            r"no\s+personal\s+statement|"
            r"personal\s+statement\s+not\s+required"
            r")\b",
            text,
            re.I,
        )
    )
    if record.get("essay_required") is False or no_essay_explicit:
        add("no_essay")

    req_count_raw = record.get("requirements_count")
    signal_count_raw = record.get("requirement_signals_count")
    try:
        req_count = int(req_count_raw) if req_count_raw is not None else None
    except (TypeError, ValueError):
        req_count = None
    try:
        signal_count = int(signal_count_raw) if signal_count_raw is not None else None
    except (TypeError, ValueError):
        signal_count = None

    easy_keyword_match = bool(
        re.search(r"\b(easy apply|easy application|simple apply|simple application)\b", text, re.I)
    )
    quick_keyword_match = bool(
        re.search(r"\b(quick apply|one[-\s]?click apply|fast apply|instant apply)\b", text, re.I)
    )
    lightweight_keyword_match = bool(
        re.search(r"\b(few requirements|minimal requirements|simple application)\b", text, re.I)
    )

    if easy_keyword_match:
        add("easy_apply")
    if quick_keyword_match:
        add("quick_apply")
    if lightweight_keyword_match:
        add("few_requirements")

    if req_count is not None and req_count <= 2:
        add("few_requirements")

    has_apply_surface = bool(
        record.get("apply_button_text")
        or record.get("application_status_text")
        or record.get("apply_url")
    )
    heavy_requirements = any(
        record.get(key) is True
        for key in (
            "essay_required",
            "document_required",
            "photo_required",
            "link_required",
            "question_required",
            "recommendation_required",
            "transcript_required",
        )
    )
    low_friction = has_apply_surface and not heavy_requirements
    explicit_lightweight_signal = (
        lightweight_keyword_match
        or (req_count is not None and req_count <= 2)
    )
    explicit_ultra_light_signal = (
        (req_count is not None and req_count <= 1)
        or bool(re.search(r"\b(one[-\s]?step|single[-\s]?step)\b", text, re.I))
    )

    if (
        low_friction
        and signal_count is not None
        and signal_count <= 2
        and explicit_lightweight_signal
    ):
        add("easy_apply")
    if (
        low_friction
        and signal_count is not None
        and signal_count <= 1
        and explicit_ultra_light_signal
    ):
        add("quick_apply")

    return out


def derive_listing_completeness(record: dict[str, Any], blob: str | None = None) -> tuple[str, bool]:
    text = blob if blob is not None else _build_derivation_blob(record)
    raw_verified = record.get("is_verified")
    explicit_verified = False
    if isinstance(raw_verified, bool):
        explicit_verified = raw_verified
    elif isinstance(raw_verified, str):
        explicit_verified = raw_verified.strip().lower() in {"true", "1", "yes", "y"}
    elif isinstance(raw_verified, (int, float)):
        explicit_verified = bool(raw_verified)
    if not explicit_verified and (
        re.search(r"\bverified\b", text, re.I)
        or re.search(r"\bofficial\b", text, re.I)
    ):
        explicit_verified = True

    def _present(*keys: str, min_text_len: int = 1) -> bool:
        for key in keys:
            value = record.get(key)
            if isinstance(value, str):
                if len(value.strip()) >= min_text_len:
                    return True
            elif value is not None and value is not False:
                return True
        return False

    score = 0
    if _present("title", min_text_len=4):
        score += 1
    if _present("description", min_text_len=60):
        score += 1
    if _present("provider_name", min_text_len=3):
        score += 1
    if _present("apply_url", min_text_len=10):
        score += 1
    if _present("deadline_date", "deadline_text"):
        score += 1
    if _present("eligibility_text", "requirements_text", "requirements_text_clean", min_text_len=30):
        score += 1
    if _present("awards_text", "payment_details", "winner_payment_text", min_text_len=15):
        score += 1
    if _present("notification_text", min_text_len=20) or _present("support_email", "support_phone"):
        score += 1

    if explicit_verified:
        return "verified_listing", True
    if score >= 7:
        return "detailed_listing", False
    if score >= 4:
        return "standard_detail", False
    return "basic_info", False


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
