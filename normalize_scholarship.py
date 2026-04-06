"""
Post-parse normalization: filters, categories, requirement signals, ranking_score, slug.
Rule-based only — no invented facts; nullable/empty when data missing.
"""

from __future__ import annotations

import hashlib
import html as html_lib
import re
from datetime import date, datetime, timezone
from typing import Any

from scholarship_taxonomy import (
    CANONICAL_CITIZENSHIP_SLUGS,
    CANONICAL_FIELD_OF_STUDY_SLUGS,
    CANONICAL_STUDY_LEVEL_SLUGS,
    build_taxonomy_blob,
    derive_easy_apply_flags,
    derive_catalog_education_levels,
    derive_eligibility_tags,
    derive_gpa_fields,
    derive_listing_completeness,
    derive_location_tags,
    derive_structured_citizenship_statuses,
    derive_structured_field_of_study,
    derive_structured_study_levels,
    keep_only_canonical_slugs,
)

# Maps to UI category ids (scholarshipCategories.ts)
CATEGORY_RULES: list[tuple[str, list[str]]] = [
    (
        "arts",
        [
            r"\bart\b",
            r"\barts\b",
            r"visual art",
            r"performing arts",
            r"\bdesign\b",
            r"creative writing",
            r"fine art",
        ],
    ),
    (
        "education",
        [
            r"\beducation\b",
            r"teaching",
            r"teacher",
            r"school leadership",
            r"future teacher",
        ],
    ),
    (
        "humanities",
        [
            r"\bhistory\b",
            r"literature",
            r"philosophy",
            r"languages",
            r"cultural stud",
            r"humanities",
        ],
    ),
    (
        "stem",
        [
            r"\bstem\b",
            r"science",
            r"technology",
            r"engineering",
            r"mathematics",
            r"\bmath\b",
            r"computer science",
            r"\bcs\b",
            r"data science",
        ],
    ),
    (
        "medical",
        [
            r"medicine",
            r"medical",
            r"nursing",
            r"health care",
            r"healthcare",
            r"biomedical",
            r"diagnosis",
        ],
    ),
    (
        "law",
        [
            r"\blaw\b",
            r"legal",
            r"jurisprudence",
            r"pre-law",
            r"paralegal",
        ],
    ),
    (
        "community",
        [
            r"community service",
            r"volunteer",
            r"civic",
            r"local impact",
            r"public service",
        ],
    ),
    (
        "biology",
        [
            r"\bbiology\b",
            r"life science",
            r"biological science",
            r"molecular bio",
        ],
    ),
    (
        "safety",
        [
            r"\bsafety\b",
            r"protective services",
            r"public safety",
            r"law enforcement",
            r"firefighter",
        ],
    ),
    (
        "music",
        [
            r"\bmusic\b",
            r"vocal",
            r"instrument",
            r"orchestra",
            r"band ",
        ],
    ),
    (
        "disability",
        [
            r"disability",
            r"special needs",
            r"accessibility",
            r"ada\b",
            r"impairment",
        ],
    ),
    (
        "hobbies",
        [
            r"hobby",
            r"recreation",
            r"craft",
            r"personal interest",
        ],
    ),
]

REQ_SPECS: list[tuple[str, str, re.Pattern[str]]] = [
    ("essay", "essay_required", re.compile(
        r"essay|personal statement|statement of purpose|writing sample", re.I
    )),
    ("document", "document_required", re.compile(
        r"transcript|upload document|pdf|certificate|proof of|official document|"
        r"form\b|upload file|supporting document",
        re.I,
    )),
    ("photo", "photo_required", re.compile(
        r"\bphoto\b|headshot|portrait|image upload|picture", re.I
    )),
    ("video", "video_required", re.compile(
        r"\bvideo\b|recorded response|video response|webcam", re.I
    )),
    ("link", "link_required", re.compile(
        r"portfolio|linkedin|url\b|website\b|http|social media link", re.I
    )),
    ("survey", "survey_required", re.compile(
        r"survey|questionnaire", re.I
    )),
    ("question", "question_required", re.compile(
        r"short answer|short-answer|response question|essay question|prompts?\b", re.I
    )),
    ("goal", "goal_required", re.compile(
        r"career goal|academic goal|future plan|educational goal|objective", re.I
    )),
    (
        "special_eligibility",
        "special_eligibility_required",
        re.compile(
            r"veteran|disability|minority|medical condition|first generation|"
            r"lgbt|identity|state resident only|specific eligibility",
            re.I,
        ),
    ),
    ("transcript", "transcript_required", re.compile(
        r"transcript|grade report|academic record", re.I
    )),
    (
        "recommendation",
        "recommendation_required",
        re.compile(r"recommendation letter|letter of recommendation|reference letter", re.I),
    ),
]

_US_STATE_RE = re.compile(
    r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|"
    r"MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b"
)


def _blob(record: dict[str, Any]) -> str:
    parts: list[str] = []
    for k in (
        "title",
        "description",
        "eligibility_text",
        "requirements_text",
        "awards_text",
        "notification_text",
        "selection_criteria_text",
        "winner_payment_text",
        "category",
        "institutions_text",
        "state_territory_text",
        "provider_mission",
    ):
        v = record.get(k)
        if v:
            parts.append(str(v))
    tags = record.get("tags")
    if isinstance(tags, list):
        parts.extend(str(t) for t in tags if t)
    return " ".join(parts).lower()


def _slugify(title: str, suffix: str | None) -> str:
    t = re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")[:72]
    suf = re.sub(r"[^a-z0-9]+", "-", (suffix or "").lower()).strip("-")[:32]
    if t and suf:
        out = f"{t}-{suf}"
    elif t:
        out = t
    elif suf:
        out = f"scholarship-{suf}"
    else:
        h = hashlib.sha256((title or "x").encode()).hexdigest()[:10]
        out = f"scholarship-{h}"
    return out[:120]


def _infer_categories(blob: str) -> list[str]:
    found: list[str] = []
    for cat, patterns in CATEGORY_RULES:
        for pat in patterns:
            if re.search(pat, blob, re.I):
                if cat not in found:
                    found.append(cat)
                break
    return found if found else ["miscellaneous"]


def _extract_requirements(blob: str) -> tuple[list[str], dict[str, bool]]:
    types: list[str] = []
    flags: dict[str, bool] = {}
    for type_id, col, pat in REQ_SPECS:
        if pat.search(blob):
            if type_id not in types:
                types.append(type_id)
            flags[col] = True
    return types, flags


def _financial_need(blob: str) -> bool:
    return bool(
        re.search(
            r"financial need|demonstrate need|fafsa|efc\b|need-based", blob, re.I
        )
    )


def _payout_method(record: dict[str, Any], blob: str) -> str:
    w = " ".join(
        filter(
            None,
            [
                record.get("winner_payment_text"),
                record.get("payment_details"),
                record.get("payment_html") if isinstance(record.get("payment_html"), str) else None,
            ],
        )
    ).lower()
    if not w.strip():
        w = blob[:4000]
    if re.search(
        r"non-cash|in-kind|equipment|subscription|course credit|prize\b|not monetary|"
        r"tuition waiver only",
        w,
        re.I,
    ):
        return "non_monetary"
    if re.search(
        r"financial aid office|bursar|registrar|institution|college account|school office|"
        r"disbursed to (the )?school",
        w,
        re.I,
    ):
        return "college"
    if re.search(
        r"directly to you|paid to (the )?student|student account|recipient|individual",
        w,
        re.I,
    ):
        return "student"
    if len(w.strip()) < 8:
        return "not_stated"
    return "not_stated"


def _deadline_fields(
    deadline_date: str | None,
    status_text: str | None,
) -> tuple[int | None, str | None, str | None]:
    st_low = (status_text or "").strip().lower()
    if not deadline_date:
        if st_low == "open":
            return None, None, "open"
        if st_low == "closed":
            return None, None, "closed"
        return None, None, "unknown"
    try:
        y, m, d = deadline_date.strip()[:10].split("-")
        ddt = date(int(y), int(m), int(d))
    except (ValueError, AttributeError):
        return None, None, "unknown"
    today = date.today()
    delta = (ddt - today).days
    if delta < 0:
        bucket = "unknown"
    elif delta < 1:
        bucket = "lt_1d"
    elif delta <= 7:
        bucket = "d1_7"
    elif delta <= 28:
        bucket = "d8_28"
    else:
        bucket = "gt_28"
    st = (status_text or "").strip().lower()
    if st == "open":
        sch_status = "open"
    elif st == "closed":
        sch_status = "closed"
    elif delta > 30:
        sch_status = "upcoming"
    elif delta >= 0:
        sch_status = "open"
    else:
        sch_status = "closed"
    return delta, bucket, sch_status


def _credibility(record: dict[str, Any], blob: str) -> tuple[int | None, str]:
    if record.get("is_verified"):
        return 95, "verified"
    txt = record.get("credibility_score_text") or ""
    m = re.search(r"(\d{1,3})\s*%", str(txt))
    if m:
        n = int(m.group(1))
        n = max(0, min(100, n))
        if n <= 35:
            b = "low"
        elif n <= 69:
            b = "medium"
        else:
            b = "high"
        return n, b
    score = 0
    if record.get("provider_url"):
        score += 15
    if record.get("apply_url"):
        score += 20
    if record.get("deadline_date"):
        score += 15
    if record.get("requirements_text"):
        score += 10
    if record.get("support_email") or record.get("support_phone"):
        score += 10
    if record.get("winner_payment_text"):
        score += 10
    if len(blob) > 400:
        score += 10
    if record.get("source") == "scholarship_america":
        score += 10
    score = min(100, score)
    if score == 0:
        return None, "unknown"
    if score <= 35:
        return score, "low"
    if score <= 69:
        return score, "medium"
    return score, "high"


def _ranking_score(
    record: dict[str, Any],
    days_left: int | None,
    req_count: int,
    amount_sort: float | None,
) -> float:
    s = 0.0
    st = (record.get("scholarship_status") or "").lower()
    if st == "open":
        s += 25
    elif st == "upcoming":
        s += 15
    if days_left is not None and days_left >= 0:
        s += min(20.0, days_left / 14.0 * 10)
    if record.get("apply_url"):
        s += 15
    if amount_sort and amount_sort > 0:
        s += min(25.0, amount_sort / 5000.0 * 15)
    s += max(0.0, 15.0 - min(15, req_count))
    if record.get("provider_url"):
        s += 5
    if record.get("requirements_text"):
        s += 5
    return round(s, 4)


def _location_scope(state_territory: str | None) -> str | None:
    if not state_territory:
        return None
    t = state_territory.strip().lower()
    if "national" in t or "nationwide" in t or "u.s." in t or "united states" in t:
        return "national"
    if "international" in t or "global" in t:
        return "international"
    if _US_STATE_RE.search(state_territory.upper()):
        return "state"
    return "unknown"


def _state_codes(state_territory: str | None) -> list[str]:
    if not state_territory:
        return []
    return list(dict.fromkeys(_US_STATE_RE.findall(state_territory.upper())))


def _institution_types(institutions_text: str | None) -> list[str]:
    if not institutions_text:
        return []
    parts = re.split(r"[,;]", institutions_text)
    return [p.strip() for p in parts if p.strip()]


def _strip_html_to_plain(html_or_text: str | None) -> str:
    if not html_or_text:
        return ""
    t = html_lib.unescape(re.sub(r"<[^>]+>", " ", str(html_or_text)))
    return " ".join(t.split())


def _junk_cta_line(line: str) -> bool:
    low = line.lower().strip()
    if len(low) < 8:
        return False
    needles = (
        "application is ready",
        "ready to be submitted",
        "click apply",
        "mark as started",
        "mark as submitted",
        "sign up for",
        "scholarship updates",
        "subscribe to",
        "get notified",
        "application ready",
        "submit the application",
        "create an account",
        "log in to apply",
        "start your application",
    )
    return any(n in low for n in needles)


def _clean_requirements_line(text: str) -> str:
    lines: list[str] = []
    junk = {"|", ".", "not", "a", "A"}
    for line in text.replace("\r", "\n").split("\n"):
        s = line.strip()
        if not s or s in junk or (len(s) <= 1 and s.isalpha()):
            continue
        if _junk_cta_line(s):
            continue
        lines.append(s)
    return "\n".join(lines)


def _eligibility_bullet_lines(record: dict[str, Any]) -> list[str]:
    plain = (record.get("eligibility_text") or "").strip()
    if plain:
        raw = plain
    else:
        rough = str(record.get("eligibility_html") or "")
        if not rough.strip():
            return []
        rough = re.sub(r"</(p|div|li|tr|h[1-6])>", "\n", rough, flags=re.I)
        rough = re.sub(r"<br\s*/?>", "\n", rough, flags=re.I)
        raw = _strip_html_to_plain(rough)
    if not raw.strip():
        return []
    chunks: list[str] = []
    for block in re.split(r"[\n\r•]+", raw):
        b = block.strip()
        if not b:
            continue
        b = re.sub(r"^[\-\*▪·]+\s*", "", b)
        b = re.sub(r"^\d+[\).\]]\s*", "", b).strip()
        if len(b) < 12 or _junk_cta_line(b):
            continue
        chunks.append(b)
    # De-dupe preserving order (case-insensitive)
    seen: set[str] = set()
    out: list[str] = []
    for c in chunks:
        key = c.lower()
        if key in seen:
            continue
        seen.add(key)
        if len(c) > 500:
            c = c[:497] + "…"
        out.append(c)
        if len(out) >= 24:
            break
    return out


def _requirements_clean_blob(record: dict[str, Any]) -> str | None:
    parts: list[str] = []
    rt = record.get("requirements_text")
    if rt:
        parts.append(str(rt))
    rh = record.get("requirements_html")
    if rh:
        rough = re.sub(r"</(p|div|li|tr|h[1-6])>", "\n", str(rh), flags=re.I)
        rough = re.sub(r"<br\s*/?>", "\n", rough, flags=re.I)
        parts.append(_strip_html_to_plain(rough))
    blob = "\n".join(parts)
    lines: list[str] = []
    junk = {"|", ".", "not", "a", "A", "—", "-", "•", "·", "▪"}
    for line in blob.replace("\r", "\n").split("\n"):
        s = line.strip()
        if not s or s in junk:
            continue
        if len(s) <= 1 and s.isalpha():
            continue
        if _junk_cta_line(s):
            continue
        if re.match(r"^[\s|.\-–—•·▪]+$", s):
            continue
        lines.append(s)
    out = "\n".join(lines).strip()
    if not out:
        return None
    filtered = _filter_requirements_rule_lines(out, record)
    return filtered or None


def _join_natural_labels(parts: list[str]) -> str:
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return ", ".join(parts[:-1]) + f", and {parts[-1]}"


def _strip_req_cell_noise(s: str) -> str:
    t = s.strip()
    t = re.sub(r"^\|+\s*|\s*\|+$", "", t)
    t = re.sub(r"^\s*[\-*•]+\s*", "", t)
    return t.strip()


def _is_transcript_meta_line(s: str) -> bool:
    line = _strip_req_cell_noise(s)
    if len(line) > 100:
        return False
    low = line.lower()
    if re.match(r"^(student\s*)?name\s*:?\s*$", line, re.I) or re.search(
        r"^student\s+name\b", line, re.I
    ):
        return True
    if re.match(r"^school\s*name", line, re.I):
        return True
    if re.match(r"^(current\s+)?grades?\s*:?\s*$", low, re.I) or (
        re.match(r"^grades?\b", low, re.I)
        and len(line) < 48
        and not re.search(r"\b(must|should|will|are|need)\b", low)
    ):
        return True
    if (
        re.match(r"^credit\s+hours?\s*:?\s*$", low, re.I)
        or (re.match(r"^credit\s+hours?\b", low, re.I) and len(line) < 56)
        or (
            re.search(r"number\s+of\s+credits?|course\s+credits?", low)
            and len(line) < 56
        )
    ):
        return True
    if re.search(r"^cumulative\s+gpa\b|^gpa\s*:?\s*$|^class\s*rank\b", low, re.I):
        return True
    return False


def _transcript_labels_from_group(group: list[str]) -> list[str]:
    order: list[str] = []
    seen: set[str] = set()

    def add(label: str) -> None:
        k = label.lower()
        if k in seen:
            return
        seen.add(k)
        order.append(label)

    for raw in group:
        plain = _strip_req_cell_noise(raw)
        low = plain.lower()
        if re.search(r"student\s+name|^student\s*name\b", plain, re.I):
            add("student name")
        elif re.search(r"school\s*name", low):
            add("school name")
        elif re.search(r"\bgrades?\b", low) and "transcript" not in low:
            add("grades")
        elif re.search(r"credit\s+hours?|number\s+of\s+credits?", low) or (
            re.match(r"^credits?\b", plain, re.I) and len(plain) < 36
        ):
            add("course credit hours")
        elif re.search(r"gpa|class\s*rank", low):
            add("GPA or class rank")
    return order


def _merge_transcript_metadata_lines(lines: list[str]) -> list[str]:
    i = 0
    out: list[str] = []
    while i < len(lines):
        if _is_transcript_meta_line(lines[i]):
            group: list[str] = []
            while i < len(lines) and _is_transcript_meta_line(lines[i]):
                group.append(lines[i])
                i += 1
            labels = _transcript_labels_from_group(group)
            if labels:
                out.append(
                    "Official transcript must include "
                    f"{_join_natural_labels(labels)}."
                )
            continue
        out.append(lines[i])
        i += 1
    return out


def _is_standalone_doc_title(s: str, has_transcript: bool, has_rec: bool, has_essay: bool) -> bool:
    t = _strip_req_cell_noise(s)
    if len(t) > 140:
        return False
    low = t.lower().rstrip(".")
    if has_transcript and re.match(
        r"^(official )?transcript(s)?( or (academic )?record)?$", low, re.I
    ):
        return True
    if has_rec and re.match(r"^(letter of )?recommendation(s)?( letter)?$", low, re.I):
        return True
    if has_essay and re.match(r"^essay(s)?( or written statement)?$", low, re.I):
        return True
    if has_essay and re.match(r"^personal statement$", low, re.I):
        return True
    return False


def _filter_requirements_rule_lines(blob: str, record: dict[str, Any]) -> str | None:
    """Drop section headers, CTA boilerplate, contact lines, doc-only instructions."""
    support_email = (record.get("support_email") or "").strip().lower()
    lines_out: list[str] = []
    doc_blob = " ".join(
        filter(
            None,
            [
                str(record.get("requirements_text") or ""),
                _strip_html_to_plain(record.get("requirements_html")),
            ],
        )
    ).lower()
    has_transcript = bool(
        re.search(r"transcript|grade report|academic record", doc_blob)
    )
    has_rec = bool(re.search(r"recommendation|reference letter", doc_blob))
    has_essay = bool(re.search(r"\bessay\b|personal statement", doc_blob))

    header_re = re.compile(
        r"^(eligibility )?requirements\s*:?\s*$|^requirements\s*$|^requirement\s*$",
        re.I,
    )
    for line in blob.replace("\r", "\n").split("\n"):
        s = line.strip()
        if not s or header_re.match(s):
            continue
        low = s.lower()
        if low.startswith("note:") or low.startswith("note "):
            continue
        if low.startswith("your application is"):
            continue
        if low.startswith("as part of your application"):
            continue
        if re.match(r"^the following (documents|materials) (are )?required", low):
            continue
        if re.match(r"^please (direct )?any questions", low):
            continue
        if re.match(r"^required\s*documents?\s*:?\s*$", low, re.I):
            continue
        if re.match(r"^supporting\s*documents?\s*:?\s*$", low, re.I):
            continue
        if support_email and support_email in low:
            continue
        if "@" in s and "contact" in low:
            continue
        if re.search(r"\S+@\S+\.\S+", s):
            continue
        if re.search(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b", s) and len(s) < 160:
            continue
        if re.match(r"^no exceptions\.?$", low, re.I) or low in (
            "no exceptions",
            "no exception",
        ):
            continue
        if re.search(r"must be completed by (a )?(physician|doctor|medical)", low):
            continue
        if re.match(r"^for (more )?information\b", low):
            continue
        if re.match(r"^(questions?|inquiries)\b.*\b(contact|call|email)\b", low):
            continue
        if re.match(r"^(call|email|contact)\s+(us|the|our)\b", low):
            continue
        if re.match(r"^if you have (any )?questions\b", low):
            continue
        if _is_standalone_doc_title(s, has_transcript, has_rec, has_essay):
            continue
        if len(s) <= 220 and (
            re.search(
                r"\b(upload|submit|provide|attach)\b.*\b(transcript|recommendation|essay)\b",
                low,
            )
        ):
            if has_transcript and re.search(
                r"transcript|grade report|academic record", low
            ):
                continue
            if has_rec and re.search(r"recommendation|reference", low):
                continue
        lines_out.append(s)
    merged = _merge_transcript_metadata_lines(lines_out)
    out = "\n".join(merged).strip()
    return out or None


def _extract_documents_from_text(blob: str) -> list[str]:
    low = blob.lower()
    found: list[str] = []

    def add(label: str) -> None:
        if label not in found:
            found.append(label)

    if re.search(r"transcript|grade report|academic record", low):
        add("Official transcript or academic record")
    if re.search(r"letter of recommendation|recommendation letter|reference letter", low):
        add("Letter of recommendation")
    if re.search(r"\bessay\b|personal statement|statement of purpose|writing sample", low):
        add("Essay or written statement")
    if re.search(r"\bfafsa\b|student aid report|sar\b", low):
        add("FAFSA or financial aid documentation")
    if re.search(r"financial need|tax return|w-2|1040|income verification", low):
        add("Financial documentation (if required)")
    if re.search(r"\bresume\b|\bcv\b|curriculum vitae", low):
        add("Résumé or CV")
    if re.search(r"portfolio", low):
        add("Portfolio")
    if re.search(r"headshot|\bphoto\b|picture upload", low):
        add("Photo or headshot")
    if re.search(r"\bvideo\b|recorded response", low):
        add("Video submission")
    if re.search(r"test score|\bsat\b|\bact\b|standardized test", low):
        add("Standardized test scores (if applicable)")
    if re.search(r"diagnosis|medical verification|disability documentation", low):
        add("Medical or disability verification (if applicable)")
    if re.search(r"certificate|proof of enrollment|enrollment verification", low):
        add("Certificate or enrollment verification")
    if re.search(r"upload(ed)? document|supporting document|pdf upload", low):
        add("Uploaded supporting documents")
    return found


def _documents_from_flags(record: dict[str, Any]) -> list[str]:
    out: list[str] = []
    if record.get("transcript_required"):
        out.append("Official transcript or academic record")
    if record.get("recommendation_required"):
        out.append("Letter of recommendation")
    if record.get("essay_required"):
        out.append("Essay or written statement")
    if record.get("photo_required"):
        out.append("Photo or headshot")
    if record.get("video_required"):
        out.append("Video submission")
    if record.get("link_required"):
        out.append("Portfolio or external link")
    if record.get("document_required"):
        out.append("Supporting documents (as listed in requirements)")
    return out


def _merge_unique_labels(*lists: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for lst in lists:
        for x in lst:
            k = x.strip()
            if not k:
                continue
            kl = k.lower()
            if kl in seen:
                continue
            seen.add(kl)
            out.append(k)
    return out


def _build_summary_short(record: dict[str, Any]) -> str | None:
    parts: list[str] = []
    prov = (record.get("provider_name") or "").strip()
    amt = (record.get("award_amount_text") or "").strip()
    dl = (record.get("deadline_text") or "").strip()
    dd = record.get("deadline_date")
    inst = (record.get("institutions_text") or "").strip()
    loc = (record.get("state_territory_text") or "").strip()
    its = record.get("institution_types")
    scope = (record.get("location_scope") or "").strip().lower()
    states = record.get("state_codes") if isinstance(record.get("state_codes"), list) else []

    if prov:
        parts.append(
            f"{prov} offers this scholarship to help cover education costs."
        )
    else:
        parts.append(
            "This scholarship helps cover education costs for qualified students."
        )

    if isinstance(its, list) and its:
        joined = ", ".join(str(x).strip() for x in its if str(x).strip())
        if joined:
            parts.append(f"It is geared toward students attending {joined}.")
    elif inst and len(inst) <= 160:
        parts.append(f"It typically applies to {inst.rstrip('.')}.")
    elif loc and len(loc) <= 120:
        parts.append(f"The source describes eligibility as '{loc.rstrip('.')}'.")
    elif scope == "national":
        parts.append("It is open nationwide in the United States.")
    elif scope == "international":
        parts.append(
            "It may include international applicants; confirm on the official page."
        )
    elif scope == "state" and states:
        sc = ", ".join(str(x) for x in states if str(x).strip())
        if sc:
            parts.append(f"It emphasizes applicants with a connection to {sc}.")

    tail_bits: list[str] = []
    if amt:
        tail_bits.append(f"The listed award is {amt.rstrip('.')}.")
    if dl:
        tail_bits.append(f"Plan to apply by {dl.rstrip('.')}.")
    elif dd:
        tail_bits.append(f"Plan to apply by {dd}.")
    parts.extend(tail_bits)

    out = " ".join(parts).strip()
    if len(out) < 55:
        desc = _strip_html_to_plain(record.get("description"))
        if len(desc) >= 50:
            cut = desc[:300]
            if len(desc) > 300:
                cut = cut.rsplit(" ", 1)[0] + "…"
            out = cut
    if not out:
        return None
    # Cap at ~3 short sentences
    if len(out) > 420:
        out = out[:417].rsplit(" ", 1)[0] + "…"
    return out


def _build_summary_long(
    record: dict[str, Any],
    elig_bullets: list[str],
) -> str | None:
    desc = _strip_html_to_plain(record.get("description"))
    paras: list[str] = []
    if len(desc) >= 80:
        chunk = desc[:900]
        if len(desc) > 900:
            chunk = chunk.rsplit(" ", 1)[0] + "…"
        paras.append(chunk)
    if elig_bullets:
        preview = elig_bullets[:5]
        joined = " ".join(preview)
        if len(joined) > 600:
            joined = joined[:597] + "…"
        paras.append(f"Who may apply: {joined}")
    sel = (record.get("selection_criteria_text") or "").strip()
    if sel and len(sel) > 40:
        sc = sel[:500] + ("…" if len(sel) > 500 else "")
        paras.append(f"Selection notes: {sc}")
    if not paras:
        return None
    return "\n\n".join(paras)


def apply_normalization(record: dict[str, Any]) -> None:
    """Mutates record in place; adds/overwrites normalized keys for Supabase."""
    blob = _blob(record)

    sid = record.get("source_id") or ""
    url = record.get("url") or ""
    title = record.get("title") or "Scholarship"
    suf = str(sid).strip() if sid else hashlib.md5(url.encode()).hexdigest()[:12]
    record["slug"] = _slugify(str(title), suf)

    prov = record.get("provider_name") or ""
    record["provider_slug"] = _slugify(str(prov), None) if prov else None

    days, d_bucket, sch_status = _deadline_fields(
        record.get("deadline_date"),
        record.get("status_text"),
    )
    record["days_until_deadline"] = days
    record["deadline_bucket"] = d_bucket
    record["scholarship_status"] = sch_status

    amin = record.get("award_amount_min")
    amax = record.get("award_amount_max")
    num_sort: float | None = None
    if amin is not None and amax is not None:
        try:
            num_sort = float(max(amin, amax))
        except (TypeError, ValueError):
            num_sort = None
    elif amin is not None:
        try:
            num_sort = float(amin)
        except (TypeError, ValueError):
            num_sort = None
    elif amax is not None:
        try:
            num_sort = float(amax)
        except (TypeError, ValueError):
            num_sort = None
    record["award_amount_numeric_sort"] = num_sort

    record["payout_method"] = _payout_method(record, blob)

    req_types, req_flags = _extract_requirements(blob)
    record["requirement_types"] = req_types
    for k, v in req_flags.items():
        record[k] = v
    # default false for unset flags
    for _tid, col, _pat in REQ_SPECS:
        record.setdefault(col, False)

    record["financial_need_considered"] = _financial_need(blob)

    signals = sum(1 for _tid, col, _p in REQ_SPECS if record.get(col))
    record["requirement_signals_count"] = signals

    cats = _infer_categories(blob)
    existing_tags: list[str] = []
    if isinstance(record.get("tags"), list):
        existing_tags = [str(t) for t in record["tags"] if isinstance(t, str)]
    merged_tags: list[str] = []
    seen: set[str] = set()
    for t in existing_tags + cats:
        tl = t.strip().lower()
        if tl and tl not in seen:
            seen.add(tl)
            merged_tags.append(tl)
    record["tags"] = merged_tags
    if cats:
        record["category"] = cats[0]
        record["category_slug"] = cats[0].strip().lower()
    elif merged_tags:
        record["category_slug"] = merged_tags[0].strip().lower()
    else:
        record["category_slug"] = None

    taxonomy_blob = build_taxonomy_blob(record)
    existing_levels, dropped_levels = keep_only_canonical_slugs(
        record.get("study_levels"),
        CANONICAL_STUDY_LEVEL_SLUGS,
    )
    existing_fos, dropped_fos = keep_only_canonical_slugs(
        record.get("field_of_study"),
        CANONICAL_FIELD_OF_STUDY_SLUGS,
    )
    existing_cit, dropped_cit = keep_only_canonical_slugs(
        record.get("citizenship_statuses"),
        CANONICAL_CITIZENSHIP_SLUGS,
    )

    def _merge_unique_tokens(*values: list[str]) -> list[str]:
        merged: list[str] = []
        seen_local: set[str] = set()
        for arr in values:
            for item in arr:
                if not isinstance(item, str):
                    continue
                tok = item.strip().lower()
                if tok and tok not in seen_local:
                    seen_local.add(tok)
                    merged.append(tok)
        return merged

    derived_levels_raw = derive_structured_study_levels(record, taxonomy_blob)
    derived_fos_raw = derive_structured_field_of_study(record, taxonomy_blob)
    derived_citizenship_raw = derive_structured_citizenship_statuses(record, taxonomy_blob)
    derived_levels, dropped_derived_levels = keep_only_canonical_slugs(
        derived_levels_raw,
        CANONICAL_STUDY_LEVEL_SLUGS,
    )
    derived_fos, dropped_derived_fos = keep_only_canonical_slugs(
        derived_fos_raw,
        CANONICAL_FIELD_OF_STUDY_SLUGS,
    )
    derived_citizenship, dropped_derived_cit = keep_only_canonical_slugs(
        derived_citizenship_raw,
        CANONICAL_CITIZENSHIP_SLUGS,
    )

    record["study_levels"] = _merge_unique_tokens(existing_levels, derived_levels)
    record["field_of_study"] = _merge_unique_tokens(existing_fos, derived_fos)
    record["citizenship_statuses"] = _merge_unique_tokens(existing_cit, derived_citizenship)

    dropped_any = (
        dropped_levels
        or dropped_fos
        or dropped_cit
        or dropped_derived_levels
        or dropped_derived_fos
        or dropped_derived_cit
    )
    if dropped_any:
        rd = record.get("raw_data")
        if isinstance(rd, dict):
            raw_data_map = dict(rd)
        else:
            raw_data_map = {}
        raw_data_map["taxonomy_noncanonical_dropped"] = {
            "study_levels_existing": dropped_levels,
            "field_of_study_existing": dropped_fos,
            "citizenship_existing": dropped_cit,
            "study_levels_derived": dropped_derived_levels,
            "field_of_study_derived": dropped_derived_fos,
            "citizenship_derived": dropped_derived_cit,
        }
        record["raw_data"] = raw_data_map

    record["eligibility_tags"] = derive_eligibility_tags(record, taxonomy_blob)
    record["catalog_education_levels"] = derive_catalog_education_levels(record, taxonomy_blob)
    gpa_requirement_min, gpa_bucket = derive_gpa_fields(record, taxonomy_blob)
    record["gpa_requirement_min"] = gpa_requirement_min
    record["gpa_bucket"] = gpa_bucket
    record["location_tags"] = derive_location_tags(record, taxonomy_blob)
    record["easy_apply_flags"] = derive_easy_apply_flags(record, taxonomy_blob)

    stt = record.get("state_territory_text")
    if isinstance(stt, str):
        record["location_scope"] = _location_scope(stt)
        record["state_codes"] = _state_codes(stt)
    else:
        record["location_scope"] = None
        record["state_codes"] = []

    inst = record.get("institutions_text")
    record["institution_types"] = (
        _institution_types(inst) if isinstance(inst, str) else []
    )

    cscore, cbucket = _credibility(record, blob)
    record["credibility_score"] = cscore
    record["credibility_bucket"] = cbucket
    listing_bucket, is_verified = derive_listing_completeness(record, taxonomy_blob)
    record["listing_completeness_bucket"] = listing_bucket
    record["is_verified"] = is_verified

    elig_bullets = _eligibility_bullet_lines(record)
    record["who_can_apply"] = "\n".join(elig_bullets) if elig_bullets else None

    record["summary_short"] = _build_summary_short(record)
    record["summary_long"] = _build_summary_long(record, elig_bullets)

    record["notification_details"] = (record.get("notification_text") or "").strip() or None
    record["payment_details"] = (record.get("winner_payment_text") or "").strip() or None

    record["requirements_text_clean"] = _requirements_clean_blob(record)
    if not record["requirements_text_clean"]:
        rt0 = record.get("requirements_text")
        cleaned = _clean_requirements_line(str(rt0)) if rt0 else ""
        record["requirements_text_clean"] = cleaned.strip() or None

    record["official_source_name"] = (
        record.get("source") or "catalog"
    ).replace("_", " ").title()

    record["last_verified_at"] = datetime.now(timezone.utc).isoformat()
    record["is_indexable"] = True

    doc_blob = " ".join(
        filter(
            None,
            [
                str(record.get("requirements_text") or ""),
                str(record.get("eligibility_text") or ""),
                _strip_html_to_plain(record.get("requirements_html")),
                _strip_html_to_plain(record.get("eligibility_html")),
            ],
        )
    )
    record["documents_required"] = _merge_unique_labels(
        _documents_from_flags(record),
        _extract_documents_from_text(doc_blob),
    )

    record["ranking_score"] = _ranking_score(
        record,
        days,
        signals,
        num_sort,
    )

    rc = record.get("requirements_count")
    try:
        rc_int = int(rc) if rc is not None else None
    except (TypeError, ValueError):
        rc_int = None
    if (rc_int is None or rc_int == 0) and signals > 0:
        record["requirements_count"] = signals
