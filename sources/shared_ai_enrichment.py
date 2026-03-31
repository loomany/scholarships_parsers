"""
Общие хелперы для AI-enrichment записей scholarships (нормализация ответа модели, merge в record).

Используется в sources.simpler_grants_gov.parser и sources.bigfuture.parser. Не привязан к Grants.gov.
"""

from __future__ import annotations

import copy
import json
from typing import Any, Callable

_MERGE_AI_MIN_DESCRIPTION_LEN = 24
_MERGE_AI_MIN_ELIGIBILITY_LEN = 20
_MERGE_AI_MIN_REQUIREMENTS_LEN = 40


def json_safe(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, float) and (obj != obj):
        return None
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(x) for x in obj]
    return str(obj)


def empty_ai_enrichment() -> dict[str, Any]:
    return {
        "short_summary": None,
        "eligibility_list": [],
        "key_requirements": [],
        "required_documents": [],
        "funding_amount_text": None,
        "deadline_text": None,
        "payout_method": None,
        "provider_name": None,
        "student_relevance": None,
        "confidence_score": None,
    }


def coerce_str_list(val: Any) -> list[str] | None:
    if val is None:
        return None
    if isinstance(val, list):
        out = [str(x).strip() for x in val if str(x).strip()]
        return out
    if isinstance(val, str) and val.strip():
        return [val.strip()]
    return None


def normalize_ai_enrichment_parsed(raw: dict[str, Any] | None) -> dict[str, Any]:
    base = empty_ai_enrichment()
    if not raw:
        return base
    base["short_summary"] = (
        str(raw["short_summary"]).strip() if raw.get("short_summary") is not None else None
    ) or None
    for lst_key in ("eligibility_list", "key_requirements", "required_documents"):
        coerced = coerce_str_list(raw.get(lst_key))
        base[lst_key] = coerced if coerced is not None else []
    for sk in ("funding_amount_text", "deadline_text", "payout_method", "provider_name"):
        v = raw.get(sk)
        base[sk] = str(v).strip() if v is not None and str(v).strip() else None
    sr = raw.get("student_relevance")
    if sr is not None and str(sr).strip():
        base["student_relevance"] = str(sr).strip().lower()
    else:
        base["student_relevance"] = None
    cs = raw.get("confidence_score")
    if cs is None:
        base["confidence_score"] = None
    else:
        try:
            f = float(cs)
            if f != f:
                base["confidence_score"] = None
            else:
                base["confidence_score"] = max(0.0, min(1.0, f))
        except (TypeError, ValueError):
            base["confidence_score"] = None
    return base


def _record_field_str(record: dict[str, Any], key: str) -> str:
    v = record.get(key)
    if v is None:
        return ""
    return str(v).strip()


def _is_description_weak_for_ai_merge(text: str | None) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    return len(t) < _MERGE_AI_MIN_DESCRIPTION_LEN


def _eligibility_weak_for_merge(
    text: str | None,
    *,
    preclean: Callable[[str], str] | None = None,
) -> bool:
    raw = (text or "").strip()
    t = preclean(raw) if preclean else raw
    if not t:
        return True
    return len(t) < _MERGE_AI_MIN_ELIGIBILITY_LEN


def _requirements_weak_for_merge(
    text: str | None,
    *,
    preclean: Callable[[str | None], str | None] | None = None,
) -> bool:
    raw = (text or "").strip() or None
    cleaned = preclean(raw) if preclean else raw
    if not cleaned:
        return True
    return len(str(cleaned).strip()) < _MERGE_AI_MIN_REQUIREMENTS_LEN


def _ai_join_lines(items: Any) -> str | None:
    lines = coerce_str_list(items)
    if not lines:
        return None
    body = "\n".join(lines)
    return body if body.strip() else None


def ensure_mutable_raw_data(record: dict[str, Any]) -> dict[str, Any]:
    rd = record.get("raw_data")
    if isinstance(rd, dict):
        return copy.deepcopy(rd)
    if isinstance(rd, str) and rd.strip():
        try:
            parsed = json.loads(rd)
            if isinstance(parsed, dict):
                return copy.deepcopy(parsed)
        except json.JSONDecodeError:
            pass
    return {}


def merge_ai_enrichment_into_record(
    record: dict[str, Any] | None,
    ai_data: dict[str, Any] | None,
    *,
    eligibility_text_preclean: Callable[[str], str] | None = None,
    requirements_text_preclean: Callable[[str | None], str | None] | None = None,
) -> dict[str, Any]:
    """
    Вливает поля из normalize_ai_enrichment_parsed(ai_data) в запись, не затирая сильные значения.

    eligibility_text_preclean / requirements_text_preclean — опционально (Simpler: gov/fluff cleanup;
    BigFuture: None — только длина исходного текста).
    """
    out = dict(record or {})
    rd = ensure_mutable_raw_data(out)
    if isinstance(ai_data, dict):
        rd["ai_enrichment"] = json_safe(copy.deepcopy(ai_data))
    else:
        rd["ai_enrichment"] = json_safe(ai_data)
    out["raw_data"] = rd

    ai = (
        normalize_ai_enrichment_parsed(ai_data)
        if isinstance(ai_data, dict)
        else empty_ai_enrichment()
    )

    if _is_description_weak_for_ai_merge(_record_field_str(out, "description")):
        summ = (ai.get("short_summary") or "").strip() if ai.get("short_summary") else ""
        if summ:
            out["description"] = summ

    if _eligibility_weak_for_merge(
        _record_field_str(out, "eligibility_text"),
        preclean=eligibility_text_preclean,
    ):
        joined = _ai_join_lines(ai.get("eligibility_list"))
        if joined:
            out["eligibility_text"] = joined

    if _requirements_weak_for_merge(
        _record_field_str(out, "requirements_text"),
        preclean=requirements_text_preclean,
    ):
        joined = _ai_join_lines(ai.get("key_requirements"))
        if joined:
            out["requirements_text"] = joined

    if not _record_field_str(out, "award_amount_text"):
        famt = (ai.get("funding_amount_text") or "").strip() if ai.get("funding_amount_text") else ""
        if famt:
            out["award_amount_text"] = famt

    if not _record_field_str(out, "deadline_text"):
        dlt = (ai.get("deadline_text") or "").strip() if ai.get("deadline_text") else ""
        if dlt:
            out["deadline_text"] = dlt

    if not _record_field_str(out, "winner_payment_text"):
        pay = (ai.get("payout_method") or "").strip() if ai.get("payout_method") else ""
        if pay:
            out["winner_payment_text"] = pay

    if not _record_field_str(out, "provider_name"):
        pn = (ai.get("provider_name") or "").strip() if ai.get("provider_name") else ""
        if pn:
            out["provider_name"] = pn

    return out
