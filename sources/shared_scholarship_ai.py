"""
Единый финальный AI-слой для карточек scholarship (все парсеры каталога).

Вызывается из utils.upsert_scholarship после normalize/build_full_record и бизнес-фильтров
парсера, до записи в БД. Не подменяет факты из источника: фактические поля (amount, deadline,
provider, …) модель только интерпретирует; выдумывать их запрещено.

Схема ответа модели — единая JSON (student_summary, best_for, SEO, score, …).
Дополнительно считается rule-based score для устойчивости и смешивания с ai_match_score.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import date, datetime, timedelta
from typing import Any

from ai_monitoring import record_ai_completion, record_ai_error, record_ai_reuse
from business_filters import MIN_LEAD_DAYS_BEFORE_DEADLINE
from config import get_scholarships_ai_final_config
from sources.shared_ai_enrichment import json_safe

_JSON_SYSTEM = """You are a scholarship catalog editor helping U.S. students decide whether to apply.

STRICT RULES:
1) Do NOT invent facts: amounts, deadlines, provider names, eligibility rules, residency, degree level, field of study, application steps, essay requirements, or payout methods must ONLY come from the provided excerpt JSON. If unknown, say so in missing_info or use empty arrays — never fabricate.
2) Separate FACTS (tied to excerpt) from GUIDANCE (practical advice). Guidance arrays (application_tips, why_apply, important_checks) are labeled as advice — they must still be reasonable and generic when specifics are missing, and must NOT assert unverified facts.
3) Tone: clear, student-friendly, concise. Prefer short bullets. No fluff.
4) If data is thin: state that clearly; strengthen the card with honest missing_info, red_flags, and what to verify on the official site.
5) urgency_level MUST be one of: low, medium, high, urgent, unknown — derived from deadline in excerpt when present; otherwise unknown.
6) difficulty_level MUST be one of: easy, moderate, selective, unknown — heuristic from excerpt; unknown if unclear.
7) ai_match_score: integer 0-100 = usefulness/quality for a typical student seeker (NOT admission odds). Base it on excerpt strength + your judgment, but MUST align with rule_score_hint (within ~25 points unless excerpt clearly warrants otherwise).
8) ai_match_band: low (<45), medium (45-69), high (>=70) — must match ai_match_score.
9) confidence_score: 0.0-1.0 how well the excerpt supports your outputs.
10) seo_faq: 2-5 items; each answer must only use facts present in excerpt or honestly say the listing does not state X.
11) The excerpt may contain long-form content in description_html and detailed requirements/essay prompts in requirements_text or raw_data_preview. Use them. If an essay prompt or word-count guidance is present, summarize the essay theme in eligibility_summary or important_checks and include practical application_tips tied to that prompt without inventing extra requirements.

Return a single JSON object with exactly these keys (arrays may be empty):
{
  "student_summary": string,
  "best_for": string[],
  "key_highlights": string[],
  "eligibility_summary": string[],
  "important_checks": string[],
  "application_tips": string[],
  "why_apply": string[],
  "red_flags": string[],
  "missing_info": string[],
  "urgency_level": "low"|"medium"|"high"|"urgent"|"unknown",
  "difficulty_level": "easy"|"moderate"|"selective"|"unknown",
  "ai_match_score": number,
  "ai_match_band": "low"|"medium"|"high",
  "score_explanation_short": string,
  "seo_excerpt": string|null,
  "seo_overview": string|null,
  "seo_eligibility": string|null,
  "seo_application": string|null,
  "seo_faq": [{"q": string, "a": string}],
  "confidence_score": number|null
}
Output JSON only, no markdown."""


def _s(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


_AI_REUSE_FIELDS: tuple[str, ...] = (
    "ai_student_summary",
    "ai_best_for",
    "ai_key_highlights",
    "ai_eligibility_summary",
    "ai_important_checks",
    "ai_application_tips",
    "ai_why_apply",
    "ai_red_flags",
    "ai_missing_info",
    "ai_urgency_level",
    "ai_difficulty_level",
    "ai_match_score",
    "ai_match_band",
    "ai_score_explanation",
    "ai_confidence_score",
    "seo_excerpt",
    "seo_overview",
    "seo_eligibility",
    "seo_application",
    "seo_faq",
)


def _coerce_str_list(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    if isinstance(val, str) and val.strip():
        return [val.strip()]
    return []


def _coerce_faq(val: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not isinstance(val, list):
        return out
    for item in val:
        if not isinstance(item, dict):
            continue
        q = _s(item.get("q"))
        a = _s(item.get("a"))
        if q and a:
            out.append({"q": q, "a": a})
    return out


def _parse_deadline_date(record: dict[str, Any]) -> date | None:
    dd = record.get("deadline_date")
    if dd is None:
        return None
    if isinstance(dd, date) and not isinstance(dd, datetime):
        return dd
    s = _s(dd)
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def compute_urgency_level(record: dict[str, Any]) -> str:
    d = _parse_deadline_date(record)
    if d is None:
        return "unknown"
    today = date.today()
    if d < today:
        return "urgent"
    days = (d - today).days
    if days <= 7:
        return "urgent"
    if days <= 14:
        return "high"
    if days <= 30:
        return "medium"
    return "low"


def compute_difficulty_heuristic(record: dict[str, Any]) -> str:
    blob = " ".join(
        [
            _s(record.get("title")),
            _s(record.get("description")),
            _s(record.get("eligibility_text")),
            _s(record.get("requirements_text")),
        ]
    ).lower()
    if re.search(
        r"\b(merit|competitive|highly\s+selective|national\s+competition)\b",
        blob,
    ):
        return "selective"
    if re.search(
        r"\b(local|single\s+school|one\s+college|employees\s+only|county)\b",
        blob,
    ):
        return "moderate"
    if len(blob) > 200 and not re.search(r"\b(only|restricted|must\s+be)\b", blob):
        return "easy"
    return "unknown"


def compute_rule_based_score(record: dict[str, Any]) -> tuple[int, str, dict[str, Any]]:
    """
    Детерминированный 0–100: funding, deadline runway, eligibility text, completeness, provider.
    Возвращает (score, короткое объяснение, компоненты для raw_data).
    """
    components: dict[str, Any] = {}
    score = 0

    # Funding 0–28
    amin = record.get("award_amount_min")
    amax = record.get("award_amount_max")
    mx = None
    for x in (amin, amax):
        if isinstance(x, (int, float)) and x == x and x > 0:
            mx = max(mx or 0.0, float(x))
    funding_pts = 0
    if mx is not None:
        if mx >= 20000:
            funding_pts = 28
        elif mx >= 10000:
            funding_pts = 24
        elif mx >= 5000:
            funding_pts = 20
        elif mx >= 1000:
            funding_pts = 14
        else:
            funding_pts = 8
    elif _s(record.get("award_amount_text")) and re.search(
        r"[\$£€¥]|\d", _s(record.get("award_amount_text"))
    ):
        funding_pts = 10
    score += funding_pts
    components["funding_points"] = funding_pts

    # Deadline health 0–28 (карточки до upsert уже прошли business filters — дедлайн ок)
    d = _parse_deadline_date(record)
    dl_pts = 0
    if d is None:
        dl_pts = 6
    else:
        today = date.today()
        days = (d - today).days
        if days >= 60:
            dl_pts = 28
        elif days >= 30:
            dl_pts = 24
        elif days >= MIN_LEAD_DAYS_BEFORE_DEADLINE + 10:
            dl_pts = 20
        else:
            dl_pts = 14
    score += dl_pts
    components["deadline_points"] = dl_pts

    # Eligibility clarity 0–18
    el = _s(record.get("eligibility_text"))
    el_pts = 0
    if len(el) >= 400:
        el_pts = 18
    elif len(el) >= 200:
        el_pts = 14
    elif len(el) >= 80:
        el_pts = 10
    elif len(el) >= 30:
        el_pts = 6
    score += el_pts
    components["eligibility_points"] = el_pts

    # Data completeness 0–16
    keys = (
        "title",
        "provider_name",
        "apply_url",
        "description",
        "requirements_text",
        "payout_method",
        "winner_payment_text",
    )
    filled = sum(1 for k in keys if _s(record.get(k)))
    comp_pts = min(16, filled * 2)
    score += comp_pts
    components["completeness_points"] = comp_pts

    # Provider 0–10
    pr_pts = 0
    if _s(record.get("provider_name")):
        pr_pts += 5
    if _s(record.get("provider_url")) or _s(record.get("institutions_text")):
        pr_pts += 5
    score += pr_pts
    components["provider_points"] = pr_pts

    score = max(0, min(100, int(score)))
    expl = (
        f"Rule-based mix: funding={funding_pts}, deadline={dl_pts}, "
        f"eligibility_clarity={el_pts}, fields={comp_pts}, provider={pr_pts}."
    )
    return score, expl, components


def _band_from_score(s: int) -> str:
    if s >= 70:
        return "high"
    if s >= 45:
        return "medium"
    return "low"


def _blend_scores(rule_score: int, model_score: float | None, *, use_model: bool) -> int:
    if model_score is None or not use_model:
        return max(0, min(100, rule_score))
    try:
        m = float(model_score)
        if m != m:
            return rule_score
        m = max(0.0, min(100.0, m))
    except (TypeError, ValueError):
        return rule_score
    blended = int(round(0.35 * rule_score + 0.65 * m))
    return max(0, min(100, blended))


def _build_excerpt_payload(record: dict[str, Any], max_chars: int) -> dict[str, Any]:
    """Компактный JSON для модели — только поля из записи."""
    raw = record.get("raw_data")
    raw_preview: Any = None
    if isinstance(raw, dict):
        raw_preview = {
            k: raw[k]
            for k in list(raw.keys())[:12]
            if not str(k).startswith("_")
        }
    payload = {
        "source": record.get("source"),
        "source_id": record.get("source_id"),
        "title": record.get("title"),
        "provider_name": record.get("provider_name"),
        "provider_url": record.get("provider_url"),
        "provider_mission": record.get("provider_mission"),
        "url": record.get("url"),
        "apply_url": record.get("apply_url"),
        "award_amount_text": record.get("award_amount_text"),
        "award_amount_min": record.get("award_amount_min"),
        "award_amount_max": record.get("award_amount_max"),
        "deadline_text": record.get("deadline_text"),
        "deadline_date": record.get("deadline_date"),
        "currency": record.get("currency"),
        "description": record.get("description"),
        "description_html": record.get("description_html"),
        "eligibility_text": record.get("eligibility_text"),
        "requirements_text": record.get("requirements_text"),
        "requirements_count": record.get("requirements_count"),
        "awards_text": record.get("awards_text"),
        "winner_payment_text": record.get("winner_payment_text"),
        "payout_method": record.get("payout_method"),
        "payment_details": record.get("payment_details"),
        "institutions_text": record.get("institutions_text"),
        "state_territory_text": record.get("state_territory_text"),
        "application_status_text": record.get("application_status_text"),
        "category": record.get("category"),
        "tags": record.get("tags"),
        "study_levels": record.get("study_levels"),
        "number_of_awards": record.get("number_of_awards"),
        "applicants_count": record.get("applicants_count"),
        "summary_short": record.get("summary_short"),
        "summary_long": record.get("summary_long"),
        "who_can_apply": record.get("who_can_apply"),
        "raw_data_preview": raw_preview,
    }
    long_keys = (
        "description",
        "description_html",
        "eligibility_text",
        "requirements_text",
        "summary_long",
        "who_can_apply",
    )
    for cap in (8000, 4000, 2000, 1200, 800, 500):
        for k in long_keys:
            v = payload.get(k)
            if isinstance(v, str) and len(v) > cap:
                payload[k] = v[:cap] + "…"
        if len(json.dumps(payload, ensure_ascii=False, default=str)) <= max_chars:
            return payload
    return payload


def _stable_hashable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = _s(value)
        return cleaned or None
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, list):
        out = [_stable_hashable(v) for v in value]
        return [v for v in out if v not in (None, "", [], {})]
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key in sorted(value.keys()):
            normalized = _stable_hashable(value.get(key))
            if normalized in (None, "", [], {}):
                continue
            out[str(key)] = normalized
        return out
    return _s(value) or None


def build_ai_content_hash(record: dict[str, Any]) -> str:
    """
    Stable hash for AI Final reuse.

    Intentionally excludes transport / gating fields like apply_url/provider_url so values such as
    "LOCKED" do not trigger a false re-run. Dedup text_fingerprint in utils stays unchanged.
    """
    payload = {
        "title": record.get("title"),
        "provider_name": record.get("provider_name"),
        "award_amount_text": record.get("award_amount_text"),
        "deadline_text": record.get("deadline_text"),
        "description": record.get("description"),
        "description_html": record.get("description_html"),
        "eligibility_text": record.get("eligibility_text"),
        "requirements_text": record.get("requirements_text"),
        "awards_text": record.get("awards_text"),
        "winner_payment_text": record.get("winner_payment_text"),
        "payout_method": record.get("payout_method"),
        "payment_details": record.get("payment_details"),
        "institutions_text": record.get("institutions_text"),
        "state_territory_text": record.get("state_territory_text"),
        "application_status_text": record.get("application_status_text"),
        "category": record.get("category"),
        "tags": record.get("tags"),
        "study_levels": record.get("study_levels"),
        "number_of_awards": record.get("number_of_awards"),
        "applicants_count": record.get("applicants_count"),
        "summary_short": record.get("summary_short"),
        "summary_long": record.get("summary_long"),
        "who_can_apply": record.get("who_can_apply"),
    }
    stable = _stable_hashable(payload)
    blob = json.dumps(stable, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


def _existing_ai_content_hash(existing_row: dict[str, Any] | None) -> str | None:
    if not isinstance(existing_row, dict):
        return None
    hash_value = _s(existing_row.get("ai_content_hash"))
    return hash_value or None


def _has_reusable_ai_fields(existing_row: dict[str, Any] | None) -> bool:
    if not isinstance(existing_row, dict):
        return False
    return any(existing_row.get(key) not in (None, "", []) for key in _AI_REUSE_FIELDS)


def _copy_ai_fields_from_existing(out: dict[str, Any], existing_row: dict[str, Any]) -> None:
    for key in _AI_REUSE_FIELDS:
        if key in existing_row:
            out[key] = existing_row.get(key)


def _reuse_existing_ai_fields(
    out: dict[str, Any],
    existing_row: dict[str, Any],
    ai_content_hash: str,
) -> dict[str, Any]:
    _copy_ai_fields_from_existing(out, existing_row)
    out["ai_content_hash"] = ai_content_hash
    record_ai_reuse()
    _merge_raw_finalization(
        out,
        {
            "version": 1,
            "mode": "reused_existing",
            "reused_existing_id": existing_row.get("id"),
        },
    )
    return out

def _apply_rule_only_fallback(
    record: dict[str, Any],
    rule_score: int,
    rule_expl: str,
    components: dict[str, Any],
) -> None:
    record["ai_content_hash"] = build_ai_content_hash(record)
    urg = compute_urgency_level(record)
    diff = compute_difficulty_heuristic(record)
    record["ai_urgency_level"] = urg
    record["ai_difficulty_level"] = diff
    record["ai_match_score"] = rule_score
    record["ai_match_band"] = _band_from_score(rule_score)
    record["ai_score_explanation"] = rule_expl
    record["ai_confidence_score"] = 0.35
    record["ai_student_summary"] = _s(record.get("summary_short")) or (
        (_s(record.get("description"))[:400] + "…")
        if len(_s(record.get("description"))) > 400
        else _s(record.get("description"))
    ) or None
    for k in (
        "ai_best_for",
        "ai_key_highlights",
        "ai_eligibility_summary",
        "ai_important_checks",
        "ai_application_tips",
        "ai_why_apply",
        "ai_red_flags",
        "ai_missing_info",
    ):
        record[k] = []
    record["seo_excerpt"] = None
    record["seo_overview"] = None
    record["seo_eligibility"] = None
    record["seo_application"] = None
    record["seo_faq"] = []
    _merge_raw_finalization(
        record,
        {
            "version": 1,
            "mode": "rule_fallback",
            "rule_score": rule_score,
            "rule_components": components,
        },
    )


def _merge_raw_finalization(record: dict[str, Any], meta: dict[str, Any]) -> None:
    rd = record.get("raw_data")
    if not isinstance(rd, dict):
        rd = {}
    else:
        rd = dict(rd)
    prev = rd.get("ai_finalization")
    if isinstance(prev, dict):
        prev = {k: v for k, v in prev.items() if k != "ai_content_hash"}
        meta = {**prev, **meta}
    meta = {k: v for k, v in meta.items() if k != "ai_content_hash"}
    rd["ai_finalization"] = json_safe(meta)
    record["raw_data"] = rd


def _parse_model_json(text: str) -> dict[str, Any] | None:
    t = (text or "").strip()
    if not t:
        return None
    try:
        out = json.loads(t)
        return out if isinstance(out, dict) else None
    except json.JSONDecodeError:
        return None


def apply_scholarship_ai_finalization_if_enabled(
    record: dict[str, Any],
    *,
    existing_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Если SCHOLARSHIP_AI_FINAL_ENABLED и есть OPENAI_API_KEY — дополняет record полями ai_* / seo_*.
    Иначе возвращает record без изменений (кроме случая отсутствия ключа: без изменений).
    """
    cfg = get_scholarships_ai_final_config()
    out = dict(record)
    if not cfg.enabled:
        return out

    rule_score, rule_expl, components = compute_rule_based_score(out)
    ai_content_hash = build_ai_content_hash(out)
    out["ai_content_hash"] = ai_content_hash
    existing_hash = _existing_ai_content_hash(existing_row)
    if (
        existing_hash
        and existing_hash == ai_content_hash
        and _has_reusable_ai_fields(existing_row)
    ):
        print(
            "[scholarship_ai_final] unchanged content; reuse existing AI "
            f"(row_id={existing_row.get('id')})"
        )
        return _reuse_existing_ai_fields(out, existing_row, ai_content_hash)

    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        print("[scholarship_ai_final] skipped: OPENAI_API_KEY missing")
        _apply_rule_only_fallback(out, rule_score, rule_expl, components)
        return out

    excerpt = _build_excerpt_payload(out, cfg.max_input_chars)
    user_msg = (
        "Scholarship record excerpt (facts for this listing only). "
        f"rule_score_hint={rule_score}. rule_hint_explanation={rule_expl!s}\n"
        f"code_urgency_hint={compute_urgency_level(out)!r}\n"
        f"code_difficulty_hint={compute_difficulty_heuristic(out)!r}\n"
        "Respond with the required JSON.\n"
        f"{json.dumps(excerpt, ensure_ascii=False, default=str)}"
    )
    if not cfg.write_seo:
        user_msg += (
            "\nSet seo_excerpt, seo_overview, seo_eligibility, seo_application to null "
            "and seo_faq to []."
        )
    if not cfg.write_guidance:
        user_msg += (
            "\nSet application_tips, why_apply, best_for, key_highlights to [] "
            "(empty). Still fill student_summary, eligibility_summary, important_checks, "
            "red_flags, missing_info if grounded in excerpt."
        )

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        completion = client.chat.completions.create(
            model=cfg.model,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _JSON_SYSTEM},
                {"role": "user", "content": user_msg[: cfg.max_input_chars + 2000]},
            ],
        )
        record_ai_completion(completion.usage)
        text = (completion.choices[0].message.content or "").strip()
    except Exception as e:
        record_ai_error()
        print(f"[scholarship_ai_final] API error: {type(e).__name__}: {e}")
        _apply_rule_only_fallback(out, rule_score, rule_expl, components)
        return out

    parsed = _parse_model_json(text)
    if not parsed:
        print("[scholarship_ai_final] invalid JSON from model; rule fallback")
        _apply_rule_only_fallback(out, rule_score, rule_expl, components)
        return out

    try:
        ms = parsed.get("ai_match_score", parsed.get("match_score"))
        model_score: float | None = float(ms) if ms is not None else None
    except (TypeError, ValueError):
        model_score = None

    final_score = _blend_scores(
        rule_score,
        model_score,
        use_model=cfg.write_score_from_model,
    )
    band = _band_from_score(final_score)

    try:
        conf = parsed.get("confidence_score")
        conf_f = float(conf) if conf is not None else None
        if conf_f is not None and (conf_f != conf_f):
            conf_f = None
        if conf_f is not None:
            conf_f = max(0.0, min(1.0, conf_f))
    except (TypeError, ValueError):
        conf_f = None

    out["ai_student_summary"] = _s(parsed.get("student_summary")) or None
    out["ai_best_for"] = _coerce_str_list(parsed.get("best_for"))
    out["ai_key_highlights"] = _coerce_str_list(parsed.get("key_highlights"))
    out["ai_eligibility_summary"] = _coerce_str_list(parsed.get("eligibility_summary"))
    out["ai_important_checks"] = _coerce_str_list(parsed.get("important_checks"))
    out["ai_application_tips"] = (
        _coerce_str_list(parsed.get("application_tips")) if cfg.write_guidance else []
    )
    out["ai_why_apply"] = (
        _coerce_str_list(parsed.get("why_apply")) if cfg.write_guidance else []
    )
    out["ai_red_flags"] = _coerce_str_list(parsed.get("red_flags"))
    out["ai_missing_info"] = _coerce_str_list(parsed.get("missing_info"))

    urg = _s(parsed.get("urgency_level")).lower()
    if urg not in ("low", "medium", "high", "urgent", "unknown"):
        urg = compute_urgency_level(out)
    out["ai_urgency_level"] = urg

    diff = _s(parsed.get("difficulty_level")).lower()
    if diff not in ("easy", "moderate", "selective", "unknown"):
        diff = compute_difficulty_heuristic(out)
    out["ai_difficulty_level"] = diff

    out["ai_match_score"] = final_score
    out["ai_match_band"] = band
    expl = _s(parsed.get("score_explanation_short", parsed.get("score_explanation")))
    out["ai_score_explanation"] = expl or rule_expl
    out["ai_confidence_score"] = conf_f

    if cfg.write_seo:
        out["seo_excerpt"] = _s(parsed.get("seo_excerpt")) or None
        out["seo_overview"] = _s(parsed.get("seo_overview")) or None
        out["seo_eligibility"] = _s(parsed.get("seo_eligibility")) or None
        out["seo_application"] = _s(parsed.get("seo_application")) or None
        out["seo_faq"] = _coerce_faq(parsed.get("seo_faq"))
    else:
        out["seo_excerpt"] = None
        out["seo_overview"] = None
        out["seo_eligibility"] = None
        out["seo_application"] = None
        out["seo_faq"] = []

    if not cfg.write_guidance:
        out["ai_best_for"] = []
        out["ai_key_highlights"] = []

    _merge_raw_finalization(
        out,
        {
            "version": 1,
            "mode": "openai",
            "model": cfg.model,
            "rule_score": rule_score,
            "rule_components": components,
            "blended_match_score": final_score,
        },
    )
    return out
