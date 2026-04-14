"""
Глобальные сигналы «высокоценной» награды без явной суммы в цифрах.

Используются: business_filters, BigFuture prefilter, shared_scholarship_ai (скоринг),
normalize_scholarship (теги). Источники парсеров подключают правила через
has_meaningful_funding и утилиты ниже.
"""

from __future__ import annotations

import os
import re
from typing import Any, Mapping, MutableMapping

HIGH_VALUE_AWARD_PHRASES: tuple[str, ...] = (
    "Full ride",
    "Full tuition",
    "Full cost of attendance",
    "Full scholarship",
    "Tuition waiver",
    "Tuition cover",
    "Tuition-free",
    "100% of tuition",
    "Covers full tuition",
    "Covers full cost",
    "Stipend",
    "All expenses covered",
    "Housing included",
    "Value varies",
    "Fellowship award",
)

# Единый паттерн для фильтров, prefilter и детектора non_monetary (title + суммы + awards).
HIGH_VALUE_AWARD_PATTERN: re.Pattern[str] = re.compile(
    r"\b("
    r"full[\s-]+ride|full\s+tuition|full\s+cost\s+of\s+attendance|full\s+scholarship|"
    r"tuition\s+waiver|tuition\s+cover|tuition[-\s]free|"
    r"100\s*%\s*(?:of\s+)?tuition|"
    r"covers\s+full\s+tuition|covers\s+full\s+cost|covers\s+full\s+cost\s+of\s+attendance|"
    r"stipend|all\s+expenses\s+covered|housing\s+included|value\s+varies|"
    r"fellowship\s+award"
    r")\b",
    re.I,
)

# Явные деньги в строке суммы из источника: $/£/€/¥ + число или суммы с тысячными запятыми.
_PRIMARY_OBVIOUS_MONEY: re.Pattern[str] = re.compile(
    r"[\$£€¥]\s*[\d,.]+",
    re.I,
)
_THOUSANDS_COMMA_AMOUNT: re.Pattern[str] = re.compile(
    r"\b\d{1,3}(,\d{3})+(\.\d{2})?\b",
)

SEO_TAG_HIGH_VALUE: str = "award_signal_high_value"
# Должен совпадать с AWARD_SIGNAL_SEO_TAGS на сайте (PostgREST seo_tags.ov.{...}).

# Карточка каталога: когда нет min/max и нет high-value формулировки в тексте.
DEFAULT_AWARD_AMOUNT_TEXT_UNKNOWN: str = "Amount Varies"

HIGH_VALUE_AWARD_TAG_SPECS: tuple[tuple[str, str], ...] = (
    (r"\bfull[\s-]+ride\b", "award_signal_full_ride"),
    (r"\bfull\s+tuition\b", "award_signal_full_tuition"),
    (r"\bfull\s+cost\s+of\s+attendance\b", "award_signal_full_coa"),
    (r"\bfull\s+scholarship\b", "award_signal_full_scholarship"),
    (r"\btuition\s+waiver\b", "award_signal_tuition_waiver"),
    (r"\btuition\s+cover\b", "award_signal_tuition_cover"),
    (r"\btuition[-\s]free\b", "award_signal_tuition_free"),
    (r"\b100\s*%\s*(?:of\s+)?tuition\b", "award_signal_100pct_tuition"),
    (r"\bcovers\s+full\s+tuition\b", "award_signal_covers_full_tuition"),
    (r"\bcovers\s+full\s+cost\b", "award_signal_covers_full_cost"),
    (r"\bstipend\b", "award_signal_stipend"),
    (r"\ball\s+expenses\s+covered\b", "award_signal_all_expenses_covered"),
    (r"\bhousing\s+included\b", "award_signal_housing_included"),
    (r"\bvalue\s+varies\b", "award_signal_value_varies"),
    (r"\bfellowship\s+award\b", "award_signal_fellowship_award"),
)

_HIGH_VALUE_TAG_COMPILED: tuple[tuple[re.Pattern[str], str], ...] = tuple(
    (re.compile(pat, re.I), slug) for pat, slug in HIGH_VALUE_AWARD_TAG_SPECS
)

_AUTHORITY_PROVIDER_CORE: re.Pattern[str] = re.compile(
    r"\b("
    r"university|college|foundation|institute|institution|academy|"
    r"federation|consortium|endowment|association"
    r")\b",
    re.I,
)

_extra_authority_pattern: re.Pattern[str] | None = None


def _get_extra_authority_pattern() -> re.Pattern[str] | None:
    global _extra_authority_pattern
    raw = (os.getenv("AUTHORITY_PROVIDER_EXTRA_REGEX") or "").strip()
    if not raw:
        return None
    if _extra_authority_pattern is None or getattr(
        _extra_authority_pattern, "_raw", ""
    ) != raw:
        try:
            _extra_authority_pattern = re.compile(raw, re.I)
            setattr(_extra_authority_pattern, "_raw", raw)
        except re.error:
            return None
    return _extra_authority_pattern


def primary_award_amount_text_has_obvious_money(s: str | None) -> bool:
    """
    Есть ли в основной строке суммы (award_amount_text) явная денежная величина:
    валютный символ + число или формат с тысячными запятыми (1,000 …).
    Не считает «100%», год в заголовке и пр. — только поле суммы.
    """
    if not s or not str(s).strip():
        return False
    t = str(s).strip()
    if _PRIMARY_OBVIOUS_MONEY.search(t):
        return True
    if _THOUSANDS_COMMA_AMOUNT.search(t):
        return True
    return False


def is_non_monetary_high_value_award(record: Mapping[str, Any]) -> bool:
    """
    High-value формулировка в title + award_amount_text + awards_text, но в award_amount_text
    нет осмысленной денежной строки — для payout_method=non_monetary и seo_tags.
    """
    if primary_award_amount_text_has_obvious_money(record.get("award_amount_text")):
        return False
    blob = " ".join(
        str(record.get(k) or "")
        for k in ("title", "award_amount_text", "awards_text")
    )
    if not blob.strip():
        return False
    return bool(HIGH_VALUE_AWARD_PATTERN.search(blob))


def text_has_high_value_award_signal(text: str | None) -> bool:
    if not text or not str(text).strip():
        return False
    return bool(HIGH_VALUE_AWARD_PATTERN.search(text))


def extract_high_value_display_phrase(record: Mapping[str, Any]) -> str | None:
    """
    Первая совпавшая с HIGH_VALUE_AWARD_PATTERN подстрока из title / award_amount_text / awards_text
    для отображения в award_amount_text (без выдумывания фактов — только совпадение с паттерном).
    """
    blob = " ".join(
        str(record.get(k) or "")
        for k in ("title", "award_amount_text", "awards_text")
    )
    if not blob.strip():
        return None
    m = HIGH_VALUE_AWARD_PATTERN.search(blob)
    if not m:
        return None
    return m.group(0).strip()


def record_funding_language_blob(record: dict[str, Any]) -> str:
    parts: list[str] = []
    for k in (
        "title",
        "award_amount_text",
        "awards_text",
        "eligibility_text",
        "winner_payment_text",
    ):
        v = record.get(k)
        if v:
            parts.append(str(v))
    desc = record.get("description")
    if desc:
        parts.append(str(desc)[:4000])
    return " ".join(parts)


def is_authoritative_provider_hint(record: dict[str, Any]) -> bool:
    if bool(record.get("is_verified")):
        return True
    blob = " ".join(
        str(record.get(k) or "").strip()
        for k in (
            "provider_name",
            "institutions_text",
            "provider_mission",
            "official_source_name",
        )
        if record.get(k)
    )
    if not blob.strip():
        return False
    if _AUTHORITY_PROVIDER_CORE.search(blob):
        return True
    extra = _get_extra_authority_pattern()
    if extra and extra.search(blob):
        return True
    return False


def infer_high_value_award_tags(text: str | None) -> list[str]:
    if not text or not str(text).strip():
        return []
    out: list[str] = []
    seen: set[str] = set()
    for pat, slug in _HIGH_VALUE_TAG_COMPILED:
        if pat.search(text) and slug not in seen:
            seen.add(slug)
            out.append(slug)
    return out


def _numeric_sort_is_set(record: Mapping[str, Any]) -> bool:
    n = record.get("award_amount_numeric_sort")
    if n is None:
        return False
    if isinstance(n, float) and n != n:
        return False
    return True


def ensure_catalog_listing_or(record: MutableMapping[str, Any]) -> None:
    """
    Хаб (applyMoreFilters): OR по сумме закрывается через non_monetary, numeric_sort в
    диапазоне слайдера, или seo_tags overlap с AWARD_SIGNAL_SEO_TAGS (на сайте).

    Если иначе строка отсечётся — добавляем award_signal_high_value (канонический тег из
    whitelist), а не произвольный award_signal_*: PostgREST фильтрует по фиксированному
    набору (award_signal_high_value и др.), см. vocabulary на фронте.
    """
    if _numeric_sort_is_set(record):
        return
    pay = (record.get("payout_method") or "").strip()
    if pay == "non_monetary":
        return
    raw = record.get("seo_tags")
    seo: list[str] = []
    if isinstance(raw, list):
        seo = [str(x).strip() for x in raw if isinstance(x, str) and str(x).strip()]
    seo_lc = {t.lower() for t in seo}
    if SEO_TAG_HIGH_VALUE.lower() in seo_lc:
        return
    seo.append(SEO_TAG_HIGH_VALUE)
    record["seo_tags"] = seo
