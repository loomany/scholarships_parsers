"""
Общие бизнес-правила перед upsert в парсерах каталога стипендий.

Не трогает БД и UI; только импортируется из sources.*.
"""

from __future__ import annotations

import os
import re
from datetime import date, timedelta
from typing import Any, Literal

DeadlineBiz = Literal["ok", "no_deadline", "expired", "too_close"]

def _get_min_lead_days() -> int:
    raw = (os.getenv("MIN_LEAD_DAYS", "3") or "3").strip()
    try:
        v = int(raw)
    except ValueError:
        v = 3
    return max(0, v)


# Дедлайн должен быть не раньше (сегодня + N календарных дней).
MIN_LEAD_DAYS = _get_min_lead_days()
# Backward-compatible alias (импортируется в parser'ах).
MIN_LEAD_DAYS_BEFORE_DEADLINE = MIN_LEAD_DAYS

_FUNDING_VOID = re.compile(
    r"^\s*(n/?a|tbd|none|—|-|not\s+specified)\s*$",
    re.I,
)


def _coerce_deadline_to_date(deadline_date: Any) -> date | None:
    if deadline_date is None:
        return None
    if isinstance(deadline_date, date):
        return deadline_date
    s = str(deadline_date).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def classify_business_deadline(deadline_date: Any) -> DeadlineBiz:
    """
    ok — дата есть и >= today + MIN_LEAD_DAYS.
    no_deadline — нет или не распарсилась.
    expired — строго до сегодня.
    too_close — сегодня .. today+N-1 включительно.
    """
    d = _coerce_deadline_to_date(deadline_date)
    if d is None:
        return "no_deadline"
    today = date.today()
    if d < today:
        return "expired"
    earliest_ok = today + timedelta(days=MIN_LEAD_DAYS)
    if d < earliest_ok:
        return "too_close"
    return "ok"


def is_valid_deadline(deadline_date: Any) -> bool:
    return classify_business_deadline(deadline_date) == "ok"


def has_meaningful_funding(record: dict[str, Any]) -> bool:
    """
    Есть признаки суммы/выплаты: положительные min/max, текст с суммой,
    блок awards / winner payment после нормализации.
    """
    amin = record.get("award_amount_min")
    amax = record.get("award_amount_max")
    for v in (amin, amax):
        if v is not None and isinstance(v, (int, float)):
            if v != v:  # NaN
                continue
            if v > 0:
                return True

    at = (record.get("award_amount_text") or "").strip()
    if at and not _FUNDING_VOID.match(at):
        if re.search(r"[\$£€¥]|\d", at):
            return True
        if re.search(
            r"\b(full[\s-]+ride|full\s+tuition|tuition\s+cover|"
            r"stipend|fellowship\s+award)\b",
            at,
            re.I,
        ):
            return True

    awt = (record.get("awards_text") or "").strip()
    if len(awt) >= 12 and re.search(
        r"[\$£€¥]|\d|\b(funding|award|stipend|grant|payment|remuneration)\b",
        awt,
        re.I,
    ):
        return True

    wpt = (
        (record.get("winner_payment_text") or "")
        or (record.get("payment_details") or "")
    ).strip()
    if len(wpt) >= 10:
        return True

    return False
