"""
Человекочитаемые даты для summary / ai_student_summary (англ. формат как в UI).

Убирает сырые ISO-строки вида 2026-07-09T23:59:59Z из текста и полей дедлайна.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

_MONTHS_EN = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)

# Полная ISO-дата/время или только дата (граница слова слева)
_RE_ISO_FRAGMENT = re.compile(
    r"(?<![\d-])(\d{4}-\d{2}-\d{2})(?:T[\d:.\-+]+Z?)?(?![\d-])"
)


def format_us_long_date(d: date) -> str:
    """Например: July 9, 2026."""
    return f"{_MONTHS_EN[d.month - 1]} {d.day}, {d.year}"


def parse_loose_deadline(value: Any) -> date | None:
    """Дата из deadline_date, ISO-строки или datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _looks_like_raw_iso_deadline_text(s: str) -> bool:
    """Только машинная дата/время без пояснений вроде «rolling»."""
    s = s.strip()
    if len(s) < 10:
        return False
    if s[4] != "-" or s[7] != "-":
        return False
    if not s[:10].replace("-", "").isdigit():
        return False
    rest = s[10:]
    if not rest:
        return True
    return rest[0] in "Tt "


def deadline_display_for_card(record: dict[str, Any]) -> str | None:
    """
    Строка для «Plan to apply by …» — без сырого ISO, в стиле July 9, 2026.
    Если deadline_text уже человекочитаемый — возвращаем его.
    """
    dl = (record.get("deadline_text") or "").strip()
    dd = record.get("deadline_date")

    if dl and not _looks_like_raw_iso_deadline_text(dl):
        return dl.rstrip(".")

    d = parse_loose_deadline(dd)
    if d is None and dl:
        d = parse_loose_deadline(dl)
    if d is not None:
        return format_us_long_date(d)
    if dl:
        return dl.rstrip(".")
    return None


def humanize_iso_datetimes_in_text(text: str | None) -> str | None:
    """
    Заменяет вхождения YYYY-MM-DD и YYYY-MM-DDTHH:MM…Z на July 9, 2026.
    Не трогает нераспознанные фрагменты.
    """
    if text is None:
        return None
    if not text.strip():
        return text

    def _repl(m: re.Match[str]) -> str:
        raw = m.group(0)
        d = parse_loose_deadline(raw)
        if d is None:
            return raw
        return format_us_long_date(d)

    return _RE_ISO_FRAGMENT.sub(_repl, text)
