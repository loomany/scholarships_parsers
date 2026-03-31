"""
Дешёвый fast-prefilter и JSON-store для двухфазного BigFuture pipeline.

Статусы (строки):
  prefilter_pass, prefilter_reject_deadline, prefilter_reject_funding,
  prefilter_reject_relevance, prefilter_review
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import date, datetime, timezone
from typing import Any

from business_filters import classify_business_deadline
from sources.scholarship_america.parser import parse_award_min_max, parse_deadline_date

PREFILTER_PASS = "prefilter_pass"
PREFILTER_REJECT_DEADLINE = "prefilter_reject_deadline"
PREFILTER_REJECT_FUNDING = "prefilter_reject_funding"
PREFILTER_REJECT_RELEVANCE = "prefilter_reject_relevance"
PREFILTER_REVIEW = "prefilter_review"

_STORE_VERSION = 1

_BF_HARD_SKIP: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("faculty_only", re.compile(r"\b(?:faculty|staff)\s+only\b", re.I)),
    ("employees_only", re.compile(r"\bemployees only\b", re.I)),
    ("employee_assistance", re.compile(r"\bemployee assistance program\b", re.I)),
)

_FUNDING_SOFT_OK = re.compile(
    r"\b(full[\s-]+ride|full\s+tuition|tuition\s+cover|"
    r"stipend|fellowship\s+award)\b",
    re.I,
)


def _award_text_from_api(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        if raw != raw:
            return None
        n = float(raw)
        if n.is_integer():
            return f"${int(n):,}"
        return f"${n:,.2f}"
    t = str(raw).strip()
    return t or None


def _list_relevance_blob_lc(card_row: dict[str, Any]) -> str:
    le = card_row.get("_list_extra") if isinstance(card_row.get("_list_extra"), dict) else {}
    raw = le.get("raw_list_card") if isinstance(le, dict) else None
    raw_blob = json.dumps(raw, ensure_ascii=False) if isinstance(raw, dict) else ""
    parts = [
        str(card_row.get("title") or ""),
        str(le.get("snippet") or ""),
        raw_blob,
    ]
    return " ".join(parts).lower()


def _close_date_str_from_card_row(card_row: dict[str, Any]) -> str | None:
    le = card_row.get("_list_extra") if isinstance(card_row.get("_list_extra"), dict) else {}
    raw = le.get("raw_list_card") if isinstance(le, dict) else None
    if not isinstance(raw, dict):
        return None
    cd = raw.get("closeDate")
    if cd is None:
        return None
    s = str(cd).strip()
    return s or None


def classify_fast_prefilter(
    card_row: dict[str, Any],
    *,
    min_amount_hint: int,
) -> tuple[str, str, str | None, str | None]:
    """
    Только поля листинга. Возвращает:
      (prefilter_status, prefilter_reason, amount_hint, close_date)
    """
    blob = _list_relevance_blob_lc(card_row)
    hits = [lbl for lbl, pat in _BF_HARD_SKIP if pat.search(blob)]
    if hits:
        return (
            PREFILTER_REJECT_RELEVANCE,
            f"hard_negative:{','.join(hits)}",
            None,
            _close_date_str_from_card_row(card_row),
        )

    close_s = _close_date_str_from_card_row(card_row)
    d_iso = parse_deadline_date(close_s) if close_s else None
    dbiz = classify_business_deadline(d_iso)
    if dbiz == "expired":
        return (
            PREFILTER_REJECT_DEADLINE,
            "deadline_expired",
            None,
            close_s,
        )
    if dbiz == "too_close":
        return (
            PREFILTER_REJECT_DEADLINE,
            "deadline_too_close",
            None,
            close_s,
        )

    le = card_row.get("_list_extra") if isinstance(card_row.get("_list_extra"), dict) else {}
    raw = le.get("raw_list_card") if isinstance(le, dict) else None
    raw = raw if isinstance(raw, dict) else {}
    award_t = _award_text_from_api(raw.get("scholarshipMaximumAward"))
    blurb = str(raw.get("blurb") or "")
    combined = f"{award_t or ''} {blurb}".strip()
    amin, amax = parse_award_min_max(combined if combined else None)
    mx: float | None = None
    for x in (amin, amax):
        if x is not None and (mx is None or x > mx):
            mx = float(x)

    amount_hint = award_t or (f"${int(mx):,}" if mx is not None else None)

    if mx is not None:
        if mx < float(min_amount_hint):
            return (
                PREFILTER_REJECT_FUNDING,
                f"below_min_amount_hint(<{min_amount_hint})",
                amount_hint,
                close_s,
            )
    else:
        if re.search(r"[\$£€¥]", combined) and re.search(r"\d", combined):
            pass  # ambiguous → review below
        elif _FUNDING_SOFT_OK.search(combined):
            pass  # treat as enough signal to not reject funding
        else:
            return (
                PREFILTER_REJECT_FUNDING,
                "no_amount_hint",
                amount_hint,
                close_s,
            )

    if dbiz == "no_deadline":
        return (
            PREFILTER_REVIEW,
            "no_parsed_deadline",
            amount_hint,
            close_s,
        )

    if mx is None and re.search(r"[\$£€¥]", combined) and re.search(r"\d", combined):
        return (
            PREFILTER_REVIEW,
            "ambiguous_funding_text",
            amount_hint,
            close_s,
        )

    return PREFILTER_PASS, "", amount_hint, close_s


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_dt(s: Any) -> datetime | None:
    if not s or not isinstance(s, str):
        return None
    t = s.strip()
    if not t:
        return None
    try:
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        return datetime.fromisoformat(t)
    except ValueError:
        return None


def entry_eligible_for_deep_pass(
    entry: dict[str, Any],
    *,
    now_utc: datetime,
    recheck_reject_days: int,
    include_review: bool,
) -> bool:
    """Только для режима DEEP_PASS_ONLY: брать из store."""
    st = str(entry.get("prefilter_status") or "")
    if st == PREFILTER_PASS:
        return True
    if include_review and st == PREFILTER_REVIEW:
        return True
    if st == PREFILTER_REJECT_DEADLINE:
        return False
    if st in (PREFILTER_REJECT_FUNDING, PREFILTER_REJECT_RELEVANCE):
        if recheck_reject_days <= 0:
            return False
        ls = _parse_iso_dt(entry.get("last_seen_at"))
        if ls is None:
            return True
        ls_utc = ls if ls.tzinfo else ls.replace(tzinfo=timezone.utc)
        delta = now_utc - ls_utc
        return delta.days >= recheck_reject_days
    return False


class BigFuturePrefilterStore:
    """Локальный JSON-файл: entries[source_id] = { ... }."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.entries: dict[str, dict[str, Any]] = {}

    def load(self) -> None:
        if not self.path or not os.path.isfile(self.path):
            self.entries = {}
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            self.entries = {}
            return
        if not isinstance(data, dict):
            self.entries = {}
            return
        ent = data.get("entries")
        self.entries = ent if isinstance(ent, dict) else {}

    def save(self) -> None:
        if not self.path:
            return
        parent = os.path.dirname(os.path.abspath(self.path))
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        payload = {"version": _STORE_VERSION, "entries": self.entries}
        fd, tmp = tempfile.mkstemp(
            suffix=".json",
            dir=parent or None,
            text=True,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
        except OSError:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def upsert_from_card_row(
        self,
        card_row: dict[str, Any],
        *,
        prefilter_status: str,
        prefilter_reason: str,
        amount_hint: str | None,
        close_date: str | None,
    ) -> None:
        sid = str(card_row.get("source_id") or "").strip()
        if not sid:
            return
        now = _utc_now_iso()
        self.entries[sid] = {
            "source": "bigfuture",
            "source_id": sid,
            "url": str(card_row.get("url") or ""),
            "title": str(card_row.get("title") or ""),
            "close_date": close_date,
            "amount_hint": amount_hint,
            "prefilter_status": prefilter_status,
            "prefilter_reason": prefilter_reason,
            "last_seen_at": now,
            "card_row_snapshot": card_row,
        }

    def iter_deep_candidates(
        self,
        *,
        recheck_reject_days: int,
        include_review: bool,
    ) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        out: list[dict[str, Any]] = []
        for e in self.entries.values():
            if not isinstance(e, dict):
                continue
            if entry_eligible_for_deep_pass(
                e,
                now_utc=now,
                recheck_reject_days=recheck_reject_days,
                include_review=include_review,
            ):
                out.append(e)
        return out
