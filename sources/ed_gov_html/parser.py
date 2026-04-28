from __future__ import annotations

import os
import sys
from typing import Any

import requests
from bs4 import BeautifulSoup

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from business_filters import MIN_LEAD_DAYS_BEFORE_DEADLINE, classify_business_deadline, has_meaningful_funding
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS
from utils import upsert_scholarship

SOURCE = "ed_gov_html"
PAGE_URL = "http://www.ed.gov/grants-and-programs/grants-higher-education"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def _extract_summary(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.get_text(strip=True) if soup.title else "ED.gov Grants")
    paragraphs = [p.get_text(" ", strip=True) for p in soup.select("p")]
    summary = "\n".join([x for x in paragraphs if x][:8])[:9000]
    return title, summary


def _build_record(html: str) -> dict[str, Any]:
    title, summary = _extract_summary(html)
    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": "ed-gov-grants-higher-education",
        "url": PAGE_URL,
        "apply_url": PAGE_URL,
        "title": title,
        "provider_name": "U.S. Department of Education",
        "award_amount_text": None,
        "deadline_text": None,
        "deadline_date": None,
        "description": summary or "ED.gov grants listing page.",
        "eligibility_text": summary or "See official program requirements.",
        "requirements_text": summary or "See official program requirements.",
        "mark_started_available": False,
        "mark_submitted_available": False,
        "full_content_html": html[:150000],
        "raw_data": {"seed_url": PAGE_URL, "parser_type": "HTML"},
        "is_active": True,
        "is_recurring": False,
        "currency": "USD",
    }
    apply_normalization(record)
    for k in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if k not in record:
            record[k] = None
    record["source"] = SOURCE
    record["is_active"] = True
    return record


def run() -> None:
    print(f"{SOURCE}: fetching {PAGE_URL}")
    resp = requests.get(PAGE_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    record = _build_record(resp.text)
    if not has_meaningful_funding(record):
        print(f"{SOURCE}: skip — no meaningful funding signal")
        return
    dbiz = classify_business_deadline(record.get("deadline_date"))
    if dbiz != "ok":
        if dbiz == "no_deadline":
            print(f"{SOURCE}: skip — no parsed deadline")
        elif dbiz == "expired":
            print(f"{SOURCE}: skip — deadline expired")
        else:
            print(
                f"{SOURCE}: skip — deadline too soon (need >= {MIN_LEAD_DAYS_BEFORE_DEADLINE} days)"
            )
        return
    out = upsert_scholarship(record)
    print(f"{SOURCE}: upsert done id={out.get('id')}")
