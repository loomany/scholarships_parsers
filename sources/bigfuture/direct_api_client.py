"""Direct (non-Playwright) client for BigFuture scholarship list API.

Usage:
  python -m sources.bigfuture.direct_api_client --pages 2 --size 15
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Iterable

import requests

SEARCH_URL = "https://bigfuture.collegeboard.org/scholarship-search"
API_URL = "https://scholarshipsearch-api.collegeboard.org/scholarships"

INCLUDE_FIELDS = [
    "cbScholarshipId",
    "programTitleSlug",
    "programReferenceId",
    "programOrganizationName",
    "scholarshipMaximumAward",
    "programName",
    "openDate",
    "closeDate",
    "isMeritBased",
    "isNeedBased",
    "awardVerificationCriteriaDescription",
    "programSelfDescription",
    "eligibilityCriteriaDescription",
    "blurb",
]


def _payload(page: int, size: int) -> dict[str, Any]:
    return {
        "config": {"size": size, "from": (page - 1) * size},
        "criteria": {"includeFields": INCLUDE_FIELDS},
    }


def _headers() -> dict[str, str]:
    return {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://bigfuture.collegeboard.org",
        "referer": SEARCH_URL,
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }


def fetch_page(session: requests.Session, page: int, size: int) -> dict[str, Any]:
    r = session.post(API_URL, json=_payload(page, size), headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


def iter_rows(max_pages: int, size: int) -> Iterable[dict[str, Any]]:
    s = requests.Session()
    s.get(SEARCH_URL, timeout=30)

    for page in range(1, max_pages + 1):
        data = fetch_page(s, page=page, size=size)
        rows = data.get("data") or []
        if not rows:
            break
        for row in rows:
            if isinstance(row, dict):
                yield row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=2)
    ap.add_argument("--size", type=int, default=15)
    ap.add_argument("--pretty", action="store_true")
    ns = ap.parse_args()

    for row in iter_rows(max_pages=max(1, ns.pages), size=max(1, ns.size)):
        if ns.pretty:
            print(json.dumps(row, ensure_ascii=False, indent=2))
        else:
            print(row.get("programName"), row.get("programTitleSlug"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
