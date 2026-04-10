from __future__ import annotations

import json
import os
import re
from typing import Any

from dotenv import load_dotenv

from utils import get_client

SOURCE = "bold_org"
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

load_dotenv(os.path.join(_BASE_DIR, ".env"))
load_dotenv(os.path.join(_BASE_DIR, "..", ".env"))


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _looks_like_pdf(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    lower = text.lower().split("?", 1)[0].split("#", 1)[0]
    return lower.endswith(".pdf")


_PDF_URL_RE = re.compile(r"https?://[^\s'\"<>]+?\.pdf(?:\?[^\s'\"<>]*)?(?:#[^\s'\"<>]*)?", re.I)


def _extract_pdf_urls(value: str) -> list[str]:
    text = value.strip()
    if not text:
        return []
    return [m.group(0).strip() for m in _PDF_URL_RE.finditer(text)]


def _collect_pdfs(node: Any, found: set[str]) -> None:
    if isinstance(node, str):
        extracted = _extract_pdf_urls(node)
        if extracted:
            found.update(extracted)
            return
    if _looks_like_pdf(node):
        found.add(str(node).strip())
        return
    if isinstance(node, dict):
        for value in node.values():
            _collect_pdfs(value, found)
        return
    if isinstance(node, list):
        for value in node:
            _collect_pdfs(value, found)


def _fetch_bold_rows() -> list[dict[str, Any]]:
    client = get_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    batch = 1000
    while True:
        res = (
            client.table("scholarships")
            .select("title,raw_data")
            .eq("source", SOURCE)
            .range(offset, offset + batch - 1)
            .execute()
        )
        chunk = res.data or []
        rows.extend(row for row in chunk if isinstance(row, dict))
        if len(chunk) < batch:
            break
        offset += batch
    return rows


def main() -> None:
    rows = _fetch_bold_rows()
    total_matches = 0

    for row in rows:
        title = str(row.get("title") or "").strip() or "(untitled)"
        raw_data = _as_dict(row.get("raw_data"))
        pdfs: set[str] = set()
        _collect_pdfs(raw_data, pdfs)
        if not pdfs:
            continue
        for pdf_url in sorted(pdfs):
            total_matches += 1
            print(f"Grant: {title}")
            print(f"PDF: {pdf_url}")
            print("-" * 80)

    print(f"Total PDF links found: {total_matches}")


if __name__ == "__main__":
    main()
