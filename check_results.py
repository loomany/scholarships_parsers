from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv

from utils import get_client

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

load_dotenv(os.path.join(_BASE_DIR, ".env"))
load_dotenv(os.path.join(_BASE_DIR, "..", ".env"))


def _site_base_url() -> str:
    for key in ("SITE_URL", "APP_URL", "FRONTEND_URL", "NEXT_PUBLIC_SITE_URL"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value.rstrip("/")
    return "http://localhost:3000"


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return list(parsed) if isinstance(parsed, list) else []
    return []


def _build_site_link(base_url: str, row: dict[str, Any]) -> str | None:
    slug = str(row.get("slug") or "").strip()
    if slug:
        return f"{base_url}/scholarships/{slug}"
    row_url = str(row.get("url") or "").strip()
    return row_url or None


def main() -> None:
    client = get_client()
    base_url = _site_base_url()
    res = (
        client.table("scholarships")
        .select("title,slug,url,document_urls,last_seen_at,updated_at")
        .not_.is_("document_urls", "null")
        .eq("source", "bold_org")
        .order("updated_at", desc=True)
        .limit(25)
        .execute()
    )
    rows = [row for row in (res.data or []) if isinstance(row, dict)]

    shown = 0
    for row in rows:
        document_urls = _as_list(row.get("document_urls"))
        urls = []
        for item in document_urls:
            if isinstance(item, dict):
                url = str(item.get("url") or "").strip()
                if url:
                    urls.append(url)
        if not urls:
            continue

        shown += 1
        print(f"Grant: {str(row.get('title') or '').strip() or '(untitled)'}")
        print(f"Site page: {_build_site_link(base_url, row) or '(missing)'}")
        print("Documents:")
        for url in urls:
            print(f"  - {url}")
        print("-" * 80)
        if shown >= 10:
            break

    print(f"Displayed grants: {shown}")


if __name__ == "__main__":
    main()
