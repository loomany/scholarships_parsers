from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urlparse

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


def _nested_get(obj: dict[str, Any], *path: str) -> Any:
    current: Any = obj
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _is_bold_url(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    try:
        host = (urlparse(text).netloc or "").lower()
    except Exception:
        return False
    return host.endswith("bold.org")


def _fetch_all_bold_rows() -> list[dict[str, Any]]:
    client = get_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    batch = 1000
    while True:
        res = (
            client.table("scholarships")
            .select("id,source,apply_url,provider_url,raw_data")
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
    rows = _fetch_all_bold_rows()
    print(f"Found {len(rows)} scholarship row(s) with source={SOURCE!r}")
    print("")

    apply_bold_count = 0
    provider_bold_count = 0

    for row in rows:
        raw = _as_dict(row.get("raw_data"))
        raw_url = raw.get("url")
        raw_list_link = _nested_get(raw, "raw_list_card", "link")

        apply_url = row.get("apply_url")
        provider_url = row.get("provider_url")

        apply_is_bold = _is_bold_url(apply_url)
        provider_is_bold = _is_bold_url(provider_url)
        if apply_is_bold:
            apply_bold_count += 1
        if provider_is_bold:
            provider_bold_count += 1

        print(f"ID: {row.get('id')}")
        print(f"apply_url: {apply_url}")
        print(f"provider_url: {provider_url}")
        print(f"raw_data.url: {raw_url}")
        print(f"raw_data.raw_list_card.link: {raw_list_link}")
        print(f"apply_url_is_bold: {apply_is_bold}")
        print(f"provider_url_is_bold: {provider_is_bold}")
        print("-" * 80)

    print("")
    print("Summary:")
    print(f"  total_rows: {len(rows)}")
    print(f"  apply_url_on_bold: {apply_bold_count}")
    print(f"  provider_url_on_bold: {provider_bold_count}")


if __name__ == "__main__":
    main()
