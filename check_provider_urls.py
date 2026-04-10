from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv

from sources.bold_org.parser import _candidate_external_apply_url, _candidate_provider_url
from utils import get_client

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

load_dotenv(os.path.join(_BASE_DIR, ".env"))
load_dotenv(os.path.join(_BASE_DIR, "..", ".env"))

SOURCE = "bold_org"
TARGET_CASES: tuple[dict[str, str], ...] = (
    {
        "label": "milton",
        "provider_slug": "milton-foundation-for-education",
        "provider_name_ilike": "%milton%",
        "title_ilike": "%milton%",
    },
    {
        "label": "doretha pressey",
        "provider_slug": "",
        "provider_name_ilike": "%doretha pressey%",
        "title_ilike": "%doretha pressey%",
    },
)


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


def _row_select() -> str:
    return (
        "id,title,source,slug,url,apply_url,provider_name,provider_slug,"
        "provider_url,updated_at,last_seen_at,raw_data"
    )


def _fetch_latest_rows(limit: int = 10) -> list[dict[str, Any]]:
    client = get_client()
    res = (
        client.table("scholarships")
        .select(_row_select())
        .eq("source", SOURCE)
        .order("updated_at", desc=True)
        .limit(limit)
        .execute()
    )
    return [row for row in (res.data or []) if isinstance(row, dict)]


def _fetch_rows_by_provider_slug(provider_slug: str, *, limit: int = 10) -> list[dict[str, Any]]:
    if not provider_slug:
        return []
    client = get_client()
    res = (
        client.table("scholarships")
        .select(_row_select())
        .eq("source", SOURCE)
        .eq("provider_slug", provider_slug)
        .order("updated_at", desc=True)
        .limit(limit)
        .execute()
    )
    return [row for row in (res.data or []) if isinstance(row, dict)]


def _fetch_rows_by_provider_name_ilike(pattern: str, *, limit: int = 10) -> list[dict[str, Any]]:
    client = get_client()
    res = (
        client.table("scholarships")
        .select(_row_select())
        .eq("source", SOURCE)
        .ilike("provider_name", pattern)
        .order("updated_at", desc=True)
        .limit(limit)
        .execute()
    )
    return [row for row in (res.data or []) if isinstance(row, dict)]


def _fetch_rows_by_title_ilike(pattern: str, *, limit: int = 10) -> list[dict[str, Any]]:
    client = get_client()
    res = (
        client.table("scholarships")
        .select(_row_select())
        .eq("source", SOURCE)
        .ilike("title", pattern)
        .order("updated_at", desc=True)
        .limit(limit)
        .execute()
    )
    return [row for row in (res.data or []) if isinstance(row, dict)]


def _extract_url_like_fields(node: Any, prefix: str = "") -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            key_lc = str(key).lower()
            if any(token in key_lc for token in ("url", "link", "href", "website", "site")):
                out.append((path, value))
            if isinstance(value, dict):
                out.extend(_extract_url_like_fields(value, path))
    return out


def _print_core_row_fields(row: dict[str, Any]) -> None:
    print(f"ID: {row.get('id')}")
    print(f"title: {row.get('title')}")
    print(f"listing_url: {row.get('url')}")
    print(f"apply_url: {row.get('apply_url')}")
    print(f"provider_name: {row.get('provider_name')}")
    print(f"provider_slug: {row.get('provider_slug')}")
    print(f"provider_url: {row.get('provider_url')}")
    print(f"updated_at: {row.get('updated_at')}")
    print(f"last_seen_at: {row.get('last_seen_at')}")


def _print_latest_rows(rows: list[dict[str, Any]]) -> None:
    print("Latest Bold.org rows with provider fields:")
    print("")
    for row in rows:
        _print_core_row_fields(row)
        print("-" * 80)


def _print_target_rows(rows: list[dict[str, Any]], *, label: str, query_label: str) -> None:
    print("")
    print(f"Target audit: {label} ({query_label})")
    print("")
    if not rows:
        print("No rows found.")
        return

    for row in rows:
        raw = _as_dict(row.get("raw_data"))
        raw_list_card = raw.get("raw_list_card")
        raw_list_card = raw_list_card if isinstance(raw_list_card, dict) else {}
        raw_provider = raw_list_card.get("provider")
        raw_donor = raw_list_card.get("donor")
        raw_organization = raw_list_card.get("organization")
        raw_sponsor = raw_list_card.get("sponsor")

        parsed_provider_url = _candidate_provider_url(raw_list_card) if raw_list_card else None
        parsed_external_apply_url = (
            _candidate_external_apply_url(raw_list_card) if raw_list_card else None
        )
        raw_url_fields = []
        for container_name, container_value in (
            ("raw_list_card", raw_list_card),
            ("provider", raw_provider),
            ("donor", raw_donor),
            ("organization", raw_organization),
            ("sponsor", raw_sponsor),
        ):
            if isinstance(container_value, dict):
                raw_url_fields.extend(
                    _extract_url_like_fields(container_value, container_name)
                )

        _print_core_row_fields(row)
        print(f"raw_data.url: {raw.get('url')}")
        print(f"raw_data.provider_is_external: {raw.get('provider_is_external')}")
        print(f"raw_data.apply_is_external: {raw.get('apply_is_external')}")
        print(f"raw_data.raw_list_card.link: {raw_list_card.get('link')}")
        print(f"parser_candidate_provider_url: {parsed_provider_url}")
        print(f"parser_candidate_external_apply_url: {parsed_external_apply_url}")
        if raw_url_fields:
            print("raw nested URL-like fields:")
            for path, value in raw_url_fields:
                print(f"  {path}: {value}")
        else:
            print("raw nested URL-like fields: (none)")
        print("-" * 80)


def main() -> None:
    latest_rows = _fetch_latest_rows(limit=10)
    _print_latest_rows(latest_rows)

    for case in TARGET_CASES:
        slug = case.get("provider_slug", "")
        pattern = case.get("provider_name_ilike", "")
        title_pattern = case.get("title_ilike", "")
        label = case.get("label", "target")

        rows_by_slug = _fetch_rows_by_provider_slug(slug)
        _print_target_rows(
            rows_by_slug,
            label=label,
            query_label=f"provider_slug={slug!r}",
        )

        rows_by_name = _fetch_rows_by_provider_name_ilike(pattern)
        _print_target_rows(
            rows_by_name,
            label=label,
            query_label=f"provider_name ILIKE {pattern!r}",
        )

        rows_by_title = _fetch_rows_by_title_ilike(title_pattern)
        _print_target_rows(
            rows_by_title,
            label=label,
            query_label=f"title ILIKE {title_pattern!r}",
        )


if __name__ == "__main__":
    main()
