"""
Записать в raw_data.catalog_ui человекочитаемые подписи (уровни обучения, статус, поля).

Не требует новых колонок в БД. Нужны SUPABASE_URL и SUPABASE_SERVICE_ROLE_KEY в .env.

Фронт: для Quick facts брать
  raw_data.catalog_ui.study_levels_display
  raw_data.catalog_ui.field_of_study_display
  raw_data.catalog_ui.scholarship_status_display
вместо сырых slug в study_levels / scholarship_status.
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from scholarship_taxonomy import (
    field_of_study_to_display_labels,
    scholarship_status_to_display,
    study_levels_to_display_labels,
)
from utils import get_client


def _build_catalog_ui(row: dict) -> dict:
    return {
        "study_levels_display": study_levels_to_display_labels(row.get("study_levels")),
        "field_of_study_display": field_of_study_to_display_labels(row.get("field_of_study")),
        "scholarship_status_display": scholarship_status_to_display(
            row.get("scholarship_status")
        ),
    }


def _merge_raw_data(row: dict) -> dict | None:
    """Возвращает новый raw_data или None если менять нечего."""
    rd = row.get("raw_data")
    if isinstance(rd, dict):
        rd = dict(rd)
    else:
        rd = {}
    new_ui = _build_catalog_ui(row)
    old_ui = rd.get("catalog_ui")
    if isinstance(old_ui, dict) and old_ui == new_ui:
        return None
    rd["catalog_ui"] = new_ui
    return rd


def backfill(
    *,
    source: str | None,
    batch_size: int,
    limit: int | None,
    dry_run: bool,
) -> None:
    client = get_client()
    scanned = 0
    updated = 0
    unchanged = 0
    errors = 0
    offset = 0
    page_size = 1 if limit is not None else max(1, batch_size)

    while True:
        if limit is not None and scanned >= limit:
            break
        query = (
            client.table("scholarships")
            .select("id,source,study_levels,field_of_study,scholarship_status,raw_data,title")
            .order("id")
            .range(offset, offset + page_size - 1)
        )
        if source:
            query = query.eq("source", source)
        res = query.execute()
        rows = res.data or []
        if not rows:
            break

        for row in rows:
            if not isinstance(row, dict):
                continue
            if limit is not None and scanned >= limit:
                break
            scanned += 1
            rid = row.get("id")
            title = str(row.get("title") or "")[:70]
            try:
                new_rd = _merge_raw_data(row)
                if new_rd is None:
                    unchanged += 1
                    continue
                if dry_run:
                    print(f"[dry-run] id={rid} {title!r} catalog_ui={new_rd.get('catalog_ui')}", flush=True)
                else:
                    client.table("scholarships").update({"raw_data": new_rd}).eq("id", rid).execute()
                    print(f"updated id={rid} {title!r}", flush=True)
                updated += 1
            except Exception as exc:
                errors += 1
                print(f"error id={rid}: {type(exc).__name__}: {exc}", flush=True)

        offset += len(rows)
        if limit is not None and scanned >= limit:
            break
        if len(rows) < page_size:
            break

    print("", flush=True)
    print("=== backfill_display_labels (raw_data.catalog_ui) ===", flush=True)
    print(f"source: {source or 'ALL'}", flush=True)
    print(f"rows scanned: {scanned}", flush=True)
    print(f"updated: {updated}{' (dry run)' if dry_run else ''}", flush=True)
    print(f"unchanged: {unchanged}", flush=True)
    print(f"errors: {errors}", flush=True)


def main() -> None:
    load_dotenv()
    p = argparse.ArgumentParser()
    p.add_argument("--source", help="e.g. bold_org")
    p.add_argument("--batch-size", type=int, default=300)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    backfill(
        source=(args.source or "").strip() or None,
        batch_size=max(1, args.batch_size),
        limit=args.limit,
        dry_run=bool(args.dry_run),
    )


if __name__ == "__main__":
    main()
