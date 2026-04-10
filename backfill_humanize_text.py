"""
Пересчитать summary_short и убрать ISO-даты из текстов карточки (ai_*, seo_*) для строк, уже в БД.

Не вызывает OpenAI — только normalize.summary_short + deadline_humanize для полей текста.
Запуск из корня репозитория с загруженным .env (SUPABASE_*).
"""

from __future__ import annotations

import argparse
import re
from copy import deepcopy
from typing import Any

from dotenv import load_dotenv

from deadline_humanize import humanize_iso_datetimes_in_text
from normalize_scholarship import apply_normalization
from utils import get_client

# Подозрение на сырую ISO-дату в тексте (ускоряет сканирование)
_RE_ISO_HINT = re.compile(r"\d{4}-\d{2}-\d{2}")

_STR_KEYS: tuple[str, ...] = (
    "ai_student_summary",
    "seo_excerpt",
    "seo_overview",
    "seo_eligibility",
    "seo_application",
)


def _row_might_contain_iso(row: dict[str, Any]) -> bool:
    parts: list[str] = []
    for k in ("summary_short", "ai_student_summary", *_STR_KEYS):
        v = row.get(k)
        if isinstance(v, str) and v:
            parts.append(v)
    faq = row.get("seo_faq")
    if isinstance(faq, list):
        for item in faq:
            if isinstance(item, dict):
                for fk in ("q", "a"):
                    fv = item.get(fk)
                    if isinstance(fv, str):
                        parts.append(fv)
    blob = "\n".join(parts)
    return bool(_RE_ISO_HINT.search(blob))


def _compute_text_updates(db_row: dict[str, Any]) -> dict[str, Any]:
    """Поля для UPDATE; пустой dict — менять нечего."""
    merged = dict(db_row)
    apply_normalization(merged)

    out: dict[str, Any] = {}

    if merged.get("summary_short") != db_row.get("summary_short"):
        out["summary_short"] = merged.get("summary_short")

    for k in _STR_KEYS:
        v = db_row.get(k)
        if not isinstance(v, str):
            continue
        nv = humanize_iso_datetimes_in_text(v)
        if nv is not None and nv != v:
            out[k] = nv

    faq = db_row.get("seo_faq")
    if isinstance(faq, list):
        new_faq: list[Any] = []
        changed = False
        for item in faq:
            if not isinstance(item, dict):
                new_faq.append(item)
                continue
            ni = deepcopy(item)
            for fk in ("q", "a"):
                fv = ni.get(fk)
                if isinstance(fv, str):
                    nn = humanize_iso_datetimes_in_text(fv)
                    if nn is not None and nn != fv:
                        ni[fk] = nn
                        changed = True
            new_faq.append(ni)
        if changed:
            out["seo_faq"] = new_faq

    return out


def backfill(
    *,
    source: str | None,
    batch_size: int,
    limit: int | None,
    dry_run: bool,
    scan_all: bool,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    client = get_client()
    scanned = 0
    candidates = 0
    updated = 0
    skipped_no_iso = 0
    unchanged = 0
    errors = 0
    offset = 0

    while True:
        if limit is not None and candidates >= limit:
            break
        query = client.table("scholarships").select("*").order("id").range(
            offset, offset + batch_size - 1
        )
        if source:
            query = query.eq("source", source)
        res = query.execute()
        rows = res.data or []
        if not rows:
            break

        for row in rows:
            if limit is not None and candidates >= limit:
                break
            if not isinstance(row, dict):
                continue
            scanned += 1
            row_id = row.get("id")
            if not scan_all and not _row_might_contain_iso(row):
                skipped_no_iso += 1
                continue

            candidates += 1
            title = str(row.get("title") or "").strip() or "<untitled>"
            try:
                payload = _compute_text_updates(row)
                if not payload:
                    unchanged += 1
                    continue
                if dry_run:
                    print(
                        f"[dry-run] id={row_id} title={title[:70]!r} "
                        f"keys={list(payload.keys())}",
                        flush=True,
                    )
                else:
                    client.table("scholarships").update(payload).eq("id", row_id).execute()
                    print(
                        f"updated id={row_id} title={title[:70]!r} keys={list(payload.keys())}",
                        flush=True,
                    )
                updated += 1
            except Exception as exc:
                errors += 1
                print(f"error id={row_id}: {type(exc).__name__}: {exc}", flush=True)

        offset += len(rows)

    print("", flush=True)
    print("=== backfill_humanize_text ===", flush=True)
    print(f"source filter: {source or 'ALL'}", flush=True)
    print(f"scan_all rows (no ISO prefilter): {scan_all}", flush=True)
    print(f"rows scanned: {scanned}", flush=True)
    print(f"rows considered (after ISO filter): {candidates}", flush=True)
    print(f"rows skipped (no ISO in text fields): {skipped_no_iso}", flush=True)
    print(f"rows unchanged after recompute: {unchanged}", flush=True)
    print(f"rows updated: {updated}{' (dry run)' if dry_run else ''}", flush=True)
    print(f"errors: {errors}", flush=True)


def main() -> None:
    load_dotenv()
    p = argparse.ArgumentParser(
        description="Refresh summary_short and humanize ISO in AI/SEO text for existing rows."
    )
    p.add_argument("--source", help="Filter by source, e.g. bold_org")
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--scan-all",
        action="store_true",
        help="Recompute every row (slow); default is only rows whose text looks like it contains ISO dates.",
    )
    args = p.parse_args()
    backfill(
        source=(args.source or "").strip() or None,
        batch_size=args.batch_size,
        limit=args.limit,
        dry_run=bool(args.dry_run),
        scan_all=bool(args.scan_all),
    )


if __name__ == "__main__":
    main()
