"""Backfill missing AI fields for existing scholarships rows.

Uses the same final AI layer as parser upserts, but only updates existing rows
that still have incomplete AI output.
"""

from __future__ import annotations

import argparse
from typing import Any

from dotenv import load_dotenv

from ai_monitoring import diff_ai_usage, snapshot_ai_usage
from sources.shared_scholarship_ai import apply_scholarship_ai_finalization_if_enabled
from utils import get_client

PRIMARY_AI_FIELDS: tuple[str, ...] = (
    "ai_student_summary",
    "ai_match_score",
    "seo_excerpt",
)

AI_WRITE_FIELDS: tuple[str, ...] = (
    "ai_student_summary",
    "ai_best_for",
    "ai_key_highlights",
    "ai_eligibility_summary",
    "ai_important_checks",
    "ai_application_tips",
    "ai_why_apply",
    "ai_red_flags",
    "ai_missing_info",
    "ai_urgency_level",
    "ai_difficulty_level",
    "ai_match_score",
    "ai_match_band",
    "ai_score_explanation",
    "ai_confidence_score",
    "seo_excerpt",
    "seo_overview",
    "seo_eligibility",
    "seo_application",
    "seo_faq",
    "ai_content_hash",
    "raw_data",
)


def _is_blank(value: Any) -> bool:
    return value in (None, "", [])


def _needs_ai_backfill(row: dict[str, Any]) -> bool:
    return any(_is_blank(row.get(key)) for key in PRIMARY_AI_FIELDS)


def _build_payload(updated_row: dict[str, Any]) -> dict[str, Any]:
    return {key: updated_row.get(key) for key in AI_WRITE_FIELDS}


def _payload_changed(before: dict[str, Any], payload: dict[str, Any]) -> bool:
    return any(before.get(key) != value for key, value in payload.items())


def backfill_missing_ai(
    *,
    source: str | None,
    batch_size: int,
    limit: int | None,
    dry_run: bool,
    force: bool,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be > 0")

    client = get_client()
    ai_start = snapshot_ai_usage()

    scanned = 0
    candidates = 0
    updated = 0
    unchanged = 0
    errors = 0
    current_offset = 0

    while True:
        if limit is not None and candidates >= limit:
            break

        query = client.table("scholarships").select("*").order("id").range(
            current_offset, current_offset + batch_size - 1
        )
        if source:
            query = query.eq("source", source)
        res = query.execute()
        rows = res.data or []
        if not rows:
            break

        scanned += len(rows)
        for row in rows:
            if limit is not None and candidates >= limit:
                break
            if not isinstance(row, dict) or not _needs_ai_backfill(row):
                continue

            candidates += 1
            row_id = row.get("id")
            title = str(row.get("title") or "").strip() or "<untitled>"
            src = str(row.get("source") or "").strip() or "unknown"
            print(f"[{candidates}] processing id={row_id} source={src} title={title}")

            try:
                existing_row_for_ai = None if _needs_ai_backfill(row) else row
                updated_row = apply_scholarship_ai_finalization_if_enabled(
                    dict(row),
                    existing_row=existing_row_for_ai,
                )
                payload = _build_payload(updated_row)
                if not force and not _payload_changed(row, payload):
                    unchanged += 1
                    print("    skipped: AI payload unchanged", flush=True)
                    continue
                if not dry_run:
                    client.table("scholarships").update(payload).eq("id", row_id).execute()
                updated += 1
                print(f"    {'would update' if dry_run else 'updated'}", flush=True)
            except Exception as exc:
                errors += 1
                print(f"    error: {type(exc).__name__}: {exc}", flush=True)

        current_offset += len(rows)

    ai_diff = diff_ai_usage(ai_start)
    print("", flush=True)
    print("=== Missing AI Backfill Summary ===", flush=True)
    print(f"source filter: {source or 'ALL'}", flush=True)
    print(f"rows scanned: {scanned}", flush=True)
    print(f"rows needing AI backfill: {candidates}", flush=True)
    print(f"rows updated: {updated}{' (dry run)' if dry_run else ''}", flush=True)
    print(f"rows unchanged after recompute: {unchanged}", flush=True)
    print(f"errors: {errors}", flush=True)
    print("AI usage:", flush=True)
    print(f"  api calls: {ai_diff.api_calls}", flush=True)
    print(f"  reused existing: {ai_diff.reused}", flush=True)
    print(f"  skipped: {ai_diff.skipped}", flush=True)
    print(f"  errors: {ai_diff.errors}", flush=True)
    print(
        "  tokens: "
        f"prompt={ai_diff.prompt_tokens}, completion={ai_diff.completion_tokens}, total={ai_diff.total_tokens}"
        ,
        flush=True,
    )
    print(f"  estimated cost: ${ai_diff.estimated_cost_usd:.4f}", flush=True)


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Backfill scholarships rows missing AI output.")
    parser.add_argument("--source", help="Optional source filter, e.g. bold_org")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Write payload even if computed AI fields match current values.",
    )
    args = parser.parse_args()

    backfill_missing_ai(
        source=(args.source or "").strip() or None,
        batch_size=args.batch_size,
        limit=args.limit,
        dry_run=bool(args.dry_run),
        force=bool(args.force),
    )


if __name__ == "__main__":
    main()
