"""One-off backfill for structured taxonomy fields on existing scholarships rows.

Updates only:
  - study_levels
  - field_of_study
  - citizenship_statuses
  - raw_data (only when taxonomy_noncanonical_dropped changed)
"""

from __future__ import annotations

import argparse
from typing import Any

from normalize_scholarship import apply_normalization
from utils import get_client


TAXONOMY_INPUT_FIELDS: tuple[str, ...] = (
    # identity / core
    "id",
    "source",
    "source_id",
    "url",
    "title",
    # text signals used by taxonomy extraction
    "description",
    "summary_short",
    "summary_long",
    "eligibility_text",
    "requirements_text",
    "awards_text",
    "who_can_apply",
    "notification_text",
    "selection_criteria_text",
    "institutions_text",
    "state_territory_text",
    # structured/context fields used by derivation
    "category",
    "tags",
    "study_levels",
    "field_of_study",
    "citizenship_statuses",
    "eligibility_tags",
    "catalog_education_levels",
    "raw_data",
    # html surfaces consumed by build_taxonomy_blob
    "description_html",
    "eligibility_html",
    "requirements_html",
    "full_content_html",
    # fields accessed by normalization helpers (safe to include for parity)
    "award_amount_text",
    "award_amount_min",
    "award_amount_max",
    "deadline_text",
    "deadline_date",
    "status_text",
    "requirements_count",
    "winner_payment_text",
    "payment_html",
    "payment_details",
    "provider_name",
    "provider_url",
    "support_email",
    "support_phone",
    "is_verified",
    "state_codes",
    "location_scope",
    "institution_types",
)


def _norm_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        token = item.strip().lower()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def _taxonomy_debug_fragment(raw_data: Any) -> Any:
    if not isinstance(raw_data, dict):
        return None
    frag = raw_data.get("taxonomy_noncanonical_dropped")
    if not isinstance(frag, dict):
        return None
    return frag


def backfill(
    *,
    batch_size: int,
    limit: int | None,
    offset: int,
    force: bool,
    dry_run: bool,
    debug_sample_size: int,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit is not None and limit < 0:
        raise ValueError("limit must be >= 0")

    client = get_client()
    processed = 0
    updated = 0
    skipped = 0
    errors = 0
    non_empty_study_levels = 0
    non_empty_field_of_study = 0
    non_empty_citizenship = 0
    all_three_empty = 0
    debug_samples: list[dict[str, Any]] = []

    current_offset = offset
    select_clause = ",".join(TAXONOMY_INPUT_FIELDS)

    while True:
        if limit is not None and processed >= limit:
            break

        end = current_offset + batch_size - 1
        res = (
            client.table("scholarships")
            .select(select_clause)
            .order("id")
            .range(current_offset, end)
            .execute()
        )
        rows = res.data or []
        if not rows:
            break

        for row in rows:
            if limit is not None and processed >= limit:
                break
            processed += 1

            row_id = row.get("id")
            if not row_id:
                errors += 1
                continue

            before_study = _norm_list(row.get("study_levels"))
            before_fos = _norm_list(row.get("field_of_study"))
            before_cit = _norm_list(row.get("citizenship_statuses"))
            before_tax_debug = _taxonomy_debug_fragment(row.get("raw_data"))

            try:
                rec = dict(row)
                apply_normalization(rec)
            except Exception:
                errors += 1
                continue

            after_study = _norm_list(rec.get("study_levels"))
            after_fos = _norm_list(rec.get("field_of_study"))
            after_cit = _norm_list(rec.get("citizenship_statuses"))
            after_raw = rec.get("raw_data")
            after_tax_debug = _taxonomy_debug_fragment(after_raw)

            if after_study:
                non_empty_study_levels += 1
            if after_fos:
                non_empty_field_of_study += 1
            if after_cit:
                non_empty_citizenship += 1
            if not after_study and not after_fos and not after_cit:
                all_three_empty += 1

            changed_taxonomy = (
                before_study != after_study
                or before_fos != after_fos
                or before_cit != after_cit
            )
            changed_raw_tax_debug = before_tax_debug != after_tax_debug

            if not force and not changed_taxonomy and not changed_raw_tax_debug:
                skipped += 1
                continue

            payload: dict[str, Any] = {
                "study_levels": after_study,
                "field_of_study": after_fos,
                "citizenship_statuses": after_cit,
            }
            if changed_raw_tax_debug:
                payload["raw_data"] = after_raw if isinstance(after_raw, dict) else row.get("raw_data")

            if len(debug_samples) < debug_sample_size:
                debug_samples.append(
                    {
                        "id": row_id,
                        "before": {
                            "study_levels": before_study,
                            "field_of_study": before_fos,
                            "citizenship_statuses": before_cit,
                            "taxonomy_noncanonical_dropped": before_tax_debug,
                        },
                        "after": {
                            "study_levels": after_study,
                            "field_of_study": after_fos,
                            "citizenship_statuses": after_cit,
                            "taxonomy_noncanonical_dropped": after_tax_debug,
                        },
                    }
                )

            if not dry_run:
                client.table("scholarships").update(payload).eq("id", row_id).execute()
            updated += 1

        current_offset += len(rows)

    print("=== Backfill structured taxonomy ===")
    print(f"processed: {processed}")
    print(f"updated: {updated}{' (dry run)' if dry_run else ''}")
    print(f"skipped: {skipped}")
    print(f"errors: {errors}")
    print("coverage (after recalculation):")
    print(f"  non-empty study_levels: {non_empty_study_levels}")
    print(f"  non-empty field_of_study: {non_empty_field_of_study}")
    print(f"  non-empty citizenship_statuses: {non_empty_citizenship}")
    print(f"  all three empty: {all_three_empty}")
    print(f"force mode: {force}")
    print(f"offset: {offset}")
    print(f"batch_size: {batch_size}")
    print(f"limit: {limit}")
    if debug_samples:
        print("debug samples (before/after):")
        for sample in debug_samples:
            print(sample)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--debug-sample-size", type=int, default=5)
    args = parser.parse_args()

    backfill(
        batch_size=args.batch_size,
        limit=args.limit,
        offset=args.offset,
        force=args.force,
        dry_run=args.dry_run,
        debug_sample_size=max(0, args.debug_sample_size),
    )


if __name__ == "__main__":
    main()
