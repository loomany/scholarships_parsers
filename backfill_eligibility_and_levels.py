"""One-off backfill for scholarships. Updates only eligibility_tags and catalog_education_levels."""

from __future__ import annotations

import argparse
from collections import Counter
from typing import Any

from scholarship_taxonomy import (
    build_taxonomy_blob,
    derive_catalog_education_levels,
    derive_eligibility_tags,
)
from utils import get_client


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


def backfill(batch_size: int, limit: int | None, dry_run: bool) -> None:
    client = get_client()

    processed = 0
    updated = 0
    no_derived = 0
    tag_counter: Counter[str] = Counter()
    level_counter: Counter[str] = Counter()

    offset = 0
    while True:
        if limit is not None and processed >= limit:
            break

        end = offset + batch_size - 1
        res = (
            client.table("scholarships")
            .select(
                "id,title,description,eligibility_text,requirements_text,"
                "awards_text,notification_text,payment_details,institutions_text,"
                "field_of_study,"
                "eligibility_tags,catalog_education_levels"
            )
            .range(offset, end)
            .execute()
        )
        rows = res.data or []
        if not rows:
            break

        for row in rows:
            if limit is not None and processed >= limit:
                break

            processed += 1
            blob = build_taxonomy_blob(row)
            new_tags = derive_eligibility_tags(row, blob)
            new_levels = derive_catalog_education_levels(row, blob)
            old_tags = _norm_list(row.get("eligibility_tags"))
            old_levels = _norm_list(row.get("catalog_education_levels"))

            for tag in new_tags:
                tag_counter[tag] += 1
            for level in new_levels:
                level_counter[level] += 1
            if not new_tags and not new_levels:
                no_derived += 1

            if new_tags == old_tags and new_levels == old_levels:
                continue

            if not dry_run:
                (
                    client.table("scholarships")
                    .update(
                        {
                            "eligibility_tags": new_tags,
                            "catalog_education_levels": new_levels,
                        }
                    )
                    .eq("id", row["id"])
                    .execute()
                )
            updated += 1

        if len(rows) < batch_size:
            break
        offset += batch_size

    print("=== Backfill eligibility_tags/catalog_education_levels ===")
    print(f"rows processed: {processed}")
    print(f"rows updated: {updated}{' (dry run)' if dry_run else ''}")
    print("counts by derived tag:")
    for key, count in sorted(tag_counter.items()):
        print(f"  {key}: {count}")
    print("counts by derived education level:")
    for key, count in sorted(level_counter.items()):
        print(f"  {key}: {count}")
    print(f"rows with no derived tags/levels: {no_derived}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    backfill(batch_size=args.batch_size, limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
