"""One-off backfill for GPA/location/easy-apply/completeness scholarship filter fields."""

from __future__ import annotations

import argparse
from collections import Counter

from scholarship_taxonomy import (
    build_taxonomy_blob,
    derive_easy_apply_flags,
    derive_gpa_fields,
    derive_listing_completeness,
    derive_location_tags,
)
from utils import get_client


def _norm_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        token = item.strip()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def backfill(batch_size: int, limit: int | None, dry_run: bool) -> None:
    client = get_client()

    processed = 0
    updated = 0
    no_derived = 0
    gpa_bucket_counter: Counter[str] = Counter()
    location_tag_counter: Counter[str] = Counter()
    easy_apply_counter: Counter[str] = Counter()
    completeness_counter: Counter[str] = Counter()
    verified_true_count = 0

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
                "field_of_study,state_territory_text,raw_data,state_codes,"
                "requirements_count,requirement_signals_count,essay_required,"
                "document_required,photo_required,link_required,question_required,"
                "recommendation_required,transcript_required,apply_button_text,"
                "application_status_text,support_email,support_phone,apply_url,"
                "deadline_text,deadline_date,provider_name,winner_payment_text,"
                "requirements_text_clean,is_verified,"
                "gpa_requirement_min,gpa_bucket,location_tags,easy_apply_flags,"
                "listing_completeness_bucket"
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
            gpa_requirement_min, gpa_bucket = derive_gpa_fields(row, blob)
            location_tags = derive_location_tags(row, blob)
            easy_apply_flags = derive_easy_apply_flags(row, blob)
            listing_bucket, is_verified = derive_listing_completeness(row, blob)

            if gpa_bucket:
                gpa_bucket_counter[gpa_bucket] += 1
            for tag in location_tags:
                location_tag_counter[tag] += 1
            for flag in easy_apply_flags:
                easy_apply_counter[flag] += 1
            if listing_bucket:
                completeness_counter[listing_bucket] += 1
            if is_verified:
                verified_true_count += 1

            old_location = _norm_list(row.get("location_tags"))
            old_easy_apply = _norm_list(row.get("easy_apply_flags"))
            old_gpa_min = row.get("gpa_requirement_min")
            old_gpa_bucket = row.get("gpa_bucket")
            old_listing_bucket = row.get("listing_completeness_bucket")
            old_verified = bool(row.get("is_verified"))

            if not any([gpa_bucket, location_tags, easy_apply_flags, listing_bucket, is_verified]):
                no_derived += 1

            if (
                old_gpa_min == gpa_requirement_min
                and old_gpa_bucket == gpa_bucket
                and old_location == location_tags
                and old_easy_apply == easy_apply_flags
                and old_listing_bucket == listing_bucket
                and old_verified == is_verified
            ):
                continue

            payload = {
                "gpa_requirement_min": gpa_requirement_min,
                "gpa_bucket": gpa_bucket,
                "location_tags": location_tags,
                "easy_apply_flags": easy_apply_flags,
                "listing_completeness_bucket": listing_bucket,
                "is_verified": is_verified,
            }
            if not dry_run:
                client.table("scholarships").update(payload).eq("id", row["id"]).execute()
            updated += 1

        if len(rows) < batch_size:
            break
        offset += batch_size

    print("=== Backfill GPA/location/easy-apply/completeness filters ===")
    print(f"rows processed: {processed}")
    print(f"rows updated: {updated}{' (dry run)' if dry_run else ''}")
    print("counts by gpa bucket:")
    for key, count in sorted(gpa_bucket_counter.items()):
        print(f"  {key}: {count}")
    print("counts by location tag:")
    for key, count in sorted(location_tag_counter.items()):
        print(f"  {key}: {count}")
    print("counts by easy-apply flag:")
    for key, count in sorted(easy_apply_counter.items()):
        print(f"  {key}: {count}")
    print("counts by listing completeness bucket:")
    for key, count in sorted(completeness_counter.items()):
        print(f"  {key}: {count}")
    print(f"is_verified=true rows derived: {verified_true_count}")
    print(f"rows with no derived values: {no_derived}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    backfill(batch_size=args.batch_size, limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
