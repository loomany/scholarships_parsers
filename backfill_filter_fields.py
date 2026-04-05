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


def backfill(
    batch_size: int,
    limit: int | None,
    dry_run: bool,
    force: bool,
    debug_sample_size: int,
) -> None:
    client = get_client()

    processed = 0
    updated = 0
    no_derived = 0
    skipped = 0
    gpa_bucket_counter: Counter[str] = Counter()
    location_tag_counter: Counter[str] = Counter()
    easy_apply_counter: Counter[str] = Counter()
    completeness_counter: Counter[str] = Counter()
    old_completeness_counter: Counter[str] = Counter()
    old_easy_apply_counter: Counter[str] = Counter()
    verified_true_count = 0
    forced_unchanged_updates = 0
    debug_old_null_samples: list[str] = []
    debug_skip_samples: list[str] = []
    debug_bad_derived_samples: list[str] = []

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
            .order("id")
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
            if listing_bucket not in {"basic_info", "standard_detail", "detailed_listing", "verified_listing"}:
                if len(debug_bad_derived_samples) < debug_sample_size:
                    debug_bad_derived_samples.append(
                        f"id={row.get('id')} derived_bucket={listing_bucket!r} derived_verified={is_verified!r}"
                    )

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
            if isinstance(old_listing_bucket, str) and old_listing_bucket:
                old_completeness_counter[old_listing_bucket] += 1
            else:
                old_completeness_counter["NULL"] += 1
                if len(debug_old_null_samples) < debug_sample_size:
                    debug_old_null_samples.append(
                        f"id={row.get('id')} old_bucket=NULL -> derived_bucket={listing_bucket} derived_verified={is_verified}"
                    )
            for flag in old_easy_apply:
                old_easy_apply_counter[flag] += 1
            if not old_easy_apply:
                old_easy_apply_counter["none"] += 1

            if not any([gpa_bucket, location_tags, easy_apply_flags, listing_bucket, is_verified]):
                no_derived += 1

            is_unchanged = (
                old_gpa_min == gpa_requirement_min
                and old_gpa_bucket == gpa_bucket
                and old_location == location_tags
                and old_easy_apply == easy_apply_flags
                and old_listing_bucket == listing_bucket
                and old_verified == is_verified
            )
            if is_unchanged and not force:
                skipped += 1
                if len(debug_skip_samples) < debug_sample_size:
                    debug_skip_samples.append(
                        f"id={row.get('id')} old_bucket={old_listing_bucket!r} derived_bucket={listing_bucket!r} force={force}"
                    )
                continue
            if is_unchanged and force:
                forced_unchanged_updates += 1

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
    print(f"rows skipped (unchanged): {skipped}")
    print(f"force mode: {force}")
    print("counts by gpa bucket:")
    for key, count in sorted(gpa_bucket_counter.items()):
        print(f"  {key}: {count}")
    print("counts by location tag:")
    for key, count in sorted(location_tag_counter.items()):
        print(f"  {key}: {count}")
    print("counts by easy-apply flag:")
    for key, count in sorted(easy_apply_counter.items()):
        print(f"  {key}: {count}")
    print("old counts by easy-apply flag:")
    for key, count in sorted(old_easy_apply_counter.items()):
        print(f"  {key}: {count}")
    print("counts by listing completeness bucket:")
    for key, count in sorted(completeness_counter.items()):
        print(f"  {key}: {count}")
    print("old counts by listing completeness bucket:")
    for key, count in sorted(old_completeness_counter.items()):
        print(f"  {key}: {count}")
    print(f"is_verified=true rows derived: {verified_true_count}")
    print(f"rows with no derived values: {no_derived}")
    print(f"rows force-updated despite unchanged values: {forced_unchanged_updates}")
    if debug_old_null_samples:
        print("debug sample: old NULL listing_completeness_bucket rows:")
        for line in debug_old_null_samples:
            print(f"  {line}")
    if debug_skip_samples:
        print("debug sample: skipped rows:")
        for line in debug_skip_samples:
            print(f"  {line}")
    if debug_bad_derived_samples:
        print("debug sample: invalid derived listing_completeness_bucket rows:")
        for line in debug_bad_derived_samples:
            print(f"  {line}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug-sample-size", type=int, default=10)
    args = parser.parse_args()

    backfill(
        batch_size=args.batch_size,
        limit=args.limit,
        dry_run=args.dry_run,
        force=args.force,
        debug_sample_size=max(0, args.debug_sample_size),
    )


if __name__ == "__main__":
    main()
