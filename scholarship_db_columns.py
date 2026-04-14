"""
Колонки public.scholarships, которые парсер передаёт в upsert (кроме служебных).

Служебные ключи, добавляемые в parsers.utils.upsert_scholarship:
  text_fingerprint, last_seen_at

Сверка с миграциями:
  supabase/migrations/20260329120000_scholarships_catalog.sql — базовая таблица
  supabase/migrations/20260330120000_scholarships_extended_fields.sql
  supabase/migrations/20260330180000_scholarships_rich_html.sql
  supabase/migrations/20260330190000_scholarships_full_content_html.sql
  supabase/migrations/20260330200000_scholarships_normalization_seo.sql
  supabase/migrations/20260401000000_scholarships_full_schema_reconcile.sql — полный reconcile
  supabase/migrations/20260331120000_scholarship_ai_finalization.sql — единый AI finalization + SEO

При добавлении поля: обновить этот tuple, импорт SCHOLARSHIP_TABLE_KEYS в парсерах,
миграцию и types_db (npm run supabase:generate-types после push).
"""

from __future__ import annotations

# Тело записи каталога (совпадает с ключами в build_full_record / normalize).
SCHOLARSHIP_UPSERT_BODY_KEYS: tuple[str, ...] = (
    "source",
    "source_id",
    "url",
    "title",
    "provider_name",
    "provider_url",
    "provider_mission",
    "award_amount_text",
    "award_amount_min",
    "award_amount_max",
    "currency",
    "deadline_text",
    "deadline_date",
    "requirements_count",
    "requirements_text",
    "applicants_count",
    "credibility_score_text",
    "is_verified",
    "is_recurring",
    "winner_payment_text",
    "description",
    "provider_social_facebook",
    "provider_social_instagram",
    "provider_social_linkedin",
    "apply_url",
    "apply_button_text",
    "application_status_text",
    "mark_started_available",
    "mark_submitted_available",
    "status_text",
    "institutions_text",
    "state_territory_text",
    "support_email",
    "support_phone",
    "eligibility_text",
    "awards_text",
    "notification_text",
    "selection_criteria_text",
    "description_html",
    "eligibility_html",
    "awards_html",
    "notification_html",
    "payment_html",
    "requirements_html",
    "selection_criteria_html",
    "full_content_html",
    "slug",
    "provider_slug",
    "scholarship_status",
    "days_until_deadline",
    "deadline_bucket",
    "award_amount_numeric_sort",
    "payout_method",
    "credibility_score",
    "credibility_bucket",
    "ranking_score",
    "requirement_types",
    "requirement_signals_count",
    "essay_required",
    "document_required",
    "photo_required",
    "video_required",
    "link_required",
    "survey_required",
    "question_required",
    "goal_required",
    "special_eligibility_required",
    "transcript_required",
    "recommendation_required",
    "financial_need_considered",
    "study_levels",
    "field_of_study",
    "citizenship_statuses",
    "eligibility_tags",
    "catalog_education_levels",
    "gpa_requirement_min",
    "gpa_bucket",
    "location_scope",
    "state_codes",
    "location_tags",
    "institution_types",
    "easy_apply_flags",
    "listing_completeness_bucket",
    "number_of_awards",
    "summary_short",
    "summary_long",
    "who_can_apply",
    "notification_details",
    "payment_details",
    "document_urls",
    "documents_required",
    "requirements_text_clean",
    "official_source_name",
    "last_verified_at",
    "is_indexable",
    "category",
    "category_slug",
    "tags",
    "seo_tags",
    "is_active",
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

SCHOLARSHIP_UPSERT_PAYLOAD_KEYS: frozenset[str] = frozenset(
    SCHOLARSHIP_UPSERT_BODY_KEYS
) | frozenset({"text_fingerprint", "last_seen_at"})

# Ключи для доп. None в build_full_record парсеров — без AI/SEO (их задаёт только finalizer + upsert).
SCHOLARSHIP_RECORD_DEFAULT_KEYS: tuple[str, ...] = tuple(
    k
    for k in SCHOLARSHIP_UPSERT_BODY_KEYS
    if not (str(k).startswith("ai_") or str(k).startswith("seo_"))
)
