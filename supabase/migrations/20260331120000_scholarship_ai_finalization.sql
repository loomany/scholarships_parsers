-- Unified AI finalization + SEO fields for public.scholarships (catalog parsers).
-- Apply via Supabase CLI / dashboard before enabling SCHOLARSHIP_AI_FINAL_ENABLED=1.

alter table public.scholarships
  add column if not exists ai_student_summary text,
  add column if not exists ai_best_for jsonb default '[]'::jsonb,
  add column if not exists ai_key_highlights jsonb default '[]'::jsonb,
  add column if not exists ai_eligibility_summary jsonb default '[]'::jsonb,
  add column if not exists ai_important_checks jsonb default '[]'::jsonb,
  add column if not exists ai_application_tips jsonb default '[]'::jsonb,
  add column if not exists ai_why_apply jsonb default '[]'::jsonb,
  add column if not exists ai_red_flags jsonb default '[]'::jsonb,
  add column if not exists ai_missing_info jsonb default '[]'::jsonb,
  add column if not exists ai_urgency_level text,
  add column if not exists ai_difficulty_level text,
  add column if not exists ai_match_score integer,
  add column if not exists ai_match_band text,
  add column if not exists ai_score_explanation text,
  add column if not exists ai_confidence_score double precision,
  add column if not exists seo_excerpt text,
  add column if not exists seo_overview text,
  add column if not exists seo_eligibility text,
  add column if not exists seo_application text,
  add column if not exists seo_faq jsonb default '[]'::jsonb;

comment on column public.scholarships.ai_student_summary is 'AI finalization: 2–4 sentence student-facing summary; source-grounded.';
comment on column public.scholarships.ai_match_score is 'Quality/usefulness 0–100; blended rules + model.';
comment on column public.scholarships.seo_faq is 'JSON array of {q,a} from listing facts only.';
