-- OPTIONAL: дублировать подписи в колонках (парсеры также кладут то же в raw_data.catalog_ui).
-- Human-readable labels for UI (Quick facts, filters cards). Slugs stay in study_levels / field_of_study.

alter table public.scholarships
  add column if not exists study_levels_display text[],
  add column if not exists field_of_study_display text[],
  add column if not exists scholarship_status_display text;

comment on column public.scholarships.study_levels_display is 'Parallel labels for study_levels slugs, e.g. college_1 → College freshman.';
comment on column public.scholarships.field_of_study_display is 'Title-style labels for field_of_study slugs.';
comment on column public.scholarships.scholarship_status_display is 'Label for scholarship_status, e.g. Upcoming.';
