-- Canonical SEO tag strings (e.g. award_signal_high_value) from normalize_scholarship / award_signals.
alter table public.scholarships
  add column if not exists seo_tags text[] default '{}'::text[];

comment on column public.scholarships.seo_tags is
  'String tags for SEO/filters; e.g. award_signal_high_value for high-value non-monetary awards.';
