-- IEFA: страны в карточках стипендий + справочник для будущих страниц каталога.

alter table public.scholarships
  add column if not exists host_country_names text[] default '{}'::text[],
  add column if not exists applicant_country_names text[] default '{}'::text[],
  add column if not exists country_summary text;

comment on column public.scholarships.host_country_names is
  'Countries/territories where study may take place (IEFA Host Countries), normalized labels.';
comment on column public.scholarships.applicant_country_names is
  'Eligible applicant nationalities (IEFA Nationality Required), normalized labels; empty if unrestricted.';
comment on column public.scholarships.country_summary is
  'Short human-readable line for UI: host + nationality constraints.';

create table if not exists public.catalog_countries (
  id uuid primary key default gen_random_uuid(),
  source text not null,
  slug text not null,
  display_name text not null,
  last_seen_at timestamptz not null default now(),
  unique (source, slug)
);

create index if not exists catalog_countries_source_idx on public.catalog_countries (source);

comment on table public.catalog_countries is
  'Facet values from catalog sources (e.g. IEFA search country lists) for future /countries pages.';
