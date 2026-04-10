-- Store extracted document links for scholarship records.

alter table public.scholarships
  add column if not exists document_urls jsonb not null default '[]'::jsonb;

comment on column public.scholarships.document_urls is
  'Array of extracted supporting document links, e.g. PDFs or Google Drive/Docs URLs.';
