-- Store AI Final reuse hash in a dedicated column instead of raw_data.
-- Also backfill old values from raw_data.ai_finalization.ai_content_hash and remove that nested key.

alter table public.scholarships
  add column if not exists ai_content_hash text;

comment on column public.scholarships.ai_content_hash is
  'Stable content hash for AI Final reuse; excludes transport fields like apply_url/provider_url.';

update public.scholarships
set
  ai_content_hash = coalesce(
    ai_content_hash,
    nullif(raw_data #>> '{ai_finalization,ai_content_hash}', '')
  ),
  raw_data = case
    when jsonb_typeof(raw_data) = 'object'
      and jsonb_typeof(raw_data -> 'ai_finalization') = 'object'
      and (raw_data -> 'ai_finalization') ? 'ai_content_hash'
    then jsonb_set(
      raw_data,
      '{ai_finalization}',
      (raw_data -> 'ai_finalization') - 'ai_content_hash',
      true
    )
    else raw_data
  end
where
  ai_content_hash is null
  or (
    jsonb_typeof(raw_data) = 'object'
    and jsonb_typeof(raw_data -> 'ai_finalization') = 'object'
    and (raw_data -> 'ai_finalization') ? 'ai_content_hash'
  );
