# BigFuture (College Board)

Парсер [BigFuture Scholarship Search](https://bigfuture.collegeboard.org/scholarship-search) → `public.scholarships`.

## Откуда данные

- **List:** POST JSON API `scholarshipsearch-api.collegeboard.org/scholarships` из контекста Playwright (после открытия страницы поиска).
- **Detail:** HTML страница `/scholarships/{slug}` + встроенный `__NEXT_DATA__` (при `BIGFUTURE_DETAIL_FETCH=1`).

## Playwright / AI

- **Playwright:** обязателен для list и (обычно) для detail.
- **AI:** опционально (`BIGFUTURE_AI_ENRICH_ENABLED=1`); ключ `OPENAI_API_KEY` только в **корневом** `.env`.

## Дешёвый list vs дорогой deep

- **`BIGFUTURE_ACTIVE_ONLY=1`:** на уровне ответа list отбрасываются карточки с прошедшим `closeDate` (лог `skip_prefilter_expired`), без detail/AI.
- **Fast prefilter** (`sources/bigfuture/prefilter.py`): по полям list отсекает часть мусора; результаты пишутся в JSON-store (путь см. ниже). **Deep:** detail → `build_full_record` → бизнес-фильтры → AI → upsert только для прошедших fast.
- Режимы: `BIGFUTURE_FAST_PREFILTER_ONLY` (только list+store), `BIGFUTURE_DEEP_PASS_ONLY` (только очередь из store).

## Переменные окружения

См. `sources/bigfuture/.env.example`.

## Какие переменные переносить в корневой `.env`

Скопируйте из локального `.env.example` все строки, которые хотите включить, в том числе:

- `BIGFUTURE_ENABLED`, `BIGFUTURE_ACTIVE_ONLY`, `BIGFUTURE_DETAIL_FETCH`
- `BIGFUTURE_HEADLESS`, `BIGFUTURE_TIMEOUT_MS`, `BIGFUTURE_MAX_RECORDS_DEBUG`, `BIGFUTURE_KEYWORD`
- AI: `BIGFUTURE_AI_*`
- Двухфазный режим: `BIGFUTURE_FAST_PREFILTER_ONLY`, `BIGFUTURE_DEEP_PASS_ONLY`, `BIGFUTURE_MIN_AMOUNT_HINT`, `BIGFUTURE_PREFILTER_STORE_PATH`, `BIGFUTURE_RECHECK_REJECT_DAYS`, `BIGFUTURE_DEEP_INCLUDE_REVIEW`

В корне дополнительно: `PARSER_SOURCES` (`bigfuture` или `bf`), глобальные лимиты, `SUPABASE_*`, при AI — `OPENAI_API_KEY`.

## Быстрый тест

Меньше сети и браузера:

```env
PARSER_SOURCES=bf
BIGFUTURE_ENABLED=1
BIGFUTURE_DETAIL_FETCH=0
BIGFUTURE_AI_ENRICH_ENABLED=0
BIGFUTURE_MAX_RECORDS_DEBUG=5
TARGET_NEW_ITEMS=5
```

## Полный прогон

```env
BIGFUTURE_DETAIL_FETCH=1
BIGFUTURE_AI_ENRICH_ENABLED=1
BIGFUTURE_MAX_RECORDS_DEBUG=0
```

## Типичные skip

- List: `skip_prefilter_expired`, пустая страница API, fast prefilter (`prefilter_reject_*`, `prefilter_review`).
- После detail: relevance (hard negatives), бизнес-фильтры (funding / deadline), known index.
- Логи «page filtered out (all expired), continue paging» — API вернул строки, локально все с истёкшим `closeDate`, листинг продолжается.
