# Simpler.Grants.gov

Парсер публичного HTML-поиска [simpler.grants.gov](https://simpler.grants.gov/search) → `public.scholarships`. Grants API и ключи не нужны.

## Откуда данные

- **List:** GET `/search` с фильтрами (статус, funding instrument, eligibility, текстовый запрос и т.д.), как у публичного UI.
- **Detail:** GET страницы возможности (opportunity) по ссылке с листинга — **важно** для полного текста и полей; при `SIMPLER_DETAIL_FETCH=0` запись собирается урезанно.

## Playwright / AI

- **Playwright:** не используется (`requests` + BeautifulSoup).
- **AI:** опционально (`SIMPLER_AI_ENRICH_ENABLED=1`, модель и лимиты — см. `.env.example`; ключ `OPENAI_API_KEY` задаётся в **корневом** `.env`).

## Переменные окружения

См. `sources/simpler_grants_gov/.env.example`. Включение: `SIMPLER_ENABLED` (или fallback `SIMPLER_GRANTS_GOV_ENABLED`).

## Какие переменные переносить в корневой `.env`

- `SIMPLER_ENABLED` (или `SIMPLER_GRANTS_GOV_ENABLED`)
- `SIMPLER_MAX_RECORDS_DEBUG`, `SIMPLER_DETAIL_FETCH`, `SIMPLER_INCLUDE_EXTENDED_SEARCH`
- При AI: `SIMPLER_AI_ENRICH_ENABLED`, `SIMPLER_AI_MODEL`, `SIMPLER_AI_MAX_INPUT_CHARS`
- В корне: `PARSER_SOURCES` (`simpler_grants_gov` или алиас `simpler`), глобальные лимиты, `SUPABASE_*`, при AI — `OPENAI_API_KEY`.

## Быстрый тест

```env
PARSER_SOURCES=simpler
TARGET_NEW_ITEMS=5
SIMPLER_MAX_RECORDS_DEBUG=5
SIMPLER_DETAIL_FETCH=0
SIMPLER_AI_ENRICH_ENABLED=0
```

## Полный прогон

```env
SIMPLER_MAX_RECORDS_DEBUG=0
SIMPLER_DETAIL_FETCH=1
SIMPLER_INCLUDE_EXTENDED_SEARCH=0
```

## Типичные skip

- **Student relevance:** не проходят пути A/B/C (см. docstring в `parser.py`) — institutional без student-сигналов, нет ключевых слов scholarship/fellowship и т.д.
- **Бизнес-фильтры:** нет meaningful funding, нет дедлайна / просрочен / слишком близкий дедлайн.
- **Known** на листинге (как у других источников).
- Опционально AI пропуск при ошибке вызова модели (запись может сохраниться с пометкой ошибки в `raw_data`).
