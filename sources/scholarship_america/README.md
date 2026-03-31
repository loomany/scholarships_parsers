# Scholarship America

Парсер каталога [Scholarship America](https://scholarshipamerica.org/students/browse-scholarships/) → таблица `public.scholarships` (Supabase).

## Откуда данные

- **List:** HTML страница browse (пагинация листинга).
- **Detail:** HTML карточки стипендии по ссылке с листинга (если `SCHOLARSHIP_AMERICA_DETAIL_FETCH=1`).

## Playwright / AI

- **Playwright:** не используется (только `requests` + BeautifulSoup).
- **AI enrichment:** в `config.py` для этого источника не подключён; обогащение через OpenAI не применяется.

## Переменные окружения

См. локальный `.env.example`. В корне также нужны `PARSER_SOURCES`, глобальные лимиты (`TARGET_NEW_ITEMS`, `MAX_LIST_PAGES`, …) и секреты Supabase.

## Какие переменные переносить в корневой `.env`

Скопируйте из `sources/scholarship_america/.env.example`:

- `SCHOLARSHIP_AMERICA_ENABLED`
- `SCHOLARSHIP_AMERICA_MAX_RECORDS_DEBUG`
- `SCHOLARSHIP_AMERICA_DETAIL_FETCH`

Плюс в корне: `PARSER_SOURCES` (например `scholarship_america` или `sa`), `TARGET_NEW_ITEMS`, `SUPABASE_*`.

## Быстрый тест

```env
PARSER_SOURCES=scholarship_america
TARGET_NEW_ITEMS=5
SCHOLARSHIP_AMERICA_MAX_RECORDS_DEBUG=5
SCHOLARSHIP_AMERICA_DETAIL_FETCH=0
```

## Полный прогон

```env
SCHOLARSHIP_AMERICA_MAX_RECORDS_DEBUG=0
SCHOLARSHIP_AMERICA_DETAIL_FETCH=1
```

## Типичные skip

- Уже в индексе known (URL / `source_id` / заголовок) при `SKIP_EXISTING_ON_LIST` и `DISCOVERY_MODE=new_only`.
- Бизнес-фильтры после сборки записи: нет осмысленного funding, нет/просрочен/слишком близкий дедлайн (`business_filters`).
