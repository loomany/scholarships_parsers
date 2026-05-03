# Grants / scholarships parsers

Единая точка входа: `python run_all.py` (после `load_dotenv` корневого `.env`).

## Конфигурация

- **Рабочий файл:** только корневой `.env` в корне репозитория.
- **Шаблон корня:** `.env.example` — глобальный прогон, включение источников, секреты.
- **Подсказки по источникам:** в каждой папке `sources/<источник>/.env.example` перечислены переменные этого парсера. Код их **не читает**; скопируйте нужные строки в корневой `.env` вручную.
- Документация по источнику: `sources/<источник>/README.md`.

### Режимный запуск (единый контракт)

- Единая точка запуска: `python run_all.py`
- Режим задается env-переменной `PARSER_MODE`:
  - `html`
  - `api`
  - `browser`
- Маппинг источников по режимам:
  - `PARSER_MODE_SOURCES_HTML` (по умолчанию `scholarship_america`)
  - `PARSER_MODE_SOURCES_API` (по умолчанию `simpler_grants_gov,bigfuture`)
  - `PARSER_MODE_SOURCES_BROWSER` (по умолчанию `bold_org`)
- Если `PARSER_MODE` не задан, используется `PARSER_SOURCES` как раньше.

Примеры:

- `PARSER_MODE=html python run_all.py`
- `PARSER_MODE=api python run_all.py`
- `PARSER_MODE=browser python run_all.py`

## Структура

- `config.py` — чтение env, алиасы `PARSER_SOURCES`, сводка в консоль.
- `sources/<canonical_key>/` — пакет парсера: `parser.py`, `__init__.py` (`run`), локальные `README.md` и `.env.example`.
- `sources/shared_scholarship_ai.py` — **единый финальный AI-слой** перед записью (внутри `utils.upsert_scholarship`), см. `docs/SCHOLARSHIP_AI_FINALIZATION.md`.
- `docs/NEW_PARSER_TEMPLATE.md` — контракт для новых источников.

### AI finalization

После применения миграции `supabase/migrations/20260331120000_scholarship_ai_finalization.sql` можно включить `SCHOLARSHIP_AI_FINAL_ENABLED=1` и задать `OPENAI_API_KEY`. Рабочие переменные только в **корневом** `.env`.

## Источники

| Ключ | Папка |
|------|--------|
| `scholarship_america` | `sources/scholarship_america/` |
| `simpler_grants_gov` | `sources/simpler_grants_gov/` |
| `bigfuture` | `sources/bigfuture/` |
| `ed_gov_html` | `sources/ed_gov_html/` |
| `uoregon_research_html` | `sources/uoregon_research_html/` |
| `oneyoungworld` | `sources/oneyoungworld/` |
| `mina7portal` | `sources/mina7portal/` |
