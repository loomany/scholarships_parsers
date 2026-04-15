# Grants / scholarships parsers

Единая точка входа: `python run_all.py` (после `load_dotenv` корневого `.env`).

## Конфигурация

- **Рабочий файл:** только корневой `.env` в корне репозитория.
- **Шаблон корня:** `.env.example` — глобальный прогон, включение источников, секреты.
- **Подсказки по источникам:** в каждой папке `sources/<источник>/.env.example` перечислены переменные этого парсера. Код их **не читает**; скопируйте нужные строки в корневой `.env` вручную.
- Документация по источнику: `sources/<источник>/README.md`.

## Структура

- `config.py` — чтение env, алиасы `PARSER_SOURCES`, сводка в консоль.
- `sources/<canonical_key>/` — пакет парсера: `parser.py`, `__init__.py` (`run`), локальные `README.md` и `.env.example`.
- `sources/shared_scholarship_ai.py` — **единый финальный AI-слой** перед записью (внутри `utils.upsert_scholarship`), см. `docs/SCHOLARSHIP_AI_FINALIZATION.md`.
- `docs/NEW_PARSER_TEMPLATE.md` — контракт для новых источников.

### Repeater (циклический запуск)

Для сценария «выкатил статью → через минуту пересобрал опубликованные статьи → подождал 2 часа → повторил»
используйте `repeater.py`:

- `PUBLICATION_INTERVAL_MINUTES` — приоритетный интервал между циклами (например, `180` или `360` минут).
- `REPEATER_SLEEP_HOURS` — fallback-интервал, если `PUBLICATION_INTERVAL_MINUTES` не задан.
- `REPEATER_POST_COMMAND` — команда, которая запускается после основного скрипта и завершается перед сном.
- `RUN_REPROCESS_ON_START=false` — если `REPEATER_POST_COMMAND` не задан, включается дефолтная post-команда:
  `npm run content:reprocess-articles -- --all-published`.
- `REPEATER_POST_COMMAND_DELAY_SECONDS` — задержка перед post-командой (по умолчанию `180` секунд, то есть 3 минуты).

Пример:

```bash
export EXECUTE_SCRIPT=run_all.py
export PUBLICATION_INTERVAL_MINUTES=180
export RUN_REPROCESS_ON_START=false
export REPEATER_POST_COMMAND_DELAY_SECONDS=180
python repeater.py
```

### AI finalization

После применения миграции `supabase/migrations/20260331120000_scholarship_ai_finalization.sql` можно включить `SCHOLARSHIP_AI_FINAL_ENABLED=1` и задать `OPENAI_API_KEY`. Рабочие переменные только в **корневом** `.env`.

## Источники

| Ключ | Папка |
|------|--------|
| `scholarship_america` | `sources/scholarship_america/` |
| `simpler_grants_gov` | `sources/simpler_grants_gov/` |
| `bigfuture` | `sources/bigfuture/` |
