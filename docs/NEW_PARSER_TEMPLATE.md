# Шаблон нового парсера scholarship

## Единая карточка scholarship (продукт)

Любой новый парсер **не задаёт** собственную вёрстку карточки на сайте. Конвейер фиксированный:

**сырые данные источника → нормализованная запись (`build_full_record` / нормализация) → общий AI finalization (`upsert_scholarship`) → общая UI-структура детальной страницы** в веб-приложении.

Парсер обязан отдавать полный, чистый **source-backed** record (тексты, ссылки, дедлайн, сумма, eligibility/requirements по возможности). Поля `ai_*` и `seo_*` на стороне парсера не заполняются вручную в цикле записи — их пишет только общий финализатор при включённом флаге.

## Обязательный контракт

1. Пакет: `sources/<canonical_key>/` с `parser.py` и `__init__.py`, экспортирующим `run`.
2. Запись перед upsert должна содержать минимум (после `build_full_record` + `apply_normalization`):

   - `source`, `source_id`, `title`, `url`, `apply_url`
   - `provider_name`, `award_amount_text`, `deadline_text` / `deadline_date`
   - `description`, `eligibility_text`, `requirements_text`
   - `full_content_html` (если есть)
   - `raw_data` (служебный снимок парсера)

3. **Финальная карточка всегда проходит через общий AI-слой** при `SCHOLARSHIP_AI_FINAL_ENABLED=1`: вызов встроен в **`utils.upsert_scholarship`**. Отдельно вызывать `apply_scholarship_ai_finalization_if_enabled` не требуется.

4. Добавьте канонический ключ в `config.py` (`CANONICAL_SOURCE_KEYS`, `SOURCE_ALIASES`, `source_enabled`, `print_parser_config_summary`) и строку в `run_all._SOURCE_MODULES`.

5. Расширьте `scholarship_db_columns.SCHOLARSHIP_UPSERT_BODY_KEYS` только если появляются **новые колонки таблицы** (с миграцией Supabase). Поля `ai_*` / `seo_*` не добавляйте в цикл `build_full_record` — для них используется `SCHOLARSHIP_RECORD_DEFAULT_KEYS` (без AI/SEO); их заполняет только finalizer в `upsert_scholarship`.

6. Локальные подсказки: `sources/<key>/README.md`, `sources/<key>/.env.example`.

## Порядок в `run()`

1. List / detail → сырой dict  
2. `build_full_record` (или эквивалент)  
3. `apply_normalization(record)`  
4. Бизнес-фильтры (`has_meaningful_funding`, `classify_business_deadline`, …)  
5. (Опционально) source-specific AI — не заменяет финальный слой  
6. `upsert_scholarship(record)` → внутри **AI final** → INSERT/UPDATE  

## Документация

- Детали финального AI: [SCHOLARSHIP_AI_FINALIZATION.md](./SCHOLARSHIP_AI_FINALIZATION.md)
