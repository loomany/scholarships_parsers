# WeMakeScholars (`wemakescholars`)

HTTP-парсер [wemakescholars.com](https://www.wemakescholars.com/). Запросы только к URL **без** `?query` (см. их `robots.txt`).

## Запуск

Из каталога `scholarships_parsers`:

```powershell
.\run_wemakescholars.ps1
```

или

```powershell
PARSER_SOURCES=wemakescholars WEMAKE_SCHOLARS_ENABLED=1 python -u run_all.py
```

## Поведение

- Discovery: стартовые hub-страницы + внутренние hub-ссылки того же паттерна. Потолок hub-GET задаёт **`WEMAKE_SCHOLARS_MAX_HUB_PAGES`** (`0` = почти без лимита); если переменная не задана, используется **`MAX_LIST_PAGES`** из GlobalConfig.
- Карточки: `/scholarship/{slug}` → `article.more-about-scholarship`.
- **`source_id`**: slug из URL.

## Переменные

См. `.env.example` в этом каталоге (копировать в корневой `.env`).
