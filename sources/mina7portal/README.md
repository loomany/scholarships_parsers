# Mina7 Portal (`mina7portal`)

Источник: [mina7portal.com](https://mina7portal.com/en) — каталог стипендий, грантов и других возможностей.

Парсер по умолчанию обходит **только**:

- `/opportunity-type/grants`
- `/opportunity-type/scholarships`

(настраивается через `MINA7_OPPORTUNITY_TYPES`).

HTTP (`requests`). Даты и заголовки читаются из **JSON-LD** на странице возможности при наличии `applicationDeadline`.

```powershell
cd scholarships_parsers
$env:PARSER_SOURCES = "mina7portal"
$env:MINA7PORTAL_ENABLED = "1"
python -u run_all.py
```

Прямой запуск: `python -u -m sources.mina7portal.parser`
