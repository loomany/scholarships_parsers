# One Young World (`oneyoungworld`)

Источник: [One Young World — Scholarships](https://www.oneyoungworld.com/scholarships).

- HTTP только (`requests`), без браузера.
- Листинг: страница стипендий; деталь: `/scholarship/{slug}`; заявка часто на `apply.oneyoungworld.com`.

```powershell
cd scholarships_parsers
$env:PARSER_SOURCES = "oneyoungworld"
$env:ONEYOUNGWORLD_ENABLED = "1"
python -u run_all.py
```

Прямой модуль: `python -u -m sources.oneyoungworld.parser`
