# BigFuture Scholarship Search — API reverse engineering

## Найденные endpoint'ы

### 1) Список scholarships (основной)
- **URL:** `https://scholarshipsearch-api.collegeboard.org/scholarships`
- **Method:** `POST`
- **Назначение:** возвращает список карточек scholarship (с пагинацией `from/size`).

Источник в коде: `SCHOLARSHIPS_API`, `_post_scholarships_list`, `_list_post_body`.

### 2) Детальная страница scholarship
- **URL pattern:** `https://bigfuture.collegeboard.org/scholarships/{programTitleSlug}`
- **Method:** `GET`
- **Назначение:** detail берётся из HTML/SSR блока `__NEXT_DATA__` (не отдельный JSON API в текущей реализации).

Источник в коде: `_scholarship_url`, `fetch_detail_html`, `_extract_next_data_props`.

## Реальный request к list API

### Важные headers
Из текущего parser-а:
- `content-type: application/json`
- `accept: application/json, text/plain, */*`

Практически полезно добавить:
- `origin: https://bigfuture.collegeboard.org`
- `referer: https://bigfuture.collegeboard.org/scholarship-search`
- `user-agent: ...`

### Пример payload
```json
{
  "config": { "size": 15, "from": 0 },
  "criteria": {
    "includeFields": [
      "cbScholarshipId",
      "programTitleSlug",
      "programReferenceId",
      "programOrganizationName",
      "scholarshipMaximumAward",
      "programName",
      "openDate",
      "closeDate",
      "isMeritBased",
      "isNeedBased",
      "awardVerificationCriteriaDescription",
      "programSelfDescription",
      "eligibilityCriteriaDescription",
      "blurb"
    ]
  }
}
```

## Ожидаемая структура ответа

```json
{
  "data": [
    {
      "cbScholarshipId": "...",
      "programTitleSlug": "...",
      "programName": "...",
      "programOrganizationName": "...",
      "scholarshipMaximumAward": 5000,
      "openDate": "YYYY-MM-DD",
      "closeDate": "YYYY-MM-DD",
      "blurb": "..."
    }
  ],
  "totalHits": 1234,
  "from": 0
}
```

## Пагинация
- Размер страницы: `config.size` (в parser-е = `15`).
- Смещение: `config.from`.
- Формула для page (1-based):
  - `from = (page - 1) * size`.
- Остановка:
  - когда `data` пустой (`len(data)==0`) — конец выдачи.

## Нужен ли Playwright

По текущим наблюдениям проекта:
- list API вызывается **после открытия** `https://bigfuture.collegeboard.org/scholarship-search` в browser-контексте;
- в README проекта явно указано, что Playwright обязателен для list;
- в коде уже зафиксировано, что голый сетевой запрос не использовался как стабильный основной путь.

**Итоговая классификация:** **Вариант B** (частично доступно):
- основной запрос — обычный `POST` JSON;
- но, вероятно, нужен лёгкий bootstrap (получить origin/referer/cookies через 1 стартовый GET + `requests.Session`).

Если bootstrap сессии не проходит в вашей среде (429/403/капча/anti-bot) — деградация до **Варианта C** (Playwright нужен).

## Минимальный код без Playwright (requests)

```python
import requests

SEARCH_URL = "https://bigfuture.collegeboard.org/scholarship-search"
API_URL = "https://scholarshipsearch-api.collegeboard.org/scholarships"

INCLUDE_FIELDS = [
    "cbScholarshipId",
    "programTitleSlug",
    "programReferenceId",
    "programOrganizationName",
    "scholarshipMaximumAward",
    "programName",
    "openDate",
    "closeDate",
    "isMeritBased",
    "isNeedBased",
    "awardVerificationCriteriaDescription",
    "programSelfDescription",
    "eligibilityCriteriaDescription",
    "blurb",
]


def fetch_page(session: requests.Session, page: int, size: int = 15):
    payload = {
        "config": {"size": size, "from": (page - 1) * size},
        "criteria": {"includeFields": INCLUDE_FIELDS},
    }
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://bigfuture.collegeboard.org",
        "referer": SEARCH_URL,
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }
    r = session.post(API_URL, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def iter_scholarships(max_pages=10, size=15):
    s = requests.Session()

    # bootstrap (вариант B)
    s.get(SEARCH_URL, timeout=30)

    for page in range(1, max_pages + 1):
        data = fetch_page(s, page=page, size=size)
        rows = data.get("data") or []
        if not rows:
            break
        for row in rows:
            yield row


if __name__ == "__main__":
    for i, row in enumerate(iter_scholarships(max_pages=2), start=1):
        print(i, row.get("programName"), row.get("programTitleSlug"))
```

## Проверка в текущем контейнере
В этой среде прямой сетевой доступ к домену College Board блокируется proxy-ограничением/сетевой политикой, поэтому live-проверку 200/403/429 здесь зафиксировать невозможно.
