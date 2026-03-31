# Единый AI finalization для карточек scholarship

## Назначение

После парсинга, `build_full_record`, `apply_normalization` и бизнес-фильтров каждая запись перед `upsert_scholarship` проходит через **`sources.shared_scholarship_ai`** (если включено env).

- **Фактические блоки** — только из excerpt записи (поля + превью `raw_data`). Модель не должна выдумывать суммы, дедлайны, провайдера и т.д.
- **Guidance-блоки** (`application_tips`, `why_apply`, …) — советы; в промпте зафиксировано, что это не подтверждённые факты источника.

## Конфигурация (корневой `.env`)

| Переменная | Смысл |
|------------|--------|
| `SCHOLARSHIP_AI_FINAL_ENABLED` | `1` — вызывать слой перед каждым upsert |
| `SCHOLARSHIP_AI_MODEL` | Модель OpenAI (например `gpt-4o-mini`) |
| `SCHOLARSHIP_AI_MAX_INPUT_CHARS` | Верхняя граница размера JSON excerpt для модели |
| `SCHOLARSHIP_AI_WRITE_SEO` | Генерировать SEO поля и FAQ |
| `SCHOLARSHIP_AI_WRITE_SCORE` | Учитывать `ai_match_score` от модели (смешивание с rule-score) |
| `SCHOLARSHIP_AI_WRITE_GUIDANCE` | Заполнять «мягкие» списки (best_for, tips, why_apply, …) |

Нужен **`OPENAI_API_KEY`**. Если ключ отсутствует при `ENABLED=1`, применяется **rule-only fallback** (score, urgency, краткий summary из полей записи).

## База данных

Миграция: `supabase/migrations/20260331120000_scholarship_ai_finalization.sql`.

Колонки: `ai_*`, `seo_*` (см. `scholarship_db_columns.py`). Дополнительно в `raw_data.ai_finalization` пишется мета: `version`, `mode`, `rule_score`, `rule_components`, при успехе — `model`, `blended_match_score`.

## Rule-based score

Детерминированные компоненты (0–100): funding, deadline runway, длина/наличие eligibility, заполненность ключевых полей, провайдер. Результат смешивается с оценкой модели (по умолчанию ~35% rules / ~65% model при `WRITE_SCORE=1`).

## Единая JSON-схема ответа модели

См. системный промпт в `sources/shared_scholarship_ai.py`. Поля маппятся на колонки БД, например:

- `student_summary` → `ai_student_summary`
- `best_for` → `ai_best_for` (jsonb array)
- `seo_faq` → `seo_faq` (jsonb `[{"q","a"}, …]`)

## Pipeline

```
parser → build_full_record → apply_normalization → business filters → (опц.) source-specific AI
  → upsert_scholarship → apply_scholarship_ai_finalization_if_enabled → Supabase
```

Парсеру **не обязательно** вызывать finalizer вручную: это делает **`utils.upsert_scholarship`**.

## Целевое поведение UI (фронт вне этого репозитория)

1. **Hero:** title, provider, deadline, amount, badges `ai_match_band` / `ai_urgency_level`, 1–2 строки `ai_student_summary`, CTA на официальный apply.
2. **Quick decision:** `ai_best_for`, `ai_key_highlights`, `ai_why_apply`, `ai_important_checks` — не рендерить пустые секции.
3. **Quick facts / who can apply / overview / award** — из существующих полей записи; не дублировать тот же текст в трёх местах; скрывать блок, если нет добавочной пользы.
4. **Application tips** — подпись «AI guidance».
5. **Red flags / missing info** — всегда честные формулировки.
6. **SEO:** `seo_excerpt`, `seo_overview`, …, `seo_faq` — уникальный копирайт без фактов «с потолка».
7. **Similar scholarships** (когда будет API): показывать `ai_match_score`, amount, deadline; подпись «best match» по эвристике.

### Скрытие пустых блоков

- Массивы `ai_*` длины 0 — не показывать секцию.
- `ai_confidence_score` &lt; 0.35 — по желанию показывать только source-based блоки + disclaimer.

## Пример вывода модели (иллюстрация, не реальная стипендия)

```json
{
  "student_summary": "Merit award for nursing undergraduates at partner colleges. The listing states a spring deadline and a fixed award amount; verify current rules on the sponsor site.",
  "best_for": ["Nursing majors", "Students at listed partner schools"],
  "key_highlights": ["Stated award up to $5,000", "Deadline in excerpt is May 1"],
  "eligibility_summary": ["Undergraduate nursing students", "Enrollment at eligible institution per listing"],
  "important_checks": ["Confirm deadline has not passed", "Verify your school is in the sponsor list"],
  "application_tips": ["Gather unofficial transcript before starting", "Align essay with stated mission if required"],
  "why_apply": ["Clear award cap in listing", "Narrower pool if school-restricted"],
  "red_flags": ["Institution-limited — not national open call"],
  "missing_info": ["Payout method not stated in excerpt"],
  "urgency_level": "medium",
  "difficulty_level": "moderate",
  "ai_match_score": 62,
  "ai_match_band": "medium",
  "score_explanation_short": "Solid amount and deadline, but geographically or institutionally limited.",
  "seo_excerpt": "Nursing scholarship with a fixed award and spring deadline for eligible undergraduates.",
  "seo_overview": "…",
  "seo_eligibility": "…",
  "seo_application": "…",
  "seo_faq": [
    {"q": "Who can apply?", "a": "Per the listing, undergraduate nursing students at eligible institutions."},
    {"q": "What is the deadline?", "a": "The excerpt lists May 1; confirm on the official page."}
  ],
  "confidence_score": 0.72
}
```
