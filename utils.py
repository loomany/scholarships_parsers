"""
Утилиты для работы с Supabase и стипендиями.
Максимально простой код для новичков.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from supabase import Client, create_client

from scholarship_db_columns import SCHOLARSHIP_UPSERT_PAYLOAD_KEYS

# Имена переменных окружения (как в Supabase Dashboard → Settings → API)
ENV_URL = "SUPABASE_URL"
ENV_KEY = "SUPABASE_SERVICE_ROLE_KEY"


def get_client() -> Client:
    """
    Создаёт клиент Supabase с правами service_role (обходит RLS).
    Ключ храни только на сервере / в .env, никогда не коммить в git.
    """
    url = os.environ.get(ENV_URL, "").strip()
    key = os.environ.get(ENV_KEY, "").strip()
    if not url or not key:
        raise RuntimeError(
            f"Задай переменные окружения {ENV_URL} и {ENV_KEY} "
            "(например в файле parsers/.env или в системе)."
        )
    return create_client(url, key)


def _norm_text(value: Any) -> str:
    """None → пустая строка; иначе str, lower, trim, лишние пробелы схлопнуть."""
    if value is None:
        return ""
    s = str(value).strip().lower()
    return " ".join(s.split())


def build_text_fingerprint(record: Mapping[str, Any]) -> str:
    """
    Отпечаток для дедупликации (уровень 3 в БД).
    Берём поля, нормализуем, склеиваем, считаем SHA-256 (hex).
    """
    parts = [
        _norm_text(record.get("source")),
        _norm_text(record.get("title")),
        _norm_text(record.get("provider_name")),
        _norm_text(record.get("award_amount_text")),
        _norm_text(record.get("deadline_text")),
    ]
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _now_iso() -> str:
    """Время в ISO для timestamptz (UTC)."""
    return datetime.now(timezone.utc).isoformat()


def _find_id_by_source_url(client: Client, source: str, url: str) -> str | None:
    res = (
        client.table("scholarships")
        .select("id")
        .eq("source", source)
        .eq("url", url)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0]["id"] if rows else None


def _find_id_by_source_source_id(
    client: Client, source: str, source_id: str
) -> str | None:
    res = (
        client.table("scholarships")
        .select("id")
        .eq("source", source)
        .eq("source_id", source_id)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0]["id"] if rows else None


@dataclass
class KnownScholarshipIndex:
    """In-memory ключи существующих строк для source (быстрый skip на листинге)."""

    source_ids: set[str] = field(default_factory=set)
    urls: set[str] = field(default_factory=set)
    slugs_lc: set[str] = field(default_factory=set)
    titles_norm: set[str] = field(default_factory=set)


def load_known_scholarship_index(client: Client, source: str) -> KnownScholarshipIndex:
    """
    Загрузить source_id, url, slug, title для source одним или несколькими range-запросами.
    """
    idx = KnownScholarshipIndex()
    batch = 1000
    offset = 0
    src = str(source).strip()
    while True:
        res = (
            client.table("scholarships")
            .select("source_id,url,slug,title")
            .eq("source", src)
            .range(offset, offset + batch - 1)
            .execute()
        )
        rows = res.data or []
        for row in rows:
            sid = row.get("source_id")
            if sid is not None and str(sid).strip() != "":
                idx.source_ids.add(str(sid).strip())
            u = row.get("url")
            if u is not None and str(u).strip() != "":
                idx.urls.add(str(u).strip())
            sl = row.get("slug")
            if sl is not None and str(sl).strip() != "":
                idx.slugs_lc.add(str(sl).strip().lower())
            tit = row.get("title")
            if tit is not None and str(tit).strip() != "":
                tn = _norm_text(tit)
                if len(tn) >= 12:
                    idx.titles_norm.add(tn)
        if len(rows) < batch:
            break
        offset += batch
    return idx


def listing_is_known(
    list_data: Mapping[str, Any],
    idx: KnownScholarshipIndex,
    *,
    title_fallback: bool = False,
) -> bool:
    """
    Уже есть в БД? Порядок: source_id → url → slug (нормализованный) → опционально title.
    """
    url = str(list_data.get("url") or "").strip()
    sid = str(list_data.get("source_id") or "").strip()
    if sid and sid in idx.source_ids:
        return True
    if url and url in idx.urls:
        return True
    if sid:
        sdl = sid.lower()
        if sdl in idx.slugs_lc:
            return True
    if title_fallback:
        tn = _norm_text(list_data.get("title"))
        if len(tn) >= 12 and tn in idx.titles_norm:
            return True
    return False


def _find_id_by_source_fingerprint(
    client: Client, source: str, fingerprint: str
) -> str | None:
    res = (
        client.table("scholarships")
        .select("id")
        .eq("source", source)
        .eq("text_fingerprint", fingerprint)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0]["id"] if rows else None


# Поля, которые нельзя массово передавать в update (id задаётся в .eq)
_SKIP_ON_WRITE = frozenset({"id"})


def upsert_scholarship(record: Mapping[str, Any]) -> dict[str, Any]:
    """
    Записать стипендию с логикой дедупликации:

    1) source + url
    2) иначе source + source_id (если source_id не пустой)
    3) иначе source + text_fingerprint (считаем сами через build_text_fingerprint)
    4) иначе INSERT

    Если строка найдена — UPDATE всех полей из record + last_seen_at (+ text_fingerprint).
    Поле updated_at в БД обновит триггер; last_seen_at задаём явно.
    """
    if "source" not in record or "url" not in record or "title" not in record:
        raise ValueError("В record обязательны ключи: source, url, title")

    from sources.shared_scholarship_ai import apply_scholarship_ai_finalization_if_enabled

    record = apply_scholarship_ai_finalization_if_enabled(dict(record))

    client = get_client()
    source = str(record["source"]).strip()
    url = str(record["url"]).strip()
    fingerprint = build_text_fingerprint(record)

    row_id: str | None = _find_id_by_source_url(client, source, url)

    if row_id is None:
        sid = record.get("source_id")
        if sid is not None and str(sid).strip() != "":
            row_id = _find_id_by_source_source_id(
                client, source, str(sid).strip()
            )

    if row_id is None:
        row_id = _find_id_by_source_fingerprint(client, source, fingerprint)

    now = _now_iso()
    raw_row = {k: v for k, v in dict(record).items() if k not in _SKIP_ON_WRITE}
    unknown = set(raw_row) - SCHOLARSHIP_UPSERT_PAYLOAD_KEYS
    if unknown:
        raise ValueError(
            "Неизвестные ключи в record (добавь колонку в БД и в "
            "scholarship_db_columns.SCHOLARSHIP_UPSERT_BODY_KEYS): "
            f"{sorted(unknown)}"
        )
    payload: dict[str, Any] = dict(raw_row)
    payload["source"] = source
    payload["url"] = url
    payload["text_fingerprint"] = fingerprint
    payload["last_seen_at"] = now

    if row_id is None:
        # Новая запись (created_at / updated_at по умолчанию из БД)
        res = client.table("scholarships").insert(payload).execute()
        rows = res.data or []
        if not rows:
            check = (
                client.table("scholarships")
                .select("*")
                .eq("source", source)
                .eq("url", url)
                .limit(1)
                .execute()
            )
            rows = check.data or []
        if not rows:
            raise RuntimeError("INSERT не вернул строку: проверь ответ API и RLS.")
        return rows[0]

    # Обновление существующей
    res = (
        client.table("scholarships").update(payload).eq("id", row_id).execute()
    )
    rows = res.data or []
    if not rows:
        check = (
            client.table("scholarships")
            .select("*")
            .eq("id", row_id)
            .limit(1)
            .execute()
        )
        rows = check.data or []
    if not rows:
        raise RuntimeError("UPDATE не удалось подтвердить.")
    return rows[0]
