from __future__ import annotations

import os
import time
from dataclasses import dataclass

import requests

TELEGRAM_MAX_TEXT = 4096


@dataclass(frozen=True)
class TelegramConfig:
    token: str
    admin_ids: list[str]
    enabled: bool

    @classmethod
    def load(cls) -> "TelegramConfig":
        token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
        admins_raw = (os.environ.get("TELEGRAM_ADMIN_IDS") or "").strip()
        admin_ids = [x.strip() for x in admins_raw.split(",") if x.strip()]
        enabled = bool(token and admin_ids)
        return cls(token=token, admin_ids=admin_ids, enabled=enabled)


def chunk_message(text: str, max_len: int = TELEGRAM_MAX_TEXT) -> list[str]:
    msg = (text or "").strip()
    if not msg:
        return []
    if len(msg) <= max_len:
        return [msg]

    chunks: list[str] = []
    remaining = msg
    while remaining:
        if len(remaining) <= max_len:
            chunks.append(remaining)
            break
        cut = remaining.rfind("\n", 0, max_len)
        if cut <= 0:
            cut = remaining.rfind(" ", 0, max_len)
        if cut <= 0:
            cut = max_len
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    return chunks


def _send_one(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            resp = requests.post(
                url,
                json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
                timeout=20,
            )
            if resp.ok:
                return
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.0 * (attempt + 1))
                continue
            raise RuntimeError(f"Telegram send failed ({resp.status_code}): {resp.text[:300]}")
        except Exception as exc:
            last_err = exc
            time.sleep(1.0 * (attempt + 1))
    if last_err:
        raise last_err


def send_report(message: str) -> None:
    cfg = TelegramConfig.load()
    if not cfg.enabled:
        print("Telegram report skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_ADMIN_IDS not configured.")
        return
    parts = chunk_message(message, TELEGRAM_MAX_TEXT)
    if not parts:
        return
    total = len(parts)
    for chat_id in cfg.admin_ids:
        for idx, part in enumerate(parts, start=1):
            payload = f"[{idx}/{total}]\n{part}" if total > 1 else part
            _send_one(cfg.token, chat_id, payload)
