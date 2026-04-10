from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any

PREFILTER_PASS = "prefilter_pass"
PREFILTER_REJECT_KNOWN = "prefilter_reject_known"
PREFILTER_REJECT_MAPPING = "prefilter_reject_mapping"
PREFILTER_REJECT_FUNDING = "prefilter_reject_funding"
PREFILTER_REJECT_DEADLINE = "prefilter_reject_deadline"

_STORE_VERSION = 1


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _entry_key(source_id: str | None, url: str | None, title: str | None) -> str:
    sid = str(source_id or "").strip()
    if sid:
        return sid
    u = str(url or "").strip()
    if u:
        return u
    return str(title or "").strip()


class BoldPrefilterStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self.entries: dict[str, dict[str, Any]] = {}

    def load(self) -> None:
        if not self.path or not os.path.isfile(self.path):
            self.entries = {}
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            self.entries = {}
            return
        if not isinstance(data, dict):
            self.entries = {}
            return
        entries = data.get("entries")
        self.entries = entries if isinstance(entries, dict) else {}

    def save(self) -> None:
        if not self.path:
            return
        parent = os.path.dirname(os.path.abspath(self.path))
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        payload = {"version": _STORE_VERSION, "entries": self.entries}
        fd, tmp = tempfile.mkstemp(suffix=".json", dir=parent or None, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
        except OSError:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def upsert_candidate(
        self,
        *,
        source_id: str | None,
        url: str | None,
        title: str | None,
        response_url: str,
        snapshot_hash: str,
        prefilter_status: str,
        prefilter_reason: str,
        item_snapshot: dict[str, Any],
    ) -> None:
        key = _entry_key(source_id, url, title)
        if not key:
            return
        prev = self.entries.get(key) if isinstance(self.entries.get(key), dict) else {}
        self.entries[key] = {
            "source": "bold_org",
            "source_id": str(source_id or "").strip() or None,
            "url": str(url or "").strip() or None,
            "title": str(title or "").strip() or None,
            "response_url": response_url,
            "snapshot_hash": snapshot_hash,
            "prefilter_status": prefilter_status,
            "prefilter_reason": prefilter_reason,
            "last_seen_at": _utc_now_iso(),
            "processed_snapshot_hash": prev.get("processed_snapshot_hash"),
            "processed_at": prev.get("processed_at"),
            "item_snapshot": item_snapshot,
        }

    def iter_deep_candidates(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for entry in self.entries.values():
            if not isinstance(entry, dict):
                continue
            if str(entry.get("prefilter_status") or "") != PREFILTER_PASS:
                continue
            snapshot_hash = str(entry.get("snapshot_hash") or "")
            processed_hash = str(entry.get("processed_snapshot_hash") or "")
            if snapshot_hash and snapshot_hash == processed_hash:
                continue
            out.append(entry)
        return out

    def mark_processed(self, entry: dict[str, Any]) -> None:
        key = _entry_key(
            entry.get("source_id"),
            entry.get("url"),
            entry.get("title"),
        )
        if not key or key not in self.entries:
            return
        current = self.entries[key]
        current["processed_snapshot_hash"] = current.get("snapshot_hash")
        current["processed_at"] = _utc_now_iso()
