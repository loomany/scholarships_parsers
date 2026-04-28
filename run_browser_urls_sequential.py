"""
Последовательный headful-прогон произвольных URL в Playwright (аналогия Bold: браузер + сеть).

Использование:
  python run_browser_urls_sequential.py
  python run_browser_urls_sequential.py --url https://example.com/a --url https://example.com/b

Переменные:
  BROWSER_PROBE_HEADLESS=1 — без окна (по умолчанию 0, как BOLD_HEADLESS в run_all).
  BROWSER_PROBE_DWELL_MS=8000 — сколько ждать на странице после load.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BASE_DIR)

from dotenv import load_dotenv

load_dotenv(os.path.join(_BASE_DIR, ".env"))
load_dotenv(os.path.join(_BASE_DIR, "..", ".env"))

DEFAULT_URLS: tuple[str, ...] = (
    "https://sfs.virginia.edu/financial-aid-current-students/current-undergraduate-students/financial-aid-basics/types-aid/scholarships-grants",
    "https://www.wgu.edu/financial-aid-tuition/scholarships.html",
)


def _bool_env(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def main() -> None:
    from playwright.sync_api import sync_playwright

    urls: list[str]
    if len(sys.argv) > 1:
        import argparse

        p = argparse.ArgumentParser(description="Playwright: URLs one after another (Bold-like headful probe).")
        p.add_argument("--url", action="append", dest="urls", help="URL to visit (repeatable)")
        args = p.parse_args()
        urls = [u.strip() for u in (args.urls or []) if u and u.strip()]
        if not urls:
            print("No --url given; use --url for each target or run without args for built-in two sites.")
            raise SystemExit(2)
    else:
        urls = list(DEFAULT_URLS)

    headless = _bool_env("BROWSER_PROBE_HEADLESS", False)
    dwell_ms = max(1000, _int_env("BROWSER_PROBE_DWELL_MS", 8000))

    out_dir = Path(_BASE_DIR) / "discovery_outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = out_dir / f"browser_probe_{ts}.json"

    per_site: list[dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(60_000)

        json_hits: list[str] = []

        def on_response(response) -> None:  # type: ignore[no-untyped-def]
            try:
                ct = (response.headers.get("content-type") or "").lower()
                if "application/json" not in ct:
                    return
                u = response.url
                if u not in json_hits:
                    json_hits.append(u)
            except Exception:
                pass

        page.on("response", on_response)

        try:
            for i, url in enumerate(urls, start=1):
                json_hits.clear()
                host = urlparse(url).netloc or url
                print("")
                print(f"========== [{i}/{len(urls)}] {host} ==========")
                print(f"goto: {url}")
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(dwell_ms)
                title = ""
                try:
                    title = (page.title() or "").strip()
                except Exception:
                    title = ""
                final_url = ""
                try:
                    final_url = page.url
                except Exception:
                    final_url = url
                print(f"title: {title[:120]}")
                print(f"final_url: {final_url}")
                print(f"application/json responses seen: {len(json_hits)}")
                for j, ju in enumerate(json_hits[:25], start=1):
                    print(f"  json[{j}]: {ju[:200]}{'…' if len(ju) > 200 else ''}")
                if len(json_hits) > 25:
                    print(f"  ... and {len(json_hits) - 25} more")
                per_site.append(
                    {
                        "requested_url": url,
                        "final_url": final_url,
                        "title": title,
                        "json_response_urls": list(json_hits),
                    }
                )
                print(f"========== end {i}/{len(urls)} ==========")
        finally:
            context.close()
            browser.close()

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "headless": headless,
        "dwell_ms": dwell_ms,
        "sites": per_site,
    }
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("")
    print(f"Report written: {report_path}")


if __name__ == "__main__":
    main()
