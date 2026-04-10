from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any

INPUT_COST_PER_1M = 2.50
OUTPUT_COST_PER_1M = 15.00


@dataclass
class AiUsageSnapshot:
    api_calls: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    reused: int = 0
    skipped: int = 0
    errors: int = 0
    estimated_cost_usd: float = 0.0


_lock = Lock()
_state = AiUsageSnapshot()


def _safe_int(value: Any) -> int:
    try:
        num = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, num)


def snapshot_ai_usage() -> AiUsageSnapshot:
    with _lock:
        return AiUsageSnapshot(**vars(_state))


def record_ai_completion(usage: Any) -> None:
    prompt_tokens = _safe_int(getattr(usage, "prompt_tokens", None))
    completion_tokens = _safe_int(getattr(usage, "completion_tokens", None))
    total_tokens = _safe_int(getattr(usage, "total_tokens", None))
    if total_tokens <= 0:
        total_tokens = prompt_tokens + completion_tokens
    estimated_cost = (
        (prompt_tokens / 1_000_000.0) * INPUT_COST_PER_1M
        + (completion_tokens / 1_000_000.0) * OUTPUT_COST_PER_1M
    )
    with _lock:
        _state.api_calls += 1
        _state.prompt_tokens += prompt_tokens
        _state.completion_tokens += completion_tokens
        _state.total_tokens += total_tokens
        _state.estimated_cost_usd += estimated_cost


def record_ai_reuse() -> None:
    with _lock:
        _state.reused += 1


def record_ai_skip() -> None:
    with _lock:
        _state.skipped += 1


def record_ai_error() -> None:
    with _lock:
        _state.errors += 1


def diff_ai_usage(start: AiUsageSnapshot, end: AiUsageSnapshot | None = None) -> AiUsageSnapshot:
    finish = end or snapshot_ai_usage()
    return AiUsageSnapshot(
        api_calls=max(0, finish.api_calls - start.api_calls),
        prompt_tokens=max(0, finish.prompt_tokens - start.prompt_tokens),
        completion_tokens=max(0, finish.completion_tokens - start.completion_tokens),
        total_tokens=max(0, finish.total_tokens - start.total_tokens),
        reused=max(0, finish.reused - start.reused),
        skipped=max(0, finish.skipped - start.skipped),
        errors=max(0, finish.errors - start.errors),
        estimated_cost_usd=max(0.0, finish.estimated_cost_usd - start.estimated_cost_usd),
    )


def print_ai_session_summary(
    parser_name: str,
    *,
    processed: int,
    new_found: int,
    start: AiUsageSnapshot,
) -> None:
    diff = diff_ai_usage(start)
    print("")
    print(f"{parser_name} AI session summary:")
    print(f"  processed: {processed}")
    print(f"  new found: {new_found}")
    print(f"  ai api calls: {diff.api_calls}")
    print(f"  ai reused from existing: {diff.reused}")
    print(f"  ai skipped: {diff.skipped}")
    print(f"  ai errors: {diff.errors}")
    print(
        "  token usage: "
        f"prompt={diff.prompt_tokens}, completion={diff.completion_tokens}, total={diff.total_tokens}"
    )
    print(f"  estimated ai cost: ${diff.estimated_cost_usd:.4f}")
