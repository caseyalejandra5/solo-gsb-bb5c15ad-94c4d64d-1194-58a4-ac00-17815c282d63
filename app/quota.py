"""滚动窗口配额核心逻辑（纯函数，便于单测）。"""
from __future__ import annotations

import hashlib
import json


def merge_items(items: list[dict]) -> list[dict]:
    """合并同一桶的重复条目，并按 key 排序，得到规范化载荷。"""
    merged: dict[str, int] = {}
    for it in items:
        merged[it["key"]] = merged.get(it["key"], 0) + it["amount"]
    return [{"key": k, "amount": v} for k, v in sorted(merged.items())]


def payload_hash(items: list[dict]) -> str:
    """对规范化后的桶列表取哈希，用于幂等比对。"""
    canonical = json.dumps(items, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def earliest_retry_at(
    events: list[tuple[float, int]],
    limit: int,
    window: float,
    amount: int,
    now: float,
) -> float | None:
    """计算该桶最早可重试的 epoch 秒。

    events 为窗口内 (created_at, amount) 列表。窗口随事件过期而滚动释放，
    找到累计释放量足以容纳本次请求的最早过期时刻即可。
    返回 None 表示请求量本身超限，永远不可能成功。
    """
    if amount > limit:
        return None
    used = sum(a for _, a in events)
    if used + amount <= limit:
        return now
    need = used + amount - limit
    freed = 0
    for ts, a in sorted(events):
        freed += a
        if freed >= need:
            return ts + window
    return None  # pragma: no cover - 逻辑上不可达（need <= used）
