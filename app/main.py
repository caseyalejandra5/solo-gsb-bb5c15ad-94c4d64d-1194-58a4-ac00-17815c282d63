"""FastAPI 入口：多桶滚动配额判定 API。

核心保证：
- 同一请求的所有桶在单个 BEGIN IMMEDIATE 事务内"检查 + 扣减"，
  任一超限则整体不扣减（原子性）；
- BEGIN IMMEDIATE 使写事务串行化，并发请求不会突破限额；
- request_id 首次出现时固化载荷与判定，重复提交原样返回，
  载荷变化返回 409；
- release_id 首次释放时在单事务内撤销原请求对全部桶的用量贡献，
  同键同目标重放原样返回，改指其他 request_id 返回 409；
- 同一消费请求只能被一个 release_id 成功释放（releases.request_id 唯一约束
  兜底并发竞争）；释放后重放 consume 仍返回固化判定且不再扣减。
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from . import config
from .db import _rollback_quietly, connect, init_db, seed_buckets
from .quota import earliest_retry_at, merge_items, payload_hash
from .schemas import BucketConfig, ConsumeRequest, ReleaseRequest

logger = logging.getLogger("quota")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if config.BUCKETS_CONFIG and os.path.exists(config.BUCKETS_CONFIG):
        seeded = seed_buckets(config.BUCKETS_CONFIG)
        logger.info("seeded buckets from %s: %s", config.BUCKETS_CONFIG, seeded)
    yield


app = FastAPI(title="Rolling Quota API", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------- 配额判定

@app.post("/v1/quota/consume")
def consume(req: ConsumeRequest):
    """检查并扣减一组桶；任一超限则全部不扣减。

    - 200：全部放行，已扣减；
    - 429：至少一个桶超限，返回受限桶与最早重试时间，未做任何扣减；
    - 409：request_id 已存在但载荷不同；
    - 404：存在未配置的桶。
    """
    now = time.time()
    items = merge_items([i.model_dump() for i in req.buckets])
    phash = payload_hash(items)
    payload_json = json.dumps(
        {"request_id": req.request_id, "buckets": items}, sort_keys=True
    )

    replayed = False
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT payload_hash, decision FROM requests WHERE request_id = ?",
                (req.request_id,),
            ).fetchone()

            if row is not None:
                # 幂等：同 request_id 直接返回已固化的判定，不再扣减
                if row["payload_hash"] != phash:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"request_id '{req.request_id}' 已被不同载荷使用；"
                            "幂等键不可复用，请更换 request_id"
                        ),
                    )
                decision = json.loads(row["decision"])
                replayed = True
                conn.execute("COMMIT")
            else:
                decision = _evaluate_and_consume(conn, req.request_id, items, now)
                conn.execute(
                    "INSERT INTO requests "
                    "(request_id, payload_hash, payload, decision, allowed, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        req.request_id,
                        phash,
                        payload_json,
                        json.dumps(decision),
                        1 if decision["allowed"] else 0,
                        now,
                    ),
                )
                conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return _decision_response(decision, replayed)


# ---------------------------------------------------------------- 配额释放

@app.post("/v1/quota/releases")
def release(req: ReleaseRequest):
    """撤销原消费请求对所有桶的用量贡献（释放配额）。

    - 200：首次释放成功，原请求的全部用量事件已在同一事务内删除；
    - 同 release_id + 同 request_id 重放：原样返回首次结果，
      响应头 X-Idempotent-Replay: true；
    - 409：release_id 已指向其他 request_id；原请求判定为拒绝（无用量可释放）；
      该 request_id 已被其他 release_id 释放（含并发竞争中的落败者）；
    - 404：原 request_id 不存在。
    """
    now = time.time()

    replayed = False
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT request_id, result FROM releases WHERE release_id = ?",
                (req.release_id,),
            ).fetchone()

            if existing is not None:
                # 幂等：同 release_id 必须指向同一 request_id，原样返回
                if existing["request_id"] != req.request_id:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"release_id '{req.release_id}' 已用于释放 "
                            f"request_id '{existing['request_id']}'，不可改指其他目标"
                        ),
                    )
                result = json.loads(existing["result"])
                replayed = True
                conn.execute("COMMIT")
            else:
                result = _release_and_record(conn, req.release_id, req.request_id, now)
                conn.execute("COMMIT")
        except HTTPException:
            _rollback_quietly(conn)
            raise
        except sqlite3.IntegrityError:
            # request_id 唯一约束：并发竞争中另一个 release_id 已抢先成功
            _rollback_quietly(conn)
            raise HTTPException(
                status_code=409,
                detail=(
                    f"request_id '{req.request_id}' 已被其他 release_id 释放；"
                    "同一消费请求只能释放一次"
                ),
            )
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return JSONResponse(
        status_code=200,
        content=result,
        headers={"X-Idempotent-Replay": "true"} if replayed else None,
    )


def _release_and_record(
    conn: sqlite3.Connection, release_id: str, request_id: str, now: float
) -> dict:
    """在事务内撤销原请求的全部用量并固化释放结果。

    删除用量事件与写入 releases 记录同事务提交：不可能出现部分桶已恢复。
    """
    row = conn.execute(
        "SELECT allowed, payload FROM requests WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"request_id '{request_id}' 不存在，无可释放的消费请求",
        )
    if not row["allowed"]:
        raise HTTPException(
            status_code=409,
            detail=f"request_id '{request_id}' 的原判定为拒绝，未产生用量，无需释放",
        )

    # 撤销该请求对所有桶的用量贡献；固化载荷即释放明细
    deleted = conn.execute(
        "DELETE FROM usage_events WHERE request_id = ?", (request_id,)
    ).rowcount
    payload = json.loads(row["payload"])
    released_buckets = payload["buckets"]

    result = {
        "release_id": release_id,
        "request_id": request_id,
        "released": True,
        "released_buckets": released_buckets,
        "events_released": deleted,
        "released_at": now,
    }
    conn.execute(
        "INSERT INTO releases (release_id, request_id, result, created_at) "
        "VALUES (?, ?, ?, ?)",
        (release_id, request_id, json.dumps(result), now),
    )
    return result


def _evaluate_and_consume(
    conn: sqlite3.Connection, request_id: str, items: list[dict], now: float
) -> dict:
    """在事务内检查全部桶；全部通过才写入扣减事件。"""
    limited: list[dict] = []

    for item in items:
        bucket = conn.execute(
            "SELECT limit_amount, window_seconds FROM buckets WHERE key = ?",
            (item["key"],),
        ).fetchone()
        if bucket is None:
            raise HTTPException(
                status_code=404,
                detail=f"桶 '{item['key']}' 未配置，请先通过 PUT /v1/buckets/{item['key']} 创建",
            )

        limit = bucket["limit_amount"]
        window = bucket["window_seconds"]
        events = [
            (r["created_at"], r["amount"])
            for r in conn.execute(
                "SELECT created_at, amount FROM usage_events "
                "WHERE bucket_key = ? AND created_at > ? ORDER BY created_at",
                (item["key"], now - window),
            )
        ]
        used = sum(a for _, a in events)

        if used + item["amount"] > limit:
            retry_at = earliest_retry_at(events, limit, window, item["amount"], now)
            limited.append(
                {
                    "key": item["key"],
                    "limit": limit,
                    "window_seconds": window,
                    "used": used,
                    "requested": item["amount"],
                    "retry_at": retry_at,
                    "retry_after_seconds": (
                        None if retry_at is None else max(0.0, retry_at - now)
                    ),
                }
            )

    if limited:
        # 任一超限：全部不扣减；整体最早重试时间取各受限桶的最大值
        retry_times = [b["retry_at"] for b in limited]
        overall_retry_at = (
            max(retry_times) if all(t is not None for t in retry_times) else None
        )
        return {
            "request_id": request_id,
            "allowed": False,
            "limited_buckets": limited,
            "retry_at": overall_retry_at,
            "retry_after_seconds": (
                None if overall_retry_at is None else max(0.0, overall_retry_at - now)
            ),
            "decided_at": now,
        }

    # 全部通过：同事务写入所有桶的扣减事件
    for item in items:
        conn.execute(
            "INSERT INTO usage_events (bucket_key, request_id, amount, created_at) "
            "VALUES (?, ?, ?, ?)",
            (item["key"], request_id, item["amount"], now),
        )
    return {
        "request_id": request_id,
        "allowed": True,
        "granted": items,
        "decided_at": now,
    }


def _decision_response(decision: dict, replayed: bool) -> JSONResponse:
    headers = {}
    if replayed:
        headers["X-Idempotent-Replay"] = "true"
    if decision["allowed"]:
        return JSONResponse(status_code=200, content=decision, headers=headers)
    if decision.get("retry_after_seconds") is not None:
        headers["Retry-After"] = str(math.ceil(decision["retry_after_seconds"]))
    return JSONResponse(status_code=429, content=decision, headers=headers)


# ---------------------------------------------------------------- 桶管理

@app.put("/v1/buckets/{key}", status_code=200)
def upsert_bucket(key: str, cfg: BucketConfig):
    """创建或更新桶的限额与窗口。"""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO buckets (key, limit_amount, window_seconds)
            VALUES (?, ?, ?)
            ON CONFLICT (key) DO UPDATE SET
                limit_amount   = excluded.limit_amount,
                window_seconds = excluded.window_seconds
            """,
            (key, cfg.limit, cfg.window_seconds),
        )
        conn.execute("COMMIT")
    except Exception:
        _rollback_quietly(conn)
        raise
    finally:
        conn.close()
    return {"key": key, "limit": cfg.limit, "window_seconds": cfg.window_seconds}


@app.get("/v1/buckets")
def list_buckets():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT key, limit_amount, window_seconds FROM buckets ORDER BY key"
        ).fetchall()
    finally:
        conn.close()
    return {
        "buckets": [
            {"key": r["key"], "limit": r["limit_amount"], "window_seconds": r["window_seconds"]}
            for r in rows
        ]
    }


@app.get("/v1/buckets/{key}/usage")
def bucket_usage(key: str):
    """查询桶当前滚动窗口用量。"""
    now = time.time()
    conn = connect()
    try:
        bucket = conn.execute(
            "SELECT limit_amount, window_seconds FROM buckets WHERE key = ?", (key,)
        ).fetchone()
        if bucket is None:
            raise HTTPException(status_code=404, detail=f"桶 '{key}' 未配置")
        window = bucket["window_seconds"]
        events = conn.execute(
            "SELECT created_at, amount FROM usage_events "
            "WHERE bucket_key = ? AND created_at > ? ORDER BY created_at",
            (key, now - window),
        ).fetchall()
    finally:
        conn.close()

    used = sum(r["amount"] for r in events)
    oldest = events[0]["created_at"] if events else None
    return {
        "key": key,
        "limit": bucket["limit_amount"],
        "window_seconds": window,
        "used": used,
        "remaining": max(0, bucket["limit_amount"] - used),
        "window_start": now - window,
        "as_of": now,
        "oldest_event_expires_at": None if oldest is None else oldest + window,
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
