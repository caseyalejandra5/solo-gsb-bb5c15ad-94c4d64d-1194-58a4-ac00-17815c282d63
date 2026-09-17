"""SQLite 连接与 schema 初始化。

设计要点：
- WAL 模式：读不阻塞写，写之间通过 BEGIN IMMEDIATE 串行化；
- busy_timeout：并发写事务排队等待，避免 "database is locked" 直接失败；
- isolation_level=None（自动提交），事务边界由代码显式控制。
"""
from __future__ import annotations

import json
import os
import sqlite3

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS buckets (
    key             TEXT PRIMARY KEY,
    limit_amount    INTEGER NOT NULL CHECK (limit_amount > 0),
    window_seconds  REAL    NOT NULL CHECK (window_seconds > 0)
);

-- 滚动窗口用量事件：每次放行写入一条，窗口内求和即为当前用量
CREATE TABLE IF NOT EXISTS usage_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_key  TEXT    NOT NULL REFERENCES buckets (key),
    request_id  TEXT    NOT NULL,
    amount      INTEGER NOT NULL CHECK (amount > 0),
    created_at  REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_events_bucket_ts
    ON usage_events (bucket_key, created_at);

-- 幂等表：request_id 首次出现时固化载荷哈希与判定结果
-- release_id 非空表示该消费已被对应释放单成功释放
CREATE TABLE IF NOT EXISTS requests (
    request_id   TEXT PRIMARY KEY,
    payload_hash TEXT    NOT NULL,
    payload      TEXT    NOT NULL,
    decision     TEXT    NOT NULL,
    allowed      INTEGER NOT NULL,
    release_id   TEXT,
    created_at   REAL    NOT NULL
);

-- 释放幂等表：release_id 首次成功释放时固化目标 request_id 与释放结果
CREATE TABLE IF NOT EXISTS releases (
    release_id  TEXT PRIMARY KEY,
    request_id  TEXT    NOT NULL,
    result      TEXT    NOT NULL,
    created_at  REAL    NOT NULL
);
"""

# 旧库增量迁移：为已存在的 requests 表补充 release_id 列
_MIGRATIONS = (
    (
        "requests",
        "release_id",
        "ALTER TABLE requests ADD COLUMN release_id TEXT",
    ),
)


def connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or config.DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA busy_timeout={config.BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str | None = None) -> None:
    path = db_path or config.DB_PATH
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        _apply_migrations(conn)
    finally:
        conn.close()


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """对旧版本数据库执行增量列迁移（SQLite 无 ADD COLUMN IF NOT EXISTS）。"""
    for table, column, ddl in _MIGRATIONS:
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            conn.execute(ddl)


def seed_buckets(config_path: str, db_path: str | None = None) -> list[str]:
    """从 JSON 文件播种桶配置；同名桶按文件内容更新（配置文件即事实源）。"""
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    entries = raw["buckets"] if isinstance(raw, dict) else raw

    conn = connect(db_path)
    seeded: list[str] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        for e in entries:
            conn.execute(
                """
                INSERT INTO buckets (key, limit_amount, window_seconds)
                VALUES (?, ?, ?)
                ON CONFLICT (key) DO UPDATE SET
                    limit_amount   = excluded.limit_amount,
                    window_seconds = excluded.window_seconds
                """,
                (e["key"], int(e["limit"]), float(e["window_seconds"])),
            )
            seeded.append(e["key"])
        conn.execute("COMMIT")
    except Exception:
        _rollback_quietly(conn)
        raise
    finally:
        conn.close()
    return seeded


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass
