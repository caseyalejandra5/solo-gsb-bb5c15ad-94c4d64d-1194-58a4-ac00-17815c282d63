# Rolling Quota API（多桶滚动配额判定服务）

基于 **Python + FastAPI + SQLite** 的本地配额判定服务：一次请求可携带多个桶键，
在**同一事务**中完成全部桶的检查与扣减——任一超限则全部不扣减，并返回受限桶与最早重试时间。

## 核心特性

- **滚动窗口**：每个桶独立配置限额与窗口（秒），窗口内用量随事件过期滚动释放
- **原子扣减**：单事务内检查 + 扣减全部桶，任一超限则整体不扣减（all-or-nothing）
- **配额释放**：`POST /v1/quota/releases` 在单事务内撤销原消费请求对**全部桶**的用量贡献，
  不存在部分桶恢复；`release_id` 同键同目标重放原样返回，改指其他目标返回 **409**
- **幂等判定**：`request_id` 首次出现时固化载荷与判定；相同内容重放原样返回
  （响应头 `X-Idempotent-Replay: true`），内容变化返回 **409**；
  释放后重放仍返回固化结果且不会再次扣减
- **并发安全**：SQLite `BEGIN IMMEDIATE` 串行化写事务，并发请求不会突破限额；
  同一消费请求并发释放时仅一个 `release_id` 成功
- **用量查询**：随时查询桶当前窗口用量

## 快速启动

```bash
docker compose up --build
```

单条命令完成构建、依赖安装与初始化（建表 + 从 `buckets.json` 播种桶配置），
服务监听 **8000** 端口，数据持久化在 `quota-data` 卷。

无 Docker 时本地运行：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 桶配置

启动时从 `buckets.json`（可用环境变量 `QUOTA_BUCKETS_CONFIG` 替换）播种，同名桶按文件内容更新：

```json
{
  "buckets": [
    {"key": "api-requests", "limit": 100, "window_seconds": 60},
    {"key": "llm-tokens",   "limit": 10000, "window_seconds": 60},
    {"key": "heavy-jobs",   "limit": 5,   "window_seconds": 300}
  ]
}
```

运行时也可动态创建/更新：`PUT /v1/buckets/{key}`，请求体 `{"limit": 100, "window_seconds": 60}`。

## 示例请求

### 1. 配额判定（检查并扣减）

```bash
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 1},
                   {"key": "llm-tokens",  "amount": 500}]}'
```

放行（`200`）：

```json
{"request_id": "req-001", "allowed": true,
 "granted": [{"key": "api-requests", "amount": 1},
             {"key": "llm-tokens", "amount": 500}],
 "decided_at": 1789657657.47}
```

超限（`429`，含 `Retry-After` 响应头；所有桶均未扣减）：

```json
{"request_id": "req-002", "allowed": false,
 "limited_buckets": [{"key": "llm-tokens", "limit": 10000, "window_seconds": 60,
                      "used": 9950, "requested": 500,
                      "retry_at": 1789657660.12, "retry_after_seconds": 2.65}],
 "retry_at": 1789657660.12, "retry_after_seconds": 2.65,
 "decided_at": 1789657657.47}
```

> `retry_at` / `retry_after_seconds` 为 `null` 表示请求量本身超过限额，永远不可能放行。

### 2. 幂等语义

```bash
# 重复提交相同内容 → 原样返回首次判定，不重复扣减
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 1},
                   {"key": "llm-tokens",  "amount": 500}]}'

# 相同 request_id 但内容变化 → 409 Conflict
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 2}]}'
```

### 3. 释放配额（撤销已放行的消费）

对已放行的消费请求，可凭独立的 `release_id` 撤销它对所有桶的用量贡献：

```bash
curl -X POST http://localhost:8000/v1/quota/releases \
  -H 'Content-Type: application/json' \
  -d '{"release_id": "rel-001", "request_id": "req-001"}'
```

成功（`200`，单事务内撤销该请求的全部用量事件）：

```json
{"release_id": "rel-001", "request_id": "req-001",
 "released": true,
 "released_buckets": [{"key": "api-requests", "amount": 1},
                      {"key": "llm-tokens", "amount": 500}],
 "events_released": 2, "released_at": 1789657700.12}
```

释放语义：

- **同 `release_id` + 同 `request_id` 重放**：原样返回首次结果，
  响应头 `X-Idempotent-Replay: true`，不会重复撤销；
- **同 `release_id` 改指其他 `request_id`**：`409 Conflict`；
- **同一 `request_id` 被另一个 `release_id` 释放**（含并发竞争落败者）：`409 Conflict`，
  即一个消费请求只能被成功释放一次；
- **原 `request_id` 不存在**：`404 Not Found`；
- **原判定为拒绝（429）**：未产生用量，释放返回 `409 Conflict`；
- **释放后重放原 `consume`**：仍返回首次固化的判定（带 `X-Idempotent-Replay: true`），
  但用量不会被再次扣减。

```bash
# 重复释放（幂等重放）
curl -X POST http://localhost:8000/v1/quota/releases \
  -H 'Content-Type: application/json' \
  -d '{"release_id": "rel-001", "request_id": "req-001"}'
# → 200，响应头 X-Idempotent-Replay: true

# release_id 改指其他请求 → 409
curl -X POST http://localhost:8000/v1/quota/releases \
  -H 'Content-Type: application/json' \
  -d '{"release_id": "rel-001", "request_id": "req-002"}'
```

### 4. 查询桶当前窗口用量

```bash
curl http://localhost:8000/v1/buckets/llm-tokens/usage
```

```json
{"key": "llm-tokens", "limit": 10000, "window_seconds": 60,
 "used": 500, "remaining": 9500,
 "window_start": 1789657597.55, "as_of": 1789657657.55,
 "oldest_event_expires_at": 1789657717.47}
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/v1/quota/consume` | 多桶检查 + 扣减；`200` 放行 / `429` 超限 / `409` 幂等冲突 / `404` 桶未配置 |
| `POST` | `/v1/quota/releases` | 释放原消费请求的全部桶用量；`200` 成功（重放带 `X-Idempotent-Replay`）/ `409` 改目标·重复释放·原请求被拒 / `404` 原请求不存在 |
| `GET` | `/v1/buckets/{key}/usage` | 查询桶当前滚动窗口用量 |
| `PUT` | `/v1/buckets/{key}` | 创建或更新桶（`limit`、`window_seconds`） |
| `GET` | `/v1/buckets` | 列出全部桶配置 |
| `GET` | `/healthz` | 健康检查 |

交互式文档：`http://localhost:8000/docs`

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `QUOTA_DB_PATH` | `quota.db`（镜像内 `/data/quota.db`） | SQLite 数据库文件路径 |
| `QUOTA_BUCKETS_CONFIG` | `buckets.json`（镜像内 `/app/buckets.json`） | 启动时播种的桶配置文件，置空跳过 |
| `QUOTA_BUSY_TIMEOUT_MS` | `30000` | SQLite 写事务排队等待毫秒数 |

## 实现说明

- **原子性**：`consume` 在单个 `BEGIN IMMEDIATE` 事务内完成"读窗口用量 → 判定 →
  写扣减事件 → 固化判定"，任一环节失败整体回滚。
- **释放原子性**：`release` 在同一个 `BEGIN IMMEDIATE` 事务内完成
  "校验原请求 → `DELETE` 该 `request_id` 的全部用量事件 → 写释放记录"，
  失败整体回滚，不会出现部分桶已恢复。
- **唯一释放**：`releases` 表以 `release_id` 为主键（同键重放/改目标检测），
  并在 `request_id` 上建唯一索引；并发竞争中落败的插入触发 `IntegrityError`，
  映射为 409，保证一个消费请求只能被一个 `release_id` 成功释放。
- **释放后重放**：释放只删除 `usage_events` 中的事件，`requests` 中固化的判定不变，
  因此重放 `consume` 仍原样返回固化结果但不再写入用量；释放出的额度可被新请求使用。
- **并发**：`BEGIN IMMEDIATE` 使写事务在数据库级串行化；WAL 模式保证读（用量查询）不阻塞。
- **幂等**：`requests` 表以 `request_id` 为主键，固化载荷 SHA-256 与判定 JSON；
  重放命中时直接返回，哈希不一致返回 409。
- **最早重试时间**：按窗口内事件过期时刻累计释放量，找到最早可容纳本次请求的过期点；
  多桶受限时取各桶的最大值。

## 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest tests/ -q
```

覆盖：原子扣减、幂等重放（含拒绝判定重放）、409 冲突、滚动窗口过期恢复、
50 并发不超限额、同 `request_id` 并发只扣减一次，以及配额释放的全桶恢复、
同键重放、改目标 409、重复释放 409、原请求 404/拒绝 409、释放后 consume 重放、
10 并发释放仅一个成功、同 `release_id` 并发重放等 26 个用例。
