"""端到端测试：原子扣减、幂等、409、并发不超限、滚动窗口恢复。"""
import json
import os
import tempfile
import threading
import time
import uuid

# 必须在导入 app 之前设置环境（config 在导入时读取）
_TMP = tempfile.mkdtemp(prefix="quota-test-")
os.environ["QUOTA_DB_PATH"] = os.path.join(_TMP, "test.db")
os.environ["QUOTA_BUCKETS_CONFIG"] = os.path.join(_TMP, "buckets.json")

with open(os.environ["QUOTA_BUCKETS_CONFIG"], "w") as fh:
    json.dump(
        {"buckets": [{"key": "seeded", "limit": 100, "window_seconds": 60}]},
        fh,
    )

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

# 以上下文管理器方式启动，确保 lifespan（建表 + 播种桶配置）执行
_client_ctx = TestClient(app)
client = _client_ctx.__enter__()


def _rid() -> str:
    return uuid.uuid4().hex


def _bucket(limit: int, window: float) -> str:
    """为单个测试创建独立桶，避免测试间窗口互相干扰。"""
    key = f"b-{_rid()[:12]}"
    r = client.put(f"/v1/buckets/{key}", json={"limit": limit, "window_seconds": window})
    assert r.status_code == 200, r.text
    return key


def _consume(rid: str, key: str, amount: int):
    return client.post("/v1/quota/consume", json={
        "request_id": rid, "buckets": [{"key": key, "amount": amount}],
    })


def _release(rel_id: str, rid: str):
    return client.post("/v1/quota/releases", json={
        "release_id": rel_id, "request_id": rid,
    })


def test_seeded_bucket_and_consume():
    # 启动时从 QUOTA_BUCKETS_CONFIG 播种
    keys = [b["key"] for b in client.get("/v1/buckets").json()["buckets"]]
    assert "seeded" in keys

    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": "seeded", "amount": 30}],
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["allowed"] is True
    assert body["request_id"] == rid

    usage = client.get("/v1/buckets/seeded/usage").json()
    assert usage["used"] == 30
    assert usage["remaining"] == 70
    assert usage["limit"] == 100
    assert usage["window_seconds"] == 60


def test_atomic_all_or_nothing():
    """多桶请求中任一超限 → 全部不扣减。"""
    k1, k2 = _bucket(10, 60), _bucket(5, 60)
    r = client.post("/v1/quota/consume", json={
        "request_id": _rid(),
        "buckets": [{"key": k1, "amount": 1}, {"key": k2, "amount": 100}],
    })
    assert r.status_code == 429
    body = r.json()
    assert body["allowed"] is False
    assert [b["key"] for b in body["limited_buckets"]] == [k2]
    # 请求量本身超过限额 → 永远不可重试
    assert body["retry_at"] is None
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_denied_returns_retry_time():
    key = _bucket(2, 60)
    assert _consume(_rid(), key, 2).status_code == 200

    r = _consume(_rid(), key, 1)
    assert r.status_code == 429
    body = r.json()
    lb = body["limited_buckets"][0]
    assert lb["key"] == key
    assert lb["used"] == 2 and lb["requested"] == 1 and lb["limit"] == 2
    assert 0 < body["retry_after_seconds"] <= 60
    assert body["retry_at"] is not None
    assert "Retry-After" in r.headers


def test_idempotent_replay():
    key = _bucket(10, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 3}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200

    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.json() == r1.json()  # 原样返回固化判定
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    # 不得重复扣减
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_idempotent_replay_of_denied_decision():
    key = _bucket(2, 60)
    assert _consume(_rid(), key, 2).status_code == 200

    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 1}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 429
    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 429
    assert r2.json() == r1.json()
    assert r2.headers.get("X-Idempotent-Replay") == "true"


def test_conflict_on_changed_payload():
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 200
    r = _consume(rid, key, 2)
    assert r.status_code == 409


def test_same_amount_different_bucket_key_is_conflict():
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 1).status_code == 200
    assert _consume(rid, k2, 1).status_code == 409


def test_unknown_bucket_404():
    r = client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [{"key": "nope", "amount": 1}],
    })
    assert r.status_code == 404
    assert client.get("/v1/buckets/nope/usage").status_code == 404


def test_rolling_window_recovers():
    key = _bucket(2, 0.4)
    assert _consume(_rid(), key, 2).status_code == 200
    assert _consume(_rid(), key, 1).status_code == 429
    time.sleep(0.5)  # 窗口 0.4s，等待滚动过期
    assert _consume(_rid(), key, 1).status_code == 200


def test_rolling_window_partial_expiry():
    """窗口滚动时按事件过期逐步释放额度。"""
    key = _bucket(3, 0.6)
    assert _consume(_rid(), key, 2).status_code == 200
    time.sleep(0.35)
    assert _consume(_rid(), key, 1).status_code == 200  # 打满 3/3
    assert _consume(_rid(), key, 1).status_code == 429
    time.sleep(0.35)  # 第一笔(2)过期，释放 2 额度
    assert _consume(_rid(), key, 2).status_code == 200


def test_concurrent_requests_never_exceed_limit():
    """50 个并发请求争抢限额 10 的桶：放行数必须恰好为 10。"""
    key = _bucket(10, 60)
    results = []

    def hit():
        results.append(_consume(_rid(), key, 1).status_code)

    threads = [threading.Thread(target=hit) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(200) == 10, f"放行 {results.count(200)} 次，超出限额 10"
    assert results.count(429) == 40
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 10


def test_same_request_id_concurrent_single_deduction():
    """同一 request_id 并发提交 10 次：只应扣减一次。"""
    key = _bucket(100, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 1}]}
    codes = []

    def hit():
        codes.append(client.post("/v1/quota/consume", json=payload).status_code)

    threads = [threading.Thread(target=hit) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 1


def test_bucket_upsert_updates_config():
    key = _bucket(5, 60)
    r = client.put(f"/v1/buckets/{key}", json={"limit": 7, "window_seconds": 30})
    assert r.status_code == 200
    usage = client.get(f"/v1/buckets/{key}/usage").json()
    assert usage["limit"] == 7 and usage["window_seconds"] == 30


def test_validation_errors():
    key = _bucket(5, 60)
    # amount 必须为正
    assert _consume(_rid(), key, 0).status_code == 422
    # buckets 不能为空
    assert client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [],
    }).status_code == 422


# ---------------------------------------------------------------- 释放接口

def test_release_restores_all_buckets():
    """首次释放撤销对所有桶的用量贡献，额度恢复。"""
    k1, k2 = _bucket(10, 60), _bucket(20, 60)
    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k1, "amount": 4}, {"key": k2, "amount": 7}],
    })
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 4
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 7

    rel = _rid()
    rr = _release(rel, rid)
    assert rr.status_code == 200, rr.text
    body = rr.json()
    assert body["released"] is True
    assert body["release_id"] == rel and body["request_id"] == rid
    assert body["events_reverted"] == 2
    # 载荷经 merge_items 规范化后按 key 排序
    assert sorted(body["released_buckets"], key=lambda b: b["key"]) == sorted(
        [{"key": k1, "amount": 4}, {"key": k2, "amount": 7}],
        key=lambda b: b["key"],
    )
    assert "X-Idempotent-Replay" not in rr.headers

    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_release_frees_capacity_for_subsequent_consume():
    """释放后新的消费可以使用恢复出的额度。"""
    key = _bucket(3, 60)
    rid1 = _rid()
    assert _consume(rid1, key, 3).status_code == 200
    assert _consume(_rid(), key, 1).status_code == 429

    assert _release(_rid(), rid1).status_code == 200
    r = _consume(_rid(), key, 3)
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_release_idempotent_replay():
    """相同 release_id + 相同 request_id 重放：原样返回，不再改动用量。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200
    rel = _rid()
    r1 = _release(rel, rid)
    assert r1.status_code == 200

    # 再制造一笔用量，重放不应影响它
    assert _consume(_rid(), key, 3).status_code == 200
    r2 = _release(rel, rid)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_release_retarget_conflict():
    """相同 release_id 改指其他 request_id → 409，且不影响任一目标。"""
    key = _bucket(10, 60)
    rid1, rid2 = _rid(), _rid()
    assert _consume(rid1, key, 2).status_code == 200
    assert _consume(rid2, key, 3).status_code == 200
    rel = _rid()
    assert _release(rel, rid1).status_code == 200

    r = _release(rel, rid2)
    assert r.status_code == 409
    # rid1 已释放（-2），rid2 未被释放，用量只剩 3
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_double_release_same_consume_conflict():
    """同一消费请求只能由一个 release_id 成功释放，第二个返回 409。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 4).status_code == 200
    assert _release(_rid(), rid).status_code == 200

    r = _release(_rid(), rid)
    assert r.status_code == 409
    # 用量不会被重复"撤销"
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_unknown_request_404():
    r = _release(_rid(), f"missing-{_rid()}")
    assert r.status_code == 404


def test_release_denied_request_conflict():
    """原判定为拒绝（429）的消费不可释放 → 409。"""
    key = _bucket(1, 60)
    assert _consume(_rid(), key, 1).status_code == 200
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 429
    assert _release(_rid(), rid).status_code == 409
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 1


def test_consume_replay_after_release_returns_frozen_result():
    """释放后重放原 consume：仍返回已固化的放行结果，且不再次扣减。"""
    key = _bucket(10, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 6}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200
    assert _release(_rid(), rid).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0

    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    # 重放不得再次扣减
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_atomic_restores_every_bucket():
    """释放是原子的：多桶用量全部恢复，不存在部分恢复。"""
    keys = [_bucket(5, 60) for _ in range(5)]
    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k, "amount": 3} for k in keys],
    })
    assert r.status_code == 200
    assert _release(_rid(), rid).status_code == 200
    for k in keys:
        assert client.get(f"/v1/buckets/{k}/usage").json()["used"] == 0


def test_concurrent_releases_only_one_wins():
    """不同 release_id 并发释放同一消费：仅一个 200，其余 409。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200

    outcomes: list[tuple[str, int]] = []
    barrier = threading.Barrier(20)

    def hit(rel: str):
        barrier.wait()
        outcomes.append((rel, _release(rel, rid).status_code))

    threads = [threading.Thread(target=hit, args=(_rid(),)) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert [c for _, c in outcomes].count(200) == 1
    assert [c for _, c in outcomes].count(409) == 19
    # 只删除了一次事件，用量为 0 而非负数
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_concurrent_same_release_id_single_result():
    """同一 release_id 并发释放同一消费：全部 200，只执行一次撤销。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200

    rel = _rid()
    codes = []
    barrier = threading.Barrier(15)

    def hit():
        barrier.wait()
        codes.append(_release(rel, rid).status_code)

    threads = [threading.Thread(target=hit) for _ in range(15)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_then_new_consume_replay_interaction():
    """释放后：旧 consume 重放不扣减，新 request_id 可正常占用恢复额度。"""
    key = _bucket(5, 60)
    rid_old = _rid()
    assert _consume(rid_old, key, 5).status_code == 200
    assert _release(_rid(), rid_old).status_code == 200

    # 旧单重放不占额度
    assert _consume(rid_old, key, 5).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0
    # 新单可以打满恢复出的额度
    rid_new = _rid()
    assert _consume(rid_new, key, 5).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 5
    assert _consume(_rid(), key, 1).status_code == 429


def test_release_validation_errors():
    r = client.post("/v1/quota/releases", json={
        "release_id": "", "request_id": "x",
    })
    assert r.status_code == 422
    r = client.post("/v1/quota/releases", json={"release_id": "r1"})
    assert r.status_code == 422
