"""Flask 接口层测试：稳定编号、有效性、直接依据与错误反馈均经真实 HTTP 语义。"""

from __future__ import annotations

import json

import pytest

from app.server import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "api.db"))
    app.testing = True
    return app.test_client()


def post(client, path, body):
    return client.post(path, data=json.dumps(body),
                       content_type="application/json")


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "ok"


def test_index_page_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "低温探测器标定谱系".encode() in resp.data


def test_full_lifecycle_over_http(client):
    # 原始记录 -> 稳定编号
    r1 = post(client, "/api/records",
              {"kind": "raw", "payload": {"value": "77K channel-0"}})
    assert r1.status_code == 201
    assert r1.get_json()["id"] == "R000001"

    r2 = post(client, "/api/records",
              {"kind": "raw", "payload": {"value": "4K channel-1"}})
    # 推导记录引用两个有效前序
    d = post(client, "/api/records", {
        "kind": "derived",
        "payload": {"value": "gain calibration"},
        "parent_ids": ["R000001", "R000002"],
    })
    assert d.status_code == 201
    body = d.get_json()
    assert body["parent_ids"] == ["R000001", "R000002"]
    assert body["status"] == "valid"

    # 列表展示稳定编号 / 有效性 / 直接依据
    listed = client.get("/api/records").get_json()
    assert len(listed) == 3
    assert {r["id"] for r in listed} == {"R000001", "R000002", "R000003"}

    # 级联失效
    inv = post(client, "/api/records/R000001/invalidate",
               {"operation_id": "http-op-1"})
    assert inv.status_code == 200
    cascaded = {c["id"] for c in inv.get_json()["cascade"]}
    assert cascaded == {"R000001", "R000003"}

    r1_after = client.get("/api/records/R000001").get_json()
    assert r1_after["status"] == "invalid"
    assert r1_after["invalidated_by"] == "R000001"
    r3_after = client.get("/api/records/R000003").get_json()
    assert r3_after["invalidated_by"] == "R000001"
    # R2 不受影响
    assert client.get("/api/records/R000002").get_json()["status"] == "valid"

    # 重复裁决返回首次结果
    again = post(client, "/api/records/R000001/invalidate",
                 {"operation_id": "http-op-1"})
    assert again.status_code == 200
    assert again.get_json()["replayed"] is True
    assert {c["id"] for c in again.get_json()["cascade"]} == cascaded

    # 操作结果可经操作标识查询
    opq = client.get("/api/operations/http-op-1")
    assert opq.status_code == 200
    assert opq.get_json()["result"] == "completed"


def test_same_op_id_different_target_conflicts(client):
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "a"}})
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "b"}})
    assert post(client, "/api/records/R000001/invalidate",
                {"operation_id": "op"}).status_code == 200
    conflict = post(client, "/api/records/R000002/invalidate",
                    {"operation_id": "op"})
    assert conflict.status_code == 409
    detail = conflict.get_json()["error"]
    assert detail["code"] == "OPERATION_CONFLICT"
    assert detail["details"]["original_target"] == "R000001"
    # R2 未被牵连
    assert client.get("/api/records/R000002").get_json()["status"] == "valid"


def test_error_responses_are_locatable(client):
    # 引用不存在记录
    r = post(client, "/api/records", {
        "kind": "derived", "payload": {}, "parent_ids": ["R000999"]})
    assert r.status_code == 422
    e = r.get_json()["error"]
    assert e["code"] == "PARENT_NOT_FOUND"
    assert e["details"]["missing_parent_ids"] == ["R000999"]

    # 自引用
    r = post(client, "/api/records", {
        "kind": "derived", "record_id": "SELF",
        "payload": {}, "parent_ids": ["SELF"]})
    assert r.get_json()["error"]["code"] == "SELF_REFERENCE"

    # 裁决不存在记录
    r = post(client, "/api/records/NOPE/invalidate",
             {"operation_id": "opx"})
    assert r.status_code == 404
    assert r.get_json()["error"]["details"]["record_id"] == "NOPE"

    # 缺少操作标识
    r = post(client, "/api/records/R000001/invalidate", {})
    assert r.status_code == 400
    assert r.get_json()["error"]["code"] == "OPERATION_ID_REQUIRED"


def test_cannot_base_new_derivation_on_invalidated(client):
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "a"}})
    post(client, "/api/records/R000001/invalidate",
         {"operation_id": "kill"})
    r = post(client, "/api/records", {
        "kind": "derived", "payload": {"value": "x"},
        "parent_ids": ["R000001"]})
    assert r.status_code == 422
    assert r.get_json()["error"]["code"] == "PARENT_INVALID"


def test_rebuild_full_lifecycle_over_http(client):
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "bad"}})
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "ok"}})
    post(client, "/api/records", {
        "kind": "derived", "payload": {"value": "d1"},
        "parent_ids": ["R000001"]})
    post(client, "/api/records", {
        "kind": "derived", "payload": {"value": "d2"},
        "parent_ids": ["R000001", "R000002"]})

    rb = post(client, "/api/records/R000001/rebuild", {
        "operation_id": "fix-http-1", "payload": {"value": "corrected"}})
    assert rb.status_code == 200
    body = rb.get_json()
    assert body["result"] == "completed" and body["replayed"] is False
    pairs = {m["old_id"]: m["new_id"] for m in body["mapping"]}
    assert set(pairs) == {"R000001", "R000003", "R000004"}
    levels = {m["old_id"]: m["level"] for m in body["mapping"]}
    assert levels == {"R000001": 0, "R000003": 1, "R000004": 1}
    # 已重建依据换新编号，未替换依据保留原编号
    d2_new = pairs["R000004"]
    assert client.get(f"/api/records/{d2_new}").get_json()["parent_ids"] == \
        [pairs["R000001"], "R000002"]
    # 替代根携带替代读数
    assert client.get(f"/api/records/{pairs['R000001']}").get_json()[
        "payload"] == {"value": "corrected"}
    # 旧支失效、稳定来源
    for rid in ("R000001", "R000003", "R000004"):
        rec = client.get(f"/api/records/{rid}").get_json()
        assert rec["status"] == "invalid"
        assert rec["invalidated_by"] == "R000001"

    # 同标识同参数重放
    again = post(client, "/api/records/R000001/rebuild", {
        "operation_id": "fix-http-1", "payload": {"value": "corrected"}})
    assert again.status_code == 200
    assert again.get_json()["replayed"] is True
    assert again.get_json()["mapping"] == body["mapping"]

    # 操作结果经操作标识可查
    opq = client.get("/api/operations/fix-http-1")
    assert opq.status_code == 200
    assert opq.get_json()["replacement_record_id"] == pairs["R000001"]

    # 重建清单可查
    rbs = client.get("/api/rebuilds")
    assert rbs.status_code == 200
    assert [r["operation_id"] for r in rbs.get_json()] == ["fix-http-1"]


def test_rebuild_changed_payload_conflicts_http(client):
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "a"}})
    first = post(client, "/api/records/R000001/rebuild",
                 {"operation_id": "fx", "payload": {"value": "v1"}})
    assert first.status_code == 200
    n1 = len(client.get("/api/records").get_json())

    conflict = post(client, "/api/records/R000001/rebuild",
                    {"operation_id": "fx", "payload": {"value": "v2"}})
    assert conflict.status_code == 409
    detail = conflict.get_json()["error"]
    assert detail["code"] == "OPERATION_CONFLICT"
    assert detail["details"]["original_target"] == "R000001"
    # 不留副本
    assert len(client.get("/api/records").get_json()) == n1

    # 改换目标同样冲突
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "b"}})
    conflict2 = post(client, "/api/records/R000002/rebuild",
                     {"operation_id": "fx", "payload": {"value": "v1"}})
    assert conflict2.status_code == 409
    assert client.get("/api/records/R000002").get_json()["status"] == "valid"


def test_rebuild_rejects_derived_and_missing_target_http(client):
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "a"}})
    post(client, "/api/records", {
        "kind": "derived", "payload": {"value": "d"},
        "parent_ids": ["R000001"]})
    r = post(client, "/api/records/R000002/rebuild",
             {"operation_id": "fx", "payload": {"value": "x"}})
    assert r.status_code == 422
    assert r.get_json()["error"]["code"] == "REBUILD_TARGET_MUST_BE_RAW"

    r = post(client, "/api/records/GHOST/rebuild",
             {"operation_id": "fx", "payload": {"value": "x"}})
    assert r.status_code == 404
    assert r.get_json()["error"]["details"]["record_id"] == "GHOST"

    r = post(client, "/api/records/R000001/rebuild",
             {"operation_id": "fx"})
    assert r.status_code == 400
    assert r.get_json()["error"]["code"] == "REPLACEMENT_PAYLOAD_REQUIRED"

    r = post(client, "/api/records/R000001/rebuild",
             {"payload": {"value": "x"}})
    assert r.status_code == 400
    assert r.get_json()["error"]["code"] == "OPERATION_ID_REQUIRED"


def test_rebuild_competing_with_invalidation_http(client):
    """重建与失效裁决竞争：完整重建或拒绝，不留下有效记录引用失效记录。"""
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "p"}})
    post(client, "/api/records", {
        "kind": "derived", "payload": {"value": "d"},
        "parent_ids": ["R000001"]})
    inv = post(client, "/api/records/R000001/invalidate",
               {"operation_id": "kill-first"})
    assert inv.status_code == 200
    rb = post(client, "/api/records/R000001/rebuild",
              {"operation_id": "fix-late", "payload": {"value": "v"}})
    assert rb.status_code == 409
    assert rb.get_json()["error"]["code"] == "RECORD_ALREADY_INVALID"
    # 无副本产生：仍只有 2 条记录
    records = client.get("/api/records").get_json()
    assert len(records) == 2
