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
    # R1(失真原始) <- R2 <- R3(R2 + R4)；R4 无关
    post(client, "/api/records",
         {"kind": "raw", "payload": {"value": "77K distorted"}})
    post(client, "/api/records",
         {"kind": "raw", "payload": {"value": "4K stable"}})
    post(client, "/api/records", {
        "kind": "derived", "payload": {"value": "gain"},
        "parent_ids": ["R000001"]})
    post(client, "/api/records", {
        "kind": "derived", "payload": {"value": "gain+4K"},
        "parent_ids": ["R000003", "R000002"]})

    rb = post(client, "/api/records/R000001/rebuild", {
        "operation_id": "http-rb-1",
        "replacement_payload": {"value": "77K corrected"}})
    assert rb.status_code == 200
    body = rb.get_json()
    assert body["kind"] == "rebuild"
    assert body["result"] == "completed"
    mapping = {m["old_id"]: m["new_id"] for m in body["mapping"]}
    assert set(mapping) == {"R000001", "R000003", "R000004"}
    n1, n3, n4 = mapping["R000001"], mapping["R000003"], mapping["R000004"]
    assert body["replacement_record_id"] == n1

    # 返回中带新记录的有效性与直接依据
    records_by_id = {r["id"]: r for r in body["records"]}
    assert records_by_id[n1]["status"] == "valid"
    assert records_by_id[n1]["parent_ids"] == []
    assert records_by_id[n3]["parent_ids"] == [n1]
    # 其余依据继续指向原记录
    assert records_by_id[n4]["parent_ids"] == [n3, "R000002"]

    # 经查询接口复核持久化状态
    assert client.get(f"/api/records/{n1}").get_json()["payload"] == \
        {"value": "77K corrected"}
    assert client.get(f"/api/records/{n3}").get_json()["rebuilt_from"] \
        == "R000003"
    for old in ("R000001", "R000003", "R000004"):
        rec = client.get(f"/api/records/{old}").get_json()
        assert rec["status"] == "invalid"
        assert rec["invalidated_by"] == "R000001"
    # 无关原始记录保持有效
    assert client.get("/api/records/R000002").get_json()["status"] == "valid"

    # 同修复标识 + 同参数重试：重放首次映射
    again = post(client, "/api/records/R000001/rebuild", {
        "operation_id": "http-rb-1",
        "replacement_payload": {"value": "77K corrected"}})
    assert again.status_code == 200
    assert again.get_json()["replayed"] is True
    assert again.get_json()["mapping"] == body["mapping"]

    # 操作结果可经操作标识查询
    opq = client.get("/api/operations/http-rb-1")
    assert opq.status_code == 200
    assert opq.get_json()["kind"] == "rebuild"


def test_rebuild_changed_payload_conflicts_over_http(client):
    post(client, "/api/records",
         {"kind": "raw", "payload": {"value": "a"}})
    first = post(client, "/api/records/R000001/rebuild",
                 {"operation_id": "rb",
                  "replacement_payload": {"value": "fix-A"}})
    assert first.status_code == 200
    n = len(client.get("/api/records").get_json())

    conflict = post(client, "/api/records/R000001/rebuild",
                    {"operation_id": "rb",
                     "replacement_payload": {"value": "fix-B"}})
    assert conflict.status_code == 409
    assert conflict.get_json()["error"]["code"] == "OPERATION_CONFLICT"
    # 不留副本
    assert len(client.get("/api/records").get_json()) == n


def test_rebuild_validation_errors_over_http(client):
    post(client, "/api/records",
         {"kind": "raw", "payload": {"value": "a"}})

    # 缺少修复操作标识
    r = post(client, "/api/records/R000001/rebuild",
             {"replacement_payload": {"value": "fix"}})
    assert r.status_code == 400
    assert r.get_json()["error"]["code"] == "OPERATION_ID_REQUIRED"

    # 缺少替代读数
    r = post(client, "/api/records/R000001/rebuild",
             {"operation_id": "rb-x"})
    assert r.status_code == 400
    assert r.get_json()["error"]["code"] == "INVALID_REPLACEMENT_PAYLOAD"

    # 对推导记录重建
    post(client, "/api/records", {
        "kind": "derived", "payload": {"value": "d"},
        "parent_ids": ["R000001"]})
    r = post(client, "/api/records/R000002/rebuild",
             {"operation_id": "rb-y",
              "replacement_payload": {"value": "fix"}})
    assert r.status_code == 422
    assert r.get_json()["error"]["code"] == "REBUILD_TARGET_NOT_RAW"

    # 对不存在记录重建
    r = post(client, "/api/records/R000999/rebuild",
             {"operation_id": "rb-z",
              "replacement_payload": {"value": "fix"}})
    assert r.status_code == 404
