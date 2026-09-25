"""CalibrationStore 单元测试：级联失效 / 幂等裁决 / 引用校验 / 并发 / 替代重建 / 重启。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.store import CalibrationStore, StoreError


@pytest.fixture()
def store(tmp_path):
    db = str(tmp_path / "test.db")
    s = CalibrationStore(db)
    yield s
    s.close()


def make_chain(store: CalibrationStore):
    """R1(raw) <- R2 <- R3, 另建 R4(raw) <- R5(derived)。"""
    r1 = store.create_record("raw", {"value": "raw-1"}, None)
    r2 = store.create_record("derived", {"value": "derived-2"}, [r1["id"]])
    r3 = store.create_record("derived", {"value": "derived-3"},
                             [r2["id"], r1["id"]])
    r4 = store.create_record("raw", {"value": "raw-4"}, None)
    r5 = store.create_record("derived", {"value": "derived-5"}, [r4["id"]])
    return [r["id"] for r in (r1, r2, r3, r4, r5)]


def test_stable_ids_and_direct_basis(store):
    ids = make_chain(store)
    assert ids == ["R000001", "R000002", "R000003", "R000004", "R000005"]
    r3 = store.get_record("R000003")
    assert r3["status"] == "valid"
    assert r3["parent_ids"] == ["R000002", "R000001"]
    assert r3["invalidated_by"] is None


def test_cascade_invalidation_single_commit(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    result = store.invalidate("op-cascade-1", r1)
    cascaded = {c["id"] for c in result["cascade"]}
    assert cascaded == {r1, r2, r3}  # R4/R5 不受影响
    for rid in (r1, r2, r3):
        rec = store.get_record(rid)
        assert rec["status"] == "invalid"
        # 稳定的失效来源：裁决目标编号
        assert rec["invalidated_by"] == r1
    assert store.get_record(r5)["status"] == "valid"
    store.assert_invariants()


def test_invalidation_of_midpoint_still_cascades_downstream(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    result = store.invalidate("op-mid", r2)
    assert {c["id"] for c in result["cascade"]} == {r2, r3}
    assert store.get_record(r1)["status"] == "valid"
    assert store.get_record(r2)["invalidated_by"] == r2
    assert store.get_record(r3)["invalidated_by"] == r2


def test_idempotent_same_operation_returns_first_result(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    first = store.invalidate("op-same", r1)
    second = store.invalidate("op-same", r1)
    third = store.invalidate("op-same", r1)
    assert first["cascade"] == second["cascade"] == third["cascade"]
    assert first["replayed"] is False
    assert second["replayed"] is True and third["replayed"] is True
    # 操作重放结果可查询
    stored = store.get_operation("op-same")
    assert stored["cascade"] == first["cascade"]
    assert stored["result"] == "completed"


def test_same_operation_different_target_conflicts_and_keeps_state(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    store.invalidate("op-conflict", r1)
    with pytest.raises(StoreError) as ei:
        store.invalidate("op-conflict", r4)
    assert ei.value.status == 409
    assert ei.value.code == "OPERATION_CONFLICT"
    assert ei.value.details["original_target"] == r1
    assert ei.value.details["conflicting_target"] == r4
    # 状态未被改变：R4/R5 仍有效
    assert store.get_record(r4)["status"] == "valid"
    assert store.get_record(r5)["status"] == "valid"


def test_reference_missing_record_rejected(store):
    with pytest.raises(StoreError) as ei:
        store.create_record("derived", {"value": "x"}, ["R000999"])
    assert ei.value.code == "PARENT_NOT_FOUND"
    assert ei.value.details["missing_parent_ids"] == ["R000999"]


def test_self_reference_rejected(store):
    with pytest.raises(StoreError) as ei:
        store.create_record("derived", {"value": "x"}, ["RX1"],
                            record_id="RX1")
    assert ei.value.code == "SELF_REFERENCE"
    assert "RX1" in ei.value.details["parent_ids"]
    with pytest.raises(StoreError):
        store.get_record("RX1")  # 未落库


def test_reference_invalid_record_rejected_and_existing_preserved(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    store.invalidate("op-x", r4)  # R4/R5 失效
    with pytest.raises(StoreError) as ei:
        store.create_record("derived", {"value": "x"}, [r1, r4])
    assert ei.value.code == "PARENT_INVALID"
    assert ei.value.details["invalid_parent_ids"] == [r4]
    # 既有可用结论原样保留
    assert store.get_record(r1)["status"] == "valid"
    assert store.get_record(r5)["status"] == "invalid"
    assert store.get_record(r5)["invalidated_by"] == r4


def test_invalidate_missing_target_is_locatable_error(store):
    with pytest.raises(StoreError) as ei:
        store.invalidate("op-missing", "R000999")
    assert ei.value.status == 404
    assert ei.value.details["record_id"] == "R000999"
    # 未占用操作标识：修正目标后可正常使用
    make_chain(store)
    result = store.invalidate("op-missing", "R000001")
    assert result["replayed"] is False


def test_raw_record_cannot_have_parents(store):
    with pytest.raises(StoreError) as ei:
        store.create_record("raw", {"value": "x"}, ["R000001"])
    assert ei.value.code == "RAW_RECORD_HAS_PARENTS"


def test_derived_requires_parents(store):
    with pytest.raises(StoreError) as ei:
        store.create_record("derived", {"value": "x"}, [])
    assert ei.value.code == "DERIVED_RECORD_WITHOUT_BASIS"


def test_cycle_guard_detects_ancestor_reaching_new_node(store):
    """成环守卫：API 下新节点只会指向已有节点，成环本就被结构性排除，
    此用例用一条遗留边验证守卫的判定方向正确（防御性实现）。"""
    a = store.create_record("raw", {"value": "a"}, None, record_id="A")
    c = store.create_record("raw", {"value": "c"}, None, record_id="C")
    with store._lock:
        store._conn.execute("BEGIN IMMEDIATE")
        store._conn.execute(
            "INSERT INTO edges(child_id, parent_id, seq) VALUES ('A','C',0)")
        store._conn.execute("COMMIT")
    # A 沿 parent 方向可达 C：若新节点是 C 且候选前序含 A，则成环
    assert store._reaches_ancestor_locked(c["id"], {a["id"]}) is True
    # 不可达时不误报
    d = store.create_record("raw", {"value": "d"}, None, record_id="D")
    assert store._reaches_ancestor_locked(d["id"], {a["id"]}) is False


def test_concurrent_create_vs_invalidate_never_orphans_validity(store):
    """新推导与失效裁决竞争后：不存在有效记录依赖失效记录。"""
    pivot = store.create_record("raw", {"value": "pivot"}, None)["id"]
    errors: list[Exception] = []

    def worker(i: int):
        try:
            if i % 3 == 0:
                store.invalidate(f"op-inv-{i}", pivot)
            else:
                store.create_record(
                    "derived", {"value": f"d-{i}"}, [pivot])
        except StoreError:
            pass  # 竞争失败是合法结局，关键是不变量
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(worker, range(60)))
    assert not errors
    store.assert_invariants()
    # 裁决只允许一次成功；之后所有对 pivot 的引用都被拒
    pivot_rec = store.get_record(pivot)
    assert pivot_rec["status"] == "invalid"
    children = [r for r in store.list_records()
                if pivot in r["parent_ids"]]
    assert all(c["status"] == "invalid" for c in children)


def test_concurrent_duplicate_invalidation_single_winner(store):
    p = store.create_record("raw", {"value": "p"}, None)["id"]

    def invoke():
        try:
            return store.invalidate("op-race-single", p)
        except StoreError as e:
            return e

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: invoke(), range(32)))
    completed = [r for r in results if not isinstance(r, StoreError)]
    assert completed, "至少一次裁决应成功"
    # 所有成功返回必须是同一份首次结果
    first = completed[0]["cascade"]
    assert all(r["cascade"] == first for r in completed)
    store.assert_invariants()


def test_persistence_after_reopen(tmp_path):
    db = str(tmp_path / "persist.db")
    s1 = CalibrationStore(db)
    a = s1.create_record("raw", {"value": "a"}, None)
    b = s1.create_record("derived", {"value": "b"}, [a["id"]])
    s1.invalidate("op-restart", a["id"])
    s1.close()

    s2 = CalibrationStore(db)  # 重启
    assert s2.get_record(a["id"])["status"] == "invalid"
    assert s2.get_record(b["id"])["status"] == "invalid"
    assert s2.get_record(b["id"])["invalidated_by"] == a["id"]
    # 操作重放：返回首次结果
    replayed = s2.invalidate("op-restart", a["id"])
    assert replayed["replayed"] is True
    assert {c["id"] for c in replayed["cascade"]} == {a["id"], b["id"]}
    assert s2.get_operation("op-restart")["result"] == "completed"
    s2.close()


# --------------------------------------------------------------------- #
# 替代重建
# --------------------------------------------------------------------- #
def make_diamond(store: CalibrationStore):
    """r1(失真原始) -> r2 -> r3；r3 另据 r4(原始)；r4 -> r5（无关支）。"""
    r1 = store.create_record("raw", {"value": "raw-1-distorted"}, None)["id"]
    r4 = store.create_record("raw", {"value": "raw-4"}, None)["id"]
    r2 = store.create_record("derived", {"value": "derived-2"}, [r1])["id"]
    r3 = store.create_record("derived", {"value": "derived-3"},
                             [r2, r4])["id"]
    r5 = store.create_record("derived", {"value": "derived-5"}, [r4])["id"]
    return r1, r2, r3, r4, r5


def _map_by_old(result: dict) -> dict[str, str]:
    return {m["old_id"]: m["new_id"] for m in result["mapping"]}


def test_rebuild_copies_by_dependency_layers_and_repoints_basis(store):
    r1, r2, r3, r4, r5 = make_diamond(store)
    result = store.rebuild("rb-1", r1, {"value": "raw-1-fixed"})

    assert result["replayed"] is False
    assert result["target_record_id"] == r1
    m = _map_by_old(result)
    assert set(m) == {r1, r2, r3}  # 只复制受影响有效闭包
    n1, n2, n3 = m[r1], m[r2], m[r3]

    # 新根携带替代读数；下游副本保留原业务内容
    assert store.get_record(n1)["payload"] == {"value": "raw-1-fixed"}
    assert store.get_record(n1)["kind"] == "raw"
    assert store.get_record(n2)["payload"] == {"value": "derived-2"}
    assert store.get_record(n3)["payload"] == {"value": "derived-3"}

    # 已重建依据指向新编号；其余依据继续指向原记录（r4 未受影响）
    assert store.get_record(n2)["parent_ids"] == [n1]
    assert store.get_record(n3)["parent_ids"] == [n2, r4]

    # 溯源字段
    assert store.get_record(n3)["rebuilt_from"] == r3
    assert store.get_record(n3)["rebuilt_by"] == "rb-1"

    # 旧根与旧下游在同一提交后全部失效，来源稳定为旧根
    for rid in (r1, r2, r3):
        rec = store.get_record(rid)
        assert rec["status"] == "invalid"
        assert rec["invalidated_by"] == r1
    # 无关节支保持原样
    assert store.get_record(r4)["status"] == "valid"
    assert store.get_record(r5)["status"] == "valid"
    store.assert_invariants()


def test_rebuild_idempotent_replays_first_mapping(store):
    r1, r2, r3, r4, r5 = make_diamond(store)
    first = store.rebuild("rb-same", r1, {"value": "fixed"})
    second = store.rebuild("rb-same", r1, {"value": "fixed"})
    assert second["replayed"] is True
    assert second["mapping"] == first["mapping"]
    assert second["replacement_record_id"] == first["replacement_record_id"]
    # 重放不产生新副本
    assert len(store.list_records()) == 8
    stored = store.get_operation("rb-same")
    assert stored["kind"] == "rebuild"
    assert stored["mapping"] == first["mapping"]


def test_rebuild_same_op_changed_payload_conflicts_without_copies(store):
    r1, r2, r3, r4, r5 = make_diamond(store)
    store.rebuild("rb-conf", r1, {"value": "fixed-A"})
    n = len(store.list_records())
    with pytest.raises(StoreError) as ei:
        store.rebuild("rb-conf", r1, {"value": "fixed-B"})
    assert ei.value.status == 409
    assert ei.value.code == "OPERATION_CONFLICT"
    assert ei.value.details["original_kind"] == "rebuild"
    assert len(store.list_records()) == n  # 不留副本、不改状态


def test_rebuild_same_op_changed_target_conflicts(store):
    r1, r2, r3, r4, r5 = make_diamond(store)
    store.rebuild("rb-t", r1, {"value": "fixed"})
    with pytest.raises(StoreError) as ei:
        store.rebuild("rb-t", r4, {"value": "fixed"})
    assert ei.value.code == "OPERATION_CONFLICT"
    assert ei.value.details["conflicting_target"] == r4
    assert store.get_record(r4)["status"] == "valid"


def test_rebuild_op_id_shared_with_invalidation(store):
    r1, r2, r3, r4, r5 = make_diamond(store)
    store.invalidate("shared-op", r4)
    with pytest.raises(StoreError) as ei:
        store.rebuild("shared-op", r1, {"value": "fixed"})
    assert ei.value.code == "OPERATION_CONFLICT"
    assert ei.value.details["original_kind"] == "invalidate"
    # 未留下副本
    assert all(r["rebuilt_from"] is None
               for r in store.list_records())


def test_rebuild_target_must_be_valid_raw(store):
    r1, r2, r3, r4, r5 = make_diamond(store)
    with pytest.raises(StoreError) as ei:
        store.rebuild("rb-derived", r2, {"value": "fixed"})
    assert ei.value.code == "REBUILD_TARGET_NOT_RAW"
    # 未占用操作标识
    assert store.get_operation("rb-derived") is None

    store.invalidate("kill-r1", r1)
    before = len(store.list_records())
    with pytest.raises(StoreError) as ei:
        store.rebuild("rb-after-inv", r1, {"value": "fixed"})
    assert ei.value.status == 409
    assert ei.value.code == "RECORD_ALREADY_INVALID"
    assert len(store.list_records()) == before


def test_rebuild_missing_target_is_locatable(store):
    with pytest.raises(StoreError) as ei:
        store.rebuild("rb-missing", "R000999", {"value": "fixed"})
    assert ei.value.status == 404
    assert ei.value.details["record_id"] == "R000999"


def test_rebuild_requires_payload_object(store):
    r1 = store.create_record("raw", {"value": "x"}, None)["id"]
    with pytest.raises(StoreError) as ei:
        store.rebuild("rb-payload", r1, "not-an-object")  # type: ignore[arg-type]
    assert ei.value.code == "INVALID_PAYLOAD"


def test_concurrent_rebuild_vs_invalidate_all_or_nothing(store):
    """重建与失效裁决竞争：只能得到完整重建或无状态变化的拒绝，
    绝不存在有效记录引用已失效记录。"""
    pivot = store.create_record("raw", {"value": "pivot"}, None)["id"]
    d1 = store.create_record("derived", {"value": "d1"}, [pivot])["id"]
    d2 = store.create_record("derived", {"value": "d2"}, [d1])["id"]
    errors: list[Exception] = []

    def worker(i: int):
        try:
            if i % 2 == 0:
                store.invalidate(f"race-{i}", pivot)
            else:
                store.rebuild(f"race-{i}", pivot, {"value": f"fix-{i}"})
        except StoreError:
            pass  # 竞争失败是合法结局
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(worker, range(40)))
    assert not errors
    store.assert_invariants()

    records = {r["id"]: r for r in store.list_records()}
    rebuilt = [r for r in records.values() if r["rebuilt_from"] is not None]
    old_invalid = all(records[i]["status"] == "invalid"
                      for i in (pivot, d1, d2))
    if rebuilt:
        # 发生过完整重建：新支三层齐全且全部有效，旧支全部失效
        assert len(rebuilt) == 3
        assert all(r["status"] == "valid" for r in rebuilt)
        assert old_invalid
        roots = [r for r in rebuilt if r["kind"] == "raw"]
        assert len(roots) == 1
    else:
        # 未发生重建：裁决先行，原支全部失效
        assert old_invalid


def test_concurrent_duplicate_rebuild_single_mapping(store):
    pivot = store.create_record("raw", {"value": "p"}, None)["id"]
    d = store.create_record("derived", {"value": "d"}, [pivot])["id"]

    def invoke():
        try:
            return store.rebuild("rb-race-single", pivot,
                                 {"value": "fixed"})
        except StoreError as e:
            return e

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: invoke(), range(32)))
    completed = [r for r in results if not isinstance(r, StoreError)]
    assert completed
    first = completed[0]["mapping"]
    assert all(r["mapping"] == first for r in completed)
    store.assert_invariants()
    # 只有一条新根 + 一条新推导
    records = store.list_records()
    assert len([r for r in records if r["rebuilt_from"] == pivot]) == 1
    assert len([r for r in records if r["rebuilt_from"] == d]) == 1


def test_rebuild_persistence_after_reopen(tmp_path):
    db = str(tmp_path / "rebuild.db")
    s1 = CalibrationStore(db)
    a = s1.create_record("raw", {"value": "a"}, None)["id"]
    b = s1.create_record("derived", {"value": "b"}, [a])["id"]
    first = s1.rebuild("rb-restart", a, {"value": "a-fixed"})
    s1.close()

    s2 = CalibrationStore(db)
    m = _map_by_old(first)
    assert s2.get_record(m[a])["payload"] == {"value": "a-fixed"}
    assert s2.get_record(m[b])["parent_ids"] == [m[a]]
    assert s2.get_record(a)["status"] == "invalid"
    replayed = s2.rebuild("rb-restart", a, {"value": "a-fixed"})
    assert replayed["replayed"] is True
    assert replayed["mapping"] == first["mapping"]
    s2.close()
