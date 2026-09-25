"""CalibrationStore 单元测试：级联失效 / 幂等裁决 / 引用校验 / 并发 / 重启。"""

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


def make_rebuild_graph(store: CalibrationStore):
    """r1(失真原始) <- d1；r1,r2 <- d2；d1,d2 <- d3；r2 <- d4（无关支）。"""
    r1 = store.create_record("raw", {"value": "12.4 distorted"}, None)
    r2 = store.create_record("raw", {"value": "4.2 stable"}, None)
    d1 = store.create_record("derived", {"value": "conclusion-d1"}, [r1["id"]])
    d2 = store.create_record("derived", {"value": "conclusion-d2"},
                             [r1["id"], r2["id"]])
    d3 = store.create_record("derived", {"value": "conclusion-d3"},
                             [d1["id"], d2["id"]])
    d4 = store.create_record("derived", {"value": "conclusion-d4"}, [r2["id"]])
    return [r["id"] for r in (r1, r2, d1, d2, d3, d4)]


def test_rebuild_copies_by_level_and_rewires_basis(store):
    r1, r2, d1, d2, d3, d4 = make_rebuild_graph(store)
    res = store.rebuild("fix-1", r1, {"value": "12.1 corrected"})

    assert res["result"] == "completed" and res["replayed"] is False
    pairs = {m["old_id"]: m["new_id"] for m in res["mapping"]}
    # 目标 + 全部可达的有效推导均被复制；d4 不在闭包内
    assert set(pairs) == {r1, d1, d2, d3}
    # 按依赖层级
    levels = {m["old_id"]: m["level"] for m in res["mapping"]}
    assert levels == {r1: 0, d1: 1, d2: 1, d3: 2}

    new_r1, new_d1, new_d2, new_d3 = pairs[r1], pairs[d1], pairs[d2], pairs[d3]
    # 替代根携带替代读数
    assert store.get_record(new_r1)["payload"] == {"value": "12.1 corrected"}
    # 副本保留原有业务内容
    assert store.get_record(new_d2)["payload"] == {"value": "conclusion-d2"}
    # 已重建依据指向新编号，其余依据继续指向原记录
    assert store.get_record(new_d1)["parent_ids"] == [new_r1]
    assert store.get_record(new_d2)["parent_ids"] == [new_r1, r2]
    assert store.get_record(new_d3)["parent_ids"] == [new_d1, new_d2]

    # 旧根与旧下游失效，稳定失效来源为旧根编号；无关支不受影响
    for rid in (r1, d1, d2, d3):
        rec = store.get_record(rid)
        assert rec["status"] == "invalid"
        assert rec["invalidated_by"] == r1
    assert store.get_record(r2)["status"] == "valid"
    assert store.get_record(d4)["status"] == "valid"
    assert res["replacement_record_id"] == new_r1
    store.assert_invariants()


def test_rebuild_response_mapping_carries_status_and_basis(store):
    r1, r2, d1, d2, d3, d4 = make_rebuild_graph(store)
    res = store.rebuild("fix-map", r1, {"value": "corrected"})
    by_old = {m["old_id"]: m for m in res["mapping"]}
    assert all(m["status"] == "valid" for m in res["mapping"])
    assert by_old[d2]["parent_ids"] == [by_old[r1]["new_id"], r2]
    assert by_old[r1]["parent_ids"] == []
    assert {x["id"] for x in res["rebuilt"]} == {
        m["new_id"] for m in res["mapping"]}
    assert {x["id"] for x in res["invalidated"]} == {r1, d1, d2, d3}


def test_rebuild_idempotent_replays_first_mapping(store):
    r1, r2, d1, d2, d3, d4 = make_rebuild_graph(store)
    first = store.rebuild("fix-same", r1, {"value": "corrected"})
    second = store.rebuild("fix-same", r1, {"value": "corrected"})
    third = store.rebuild("fix-same", r1, {"value": "corrected"})
    assert first["replayed"] is False
    assert second["replayed"] is third["replayed"] is True
    assert second["mapping"] == first["mapping"]
    assert second["replacement_record_id"] == first["replacement_record_id"]
    # 操作结果可经操作标识查询
    assert store.get_operation("fix-same")["mapping"] == first["mapping"]
    # 重放不产生额外副本
    assert len(store.list_records()) == 6 + 4


def test_rebuild_changed_payload_conflicts_without_copies(store):
    r1, r2, d1, d2, d3, d4 = make_rebuild_graph(store)
    store.rebuild("fix-c", r1, {"value": "corrected"})
    n_after_first = len(store.list_records())
    with pytest.raises(StoreError) as ei:
        store.rebuild("fix-c", r1, {"value": "different value"})
    assert ei.value.status == 409
    assert ei.value.code == "OPERATION_CONFLICT"
    assert ei.value.details["original_target"] == r1
    # 不留任何副本、状态不变（新支仍有效）
    assert len(store.list_records()) == n_after_first


def test_rebuild_changed_target_conflicts_without_copies(store):
    r1, r2, d1, d2, d3, d4 = make_rebuild_graph(store)
    store.rebuild("fix-t", r1, {"value": "corrected"})
    with pytest.raises(StoreError) as ei:
        store.rebuild("fix-t", r2, {"value": "corrected"})
    assert ei.value.code == "OPERATION_CONFLICT"
    assert store.get_record(r2)["status"] == "valid"
    assert store.get_record(d4)["status"] == "valid"


def test_rebuild_operation_id_shared_with_invalidation(store):
    r1, r2, d1, d2, d3, d4 = make_rebuild_graph(store)
    store.rebuild("shared-op", r1, {"value": "corrected"})
    with pytest.raises(StoreError) as ei:
        store.invalidate("shared-op", r2)
    assert ei.value.code == "OPERATION_CONFLICT"
    assert store.get_record(r2)["status"] == "valid"


def test_rebuild_requires_existing_valid_raw_target(store):
    r1, r2, d1, d2, d3, d4 = make_rebuild_graph(store)
    # 目标不存在
    with pytest.raises(StoreError) as ei:
        store.rebuild("fix-miss", "R000999", {"value": "x"})
    assert ei.value.status == 404
    assert ei.value.details["record_id"] == "R000999"
    # 推导记录不能作为重建目标
    with pytest.raises(StoreError) as ei:
        store.rebuild("fix-derived", d1, {"value": "x"})
    assert ei.value.code == "REBUILD_TARGET_MUST_BE_RAW"
    assert store.get_record(d1)["status"] == "valid"
    # 已失效目标：拒绝且不占用操作标识以外的任何状态
    store.invalidate("op-kill-r1", r1)
    with pytest.raises(StoreError) as ei:
        store.rebuild("fix-late", r1, {"value": "x"})
    assert ei.value.status == 409
    assert ei.value.code == "RECORD_ALREADY_INVALID"


def test_rebuild_skips_already_invalid_downstream_keeps_stable_source(store):
    r1, r2, d1, d2, d3, d4 = make_rebuild_graph(store)
    # 先行失效 d1（稳定来源 d1），d3 随之失效
    store.invalidate("op-pre", d1)
    res = store.rebuild("fix-partial", r1, {"value": "corrected"})
    pairs = {m["old_id"]: m["new_id"] for m in res["mapping"]}
    # 仅仍有效的 r1、d2 被复制；d1/d3 不产生副本
    assert set(pairs) == {r1, d2}
    assert store.get_record(d1)["invalidated_by"] == d1  # 首次来源不被覆盖
    assert store.get_record(d3)["invalidated_by"] == d1
    new_d2 = pairs[d2]
    assert store.get_record(new_d2)["parent_ids"] == [pairs[r1], r2]
    store.assert_invariants()


def test_rebuild_then_invalidation_cascades_through_unrewired_edge(store):
    """重建后新支经“未替换边”仍依赖原记录：对其失效须级联到新副本。"""
    r1, r2, d1, d2, d3, d4 = make_rebuild_graph(store)
    res = store.rebuild("fix-edge", r1, {"value": "corrected"})
    pairs = {m["old_id"]: m["new_id"] for m in res["mapping"]}
    new_d2 = pairs[d2]
    inv = store.invalidate("op-r2", r2)
    cascaded = {c["id"] for c in inv["cascade"]}
    assert r2 in cascaded and new_d2 in cascaded and pairs[d3] in cascaded
    assert pairs[r1] not in cascaded and pairs[d1] not in cascaded
    store.assert_invariants()


def test_concurrent_rebuild_vs_invalidate_is_all_or_nothing(store):
    """替代重建与失效裁决竞争：要么完整重建，要么无状态变化的拒绝，
    绝不存在有效记录引用已失效记录。"""
    pivot = store.create_record("raw", {"value": "pivot"}, None)["id"]
    pre1 = store.create_record("derived", {"value": "pre1"}, [pivot])["id"]
    pre2 = store.create_record("derived", {"value": "pre2"}, [pre1])["id"]
    outcomes: list[str] = []

    def worker(i: int):
        try:
            if i % 2 == 0:
                store.rebuild(f"op-race-{i}", pivot, {"value": f"fix-{i}"})
                outcomes.append("rebuilt")
            else:
                store.invalidate(f"op-race-{i}", pivot)
                outcomes.append("invalidated")
        except StoreError as e:
            if e.code in ("RECORD_ALREADY_INVALID", "OPERATION_CONFLICT"):
                outcomes.append("rejected")
            else:
                raise

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(worker, range(48)))
    store.assert_invariants()
    # 目标相同、操作标识各异：首个写事务获胜后旧根即失效，
    # 其余重建/裁决全部被拒。故总成功数恰为 1，两类互斥，无半支重建。
    rebuilds = store.list_rebuilds()
    winning = [o for o in outcomes if o == "rebuilt"]
    assert len(winning) == len(rebuilds)
    assert len(winning) <= 1
    assert len(winning) + len([o for o in outcomes
                               if o == "invalidated"]) == 1
    for rb in rebuilds:
        pairs = {m["old_id"]: m["new_id"] for m in rb["mapping"]}
        # 完整一支：旧闭包（3 条）全部被复制，新支全部有效且依据有效
        assert set(pairs) == {pivot, pre1, pre2}
        for new_id in pairs.values():
            rec = store.get_record(new_id)
            assert rec["status"] == "valid"
            assert all(store.get_record(p)["status"] == "valid"
                       for p in rec["parent_ids"])


def test_rebuild_persistence_after_reopen(tmp_path):
    db = str(tmp_path / "rebuild-persist.db")
    s1 = CalibrationStore(db)
    r1 = s1.create_record("raw", {"value": "bad"}, None)["id"]
    r2 = s1.create_record("raw", {"value": "ok"}, None)["id"]
    d1 = s1.create_record("derived", {"value": "d1"}, [r1, r2])["id"]
    first = s1.rebuild("op-rebuild-restart", r1, {"value": "good"})
    s1.close()

    s2 = CalibrationStore(db)  # 重启
    pairs = {m["old_id"]: m["new_id"] for m in first["mapping"]}
    # 新支仍有效、旧支仍失效
    assert s2.get_record(pairs[r1])["payload"] == {"value": "good"}
    assert s2.get_record(pairs[d1])["parent_ids"] == [pairs[r1], r2]
    assert s2.get_record(r1)["status"] == "invalid"
    assert s2.get_record(d1)["status"] == "invalid"
    # 同标识同参数重放首次映射
    replayed = s2.rebuild("op-rebuild-restart", r1, {"value": "good"})
    assert replayed["replayed"] is True
    assert replayed["mapping"] == first["mapping"]
    # 重启后改参数仍冲突
    with pytest.raises(StoreError) as ei:
        s2.rebuild("op-rebuild-restart", r1, {"value": "tampered"})
    assert ei.value.code == "OPERATION_CONFLICT"
    assert len(s2.list_records()) == 5  # 3 旧 + 2 副本，未产生新副本
    s2.assert_invariants()
    s2.close()


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
