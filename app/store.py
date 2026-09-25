"""标定谱系存储层。

核心不变量（均在单个 SQLite 写事务内保证）：

1. 一条失效裁决使目标记录及全部可达下游记录在同一持久化提交中失效；
2. 操作标识幂等：重复裁决返回首次结果；同一操作标识改换目标 -> 冲突且不改状态；
3. 新建推导记录时，任一前序不存在 / 已失效 / 自引用 / 成环 -> 整笔拒绝，既有结论不变；
4. 写事务串行化（BEGIN IMMEDIATE），因此“新推导”与“失效裁决”竞争后，
   不可能存在有效记录依赖失效记录；
5. 替代重建：在同一事务内复查目标仍为有效原始记录、受影响推导的未替换直接
   依据仍有效，随后按依赖层级复制目标及全部可达的有效推导（已重建依据指向新
   编号，其余依据继续指向原记录），再使旧根与旧下游失效。同操作标识同参数
   重试重放首次映射，改换任一参数冲突且不留任何副本。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('raw', 'derived')),
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('valid', 'invalid')),
    invalidated_by  TEXT,
    invalidated_at  TEXT,
    created_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS edges (
    child_id  TEXT NOT NULL REFERENCES records(id),
    parent_id TEXT NOT NULL REFERENCES records(id),
    seq       INTEGER NOT NULL,
    PRIMARY KEY (child_id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_edges_parent ON edges(parent_id);
CREATE TABLE IF NOT EXISTS operations (
    operation_id     TEXT PRIMARY KEY,
    kind             TEXT NOT NULL,
    target_record_id TEXT NOT NULL,
    params_json      TEXT,
    response_json    TEXT NOT NULL,
    created_at       TEXT NOT NULL
);
"""


class StoreError(Exception):
    """业务校验错误，携带可定位信息。"""

    def __init__(self, code: str, message: str, status: int = 422,
                 details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}

    def to_response(self) -> tuple[dict[str, Any], int]:
        return {"error": {"code": self.code, "message": self.message,
                          "details": self.details}}, self.status


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class CalibrationStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        # check_same_thread=False + 进程内互斥锁：所有写事务串行，
        # 读也走同一连接，保证读到已提交状态。
        self._conn = sqlite3.connect(db_path, check_same_thread=False,
                                     isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)
            # 旧库平滑升级：operations.params_json 记录首次操作参数指纹
            cols = {r["name"] for r in
                    self._conn.execute("PRAGMA table_info(operations)")}
            if "params_json" not in cols:
                self._conn.execute(
                    "ALTER TABLE operations ADD COLUMN params_json TEXT")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ #
    # 读取
    # ------------------------------------------------------------------ #
    def get_record(self, record_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM records WHERE id=?", (record_id,)
            ).fetchone()
            if row is None:
                raise StoreError("RECORD_NOT_FOUND",
                                 f"记录 {record_id} 不存在", status=404,
                                 details={"record_id": record_id})
            parents = [r["parent_id"] for r in self._conn.execute(
                "SELECT parent_id FROM edges WHERE child_id=? ORDER BY seq",
                (record_id,))]
            return self._row_to_dict(row, parents)

    def list_records(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM records ORDER BY created_at, id").fetchall()
            edge_rows = self._conn.execute(
                "SELECT child_id, parent_id FROM edges ORDER BY child_id, seq"
            ).fetchall()
        parents: dict[str, list[str]] = {}
        for e in edge_rows:
            parents.setdefault(e["child_id"], []).append(e["parent_id"])
        return [self._row_to_dict(r, parents.get(r["id"], [])) for r in rows]

    def get_operation(self, operation_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT response_json FROM operations WHERE operation_id=?",
                (operation_id,)).fetchone()
        if row is None:
            return None
        return json.loads(row["response_json"])

    def list_rebuilds(self) -> list[dict[str, Any]]:
        """全部替代重建操作的首次结果（供页面展示新旧两支谱系与映射）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT response_json FROM operations WHERE kind='rebuild' "
                "ORDER BY created_at, operation_id").fetchall()
        return [json.loads(r["response_json"]) for r in rows]

    @staticmethod
    def _row_to_dict(row: sqlite3.Row, parents: list[str]) -> dict[str, Any]:
        return {
            "id": row["id"],
            "kind": row["kind"],
            "payload": json.loads(row["payload"]),
            "status": row["status"],
            "parent_ids": parents,
            "invalidated_by": row["invalidated_by"],
            "invalidated_at": row["invalidated_at"],
            "created_at": row["created_at"],
        }

    # ------------------------------------------------------------------ #
    # 创建原始 / 推导记录
    # ------------------------------------------------------------------ #
    def create_record(self, kind: str, payload: dict[str, Any],
                      parent_ids: list[str] | None,
                      record_id: str | None = None) -> dict[str, Any]:
        if kind not in ("raw", "derived"):
            raise StoreError("INVALID_KIND", f"未知记录类型 {kind!r}",
                             status=400, details={"kind": kind})
        parent_ids = list(parent_ids or [])

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rid = record_id or self._allocate_id_locked()

                if self._conn.execute(
                        "SELECT 1 FROM records WHERE id=?", (rid,)).fetchone():
                    raise StoreError("RECORD_ID_CONFLICT",
                                     f"记录编号 {rid} 已存在", status=409,
                                     details={"record_id": rid})

                if kind == "raw" and parent_ids:
                    raise StoreError(
                        "RAW_RECORD_HAS_PARENTS",
                        "原始标定记录不能引用前序记录", status=400,
                        details={"parent_ids": parent_ids})
                if kind == "derived" and not parent_ids:
                    raise StoreError(
                        "DERIVED_RECORD_WITHOUT_BASIS",
                        "推导标定记录必须至少选择一个当前有效的前序记录",
                        details={"record_id": rid})

                # 自引用（id 尚未落库也必须拦截）
                if rid in parent_ids:
                    raise StoreError(
                        "SELF_REFERENCE",
                        f"推导记录 {rid} 不能引用自身作为前序依据",
                        details={"record_id": rid, "parent_ids": parent_ids})

                if len(set(parent_ids)) != len(parent_ids):
                    dup = sorted({p for p in parent_ids
                                  if parent_ids.count(p) > 1})
                    raise StoreError("DUPLICATE_PARENT",
                                     "前序记录重复出现",
                                     details={"duplicate_parent_ids": dup})

                # 存在性 + 有效性校验（全部在写事务内读到的是已提交快照）
                placeholders = ",".join("?" * len(parent_ids))
                found = {r["id"]: r for r in self._conn.execute(
                    f"SELECT id, status FROM records WHERE id IN ({placeholders})",
                    parent_ids)} if parent_ids else {}
                missing = [p for p in parent_ids if p not in found]
                if missing:
                    raise StoreError(
                        "PARENT_NOT_FOUND",
                        f"前序记录 {', '.join(missing)} 不存在",
                        details={"missing_parent_ids": missing})
                invalid_parents = [p for p in parent_ids
                                   if found[p]["status"] != "valid"]
                if invalid_parents:
                    raise StoreError(
                        "PARENT_INVALID",
                        f"前序记录 {', '.join(invalid_parents)} 已失效，"
                        "不能作为新推导的依据",
                        details={"invalid_parent_ids": invalid_parents})

                # 成环检查：新节点沿 parent 方向可达自身即成环。
                if self._reaches_ancestor_locked(rid, set(parent_ids)):
                    raise StoreError(
                        "CYCLE_DETECTED",
                        "引用关系形成环",
                        details={"record_id": rid, "parent_ids": parent_ids})

                now = _utcnow()
                self._conn.execute(
                    "INSERT INTO records (id, kind, payload, status, "
                    "invalidated_by, invalidated_at, created_at) "
                    "VALUES (?, ?, ?, 'valid', NULL, NULL, ?)",
                    (rid, kind, json.dumps(payload, ensure_ascii=False), now))
                for seq, pid in enumerate(parent_ids):
                    self._conn.execute(
                        "INSERT INTO edges (child_id, parent_id, seq) "
                        "VALUES (?, ?, ?)", (rid, pid, seq))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return self.get_record(rid)

    def _allocate_id_locked(self) -> str:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key='seq'").fetchone()
        seq = (int(row["value"]) + 1) if row else 1
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES('seq', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(seq),))
        return f"R{seq:06d}"

    def _reaches_ancestor_locked(self, target: str,
                                 starts: set[str]) -> bool:
        """从 starts 沿 child->parent 边向上能否到达 target。"""
        if not starts:
            return False
        frontier = set(starts)
        seen: set[str] = set()
        while frontier:
            if target in frontier:
                return True
            seen |= frontier
            qmarks = ",".join("?" * len(frontier))
            rows = self._conn.execute(
                f"SELECT parent_id FROM edges WHERE child_id IN ({qmarks})",
                tuple(frontier)).fetchall()
            frontier = {r["parent_id"] for r in rows} - seen
        return False

    # ------------------------------------------------------------------ #
    # 失效裁决（级联，单事务）
    # ------------------------------------------------------------------ #
    def invalidate(self, operation_id: str,
                   target_id: str) -> dict[str, Any]:
        if not operation_id:
            raise StoreError("OPERATION_ID_REQUIRED",
                             "失效裁决必须携带操作标识", status=400)

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # 1) 操作标识幂等 / 冲突判定（在同一写事务内）
                op = self._conn.execute(
                    "SELECT kind, target_record_id, response_json "
                    "FROM operations WHERE operation_id=?",
                    (operation_id,)).fetchone()
                if op is not None:
                    if (op["kind"] == "invalidate"
                            and op["target_record_id"] == target_id):
                        # 重复同一裁决：原样返回首次结果，不改状态
                        response = json.loads(op["response_json"])
                        response["replayed"] = True
                        self._conn.execute("COMMIT")
                        return response
                    raise StoreError(
                        "OPERATION_CONFLICT",
                        f"操作标识 {operation_id} 已用于 "
                        f"{op['kind']}({op['target_record_id']})，"
                        f"不能改用于 invalidate({target_id})",
                        status=409,
                        details={"operation_id": operation_id,
                                 "original_target": op["target_record_id"],
                                 "conflicting_target": target_id})

                # 2) 目标必须存在（不记录该操作标识，允许客户端修正后重试）
                target = self._conn.execute(
                    "SELECT id, status FROM records WHERE id=?", (target_id,)
                ).fetchone()
                if target is None:
                    raise StoreError(
                        "RECORD_NOT_FOUND",
                        f"裁决目标记录 {target_id} 不存在", status=404,
                        details={"record_id": target_id,
                                 "operation_id": operation_id})
                if target["status"] != "valid":
                    # 已有稳定失效来源，不得被新裁决覆盖
                    raise StoreError(
                        "RECORD_ALREADY_INVALID",
                        f"记录 {target_id} 已失效，失效来源稳定，"
                        "不能再次裁决",
                        status=409,
                        details={"record_id": target_id,
                                 "operation_id": operation_id})

                # 3) 求目标 + 全部可达下游闭包
                closure = self._downstream_closure_locked(target_id)

                # 4) 同一提交内将闭包中仍有效的节点失效；
                #    早已失效的节点保留其首次失效来源（稳定来源）。
                now = _utcnow()
                self._conn.execute(
                    "UPDATE records SET status='invalid', "
                    "invalidated_by=?, invalidated_at=? "
                    "WHERE id IN (%s) AND status='valid'"
                    % ",".join("?" * len(closure)),
                    (target_id, now, *closure))

                response = {
                    "operation_id": operation_id,
                    "result": "completed",
                    "replayed": False,
                    "target_record_id": target_id,
                    "cascade": [
                        {"id": rid, "invalidated_by": target_id}
                        for rid in closure
                    ],
                }
                inv_params = json.dumps(
                    {"kind": "invalidate", "target_record_id": target_id},
                    ensure_ascii=False, sort_keys=True)
                self._conn.execute(
                    "INSERT INTO operations (operation_id, kind, "
                    "target_record_id, params_json, response_json, created_at) "
                    "VALUES (?, 'invalidate', ?, ?, ?, ?)",
                    (operation_id, target_id, inv_params,
                     json.dumps(response, ensure_ascii=False), now))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return response

    def _downstream_closure_locked(self, target_id: str) -> list[str]:
        """目标及其全部可达下游（沿 parent->child 传播）。"""
        rows = self._conn.execute(
            """
            WITH RECURSIVE reach(id) AS (
                SELECT ?
                UNION
                SELECT e.child_id
                FROM edges e JOIN reach r ON e.parent_id = r.id
            )
            SELECT id FROM reach ORDER BY id
            """, (target_id,)).fetchall()
        return [r["id"] for r in rows]

    # ------------------------------------------------------------------ #
    # 替代重建（复制有效子树 + 旧支失效，单事务）
    # ------------------------------------------------------------------ #
    def rebuild(self, operation_id: str, target_id: str,
                replacement_payload: dict[str, Any]) -> dict[str, Any]:
        if not operation_id:
            raise StoreError("OPERATION_ID_REQUIRED",
                             "替代重建必须携带非空操作标识", status=400)
        if not isinstance(replacement_payload, dict):
            raise StoreError("REPLACEMENT_PAYLOAD_REQUIRED",
                             "替代重建必须提供替代读数 payload（对象）",
                             status=400)

        # 参数指纹：目标编号 + 替代内容。同标识同参数重放，改任一参数即冲突。
        params = {"kind": "rebuild", "target_record_id": target_id,
                  "replacement_payload": replacement_payload}
        params_json = json.dumps(params, ensure_ascii=False, sort_keys=True)

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # 1) 操作标识幂等 / 冲突判定（与失效裁决共用同一标识空间）
                op = self._conn.execute(
                    "SELECT kind, target_record_id, params_json, response_json "
                    "FROM operations WHERE operation_id=?",
                    (operation_id,)).fetchone()
                if op is not None:
                    if op["kind"] == "rebuild" and op["params_json"] == params_json:
                        # 同标识 + 同目标 + 同替代内容：重放首次映射，不改状态
                        replay = json.loads(op["response_json"])
                        replay["replayed"] = True
                        self._conn.execute("COMMIT")
                        return replay
                    raise StoreError(
                        "OPERATION_CONFLICT",
                        f"操作标识 {operation_id} 已用于 "
                        f"{op['kind']}({op['target_record_id']})，"
                        f"不能改用于 rebuild({target_id}) 的另一组参数",
                        status=409,
                        details={"operation_id": operation_id,
                                 "original_kind": op["kind"],
                                 "original_target": op["target_record_id"],
                                 "conflicting_kind": "rebuild",
                                 "conflicting_target": target_id})

                # 2) 事务内复查：目标存在、仍为“有效原始记录”
                target = self._conn.execute(
                    "SELECT id, kind, status FROM records WHERE id=?",
                    (target_id,)).fetchone()
                if target is None:
                    raise StoreError(
                        "RECORD_NOT_FOUND",
                        f"重建目标记录 {target_id} 不存在", status=404,
                        details={"record_id": target_id,
                                 "operation_id": operation_id})
                if target["kind"] != "raw":
                    raise StoreError(
                        "REBUILD_TARGET_MUST_BE_RAW",
                        f"记录 {target_id} 不是原始读数，"
                        "替代重建只能针对有效的原始记录发起",
                        details={"record_id": target_id, "kind": target["kind"]})
                if target["status"] != "valid":
                    # 与失效裁决竞争失败：拒绝且不留任何状态变化
                    raise StoreError(
                        "RECORD_ALREADY_INVALID",
                        f"记录 {target_id} 已失效，不能作为替代重建目标",
                        status=409,
                        details={"record_id": target_id,
                                 "operation_id": operation_id})

                # 3) 目标 + 全部可达下游闭包；仅仍有效的节点参与复制
                closure = self._downstream_closure_locked(target_id)
                qmarks = ",".join("?" * len(closure))
                rows = {r["id"]: r for r in self._conn.execute(
                    f"SELECT id, kind, payload, status, created_at "
                    f"FROM records WHERE id IN ({qmarks})", closure)}
                valid_ids = {rid for rid in closure
                             if rows[rid]["status"] == "valid"}

                # 读入闭包内的直接依据边（保留 seq 次序）
                edge_rows = self._conn.execute(
                    f"SELECT child_id, parent_id, seq FROM edges "
                    f"WHERE child_id IN ({qmarks}) ORDER BY child_id, seq",
                    closure).fetchall()
                parents_of: dict[str, list[str]] = {}
                children_of: dict[str, list[str]] = {}
                for e in edge_rows:
                    parents_of.setdefault(e["child_id"], []).append(
                        e["parent_id"])
                    children_of.setdefault(e["parent_id"], []).append(
                        e["child_id"])

                # 4) 写入前最后复查：每个受影响有效推导的“未替换直接依据”
                #    （不在有效闭包内、不会被复制的依据）必须仍有效。
                #    有效节点不可能依赖闭包内失效节点（全局不变量），
                #    此处捕获的是与本事务竞争后刚刚失效的闭包外依据。
                invalid_basis: dict[str, list[str]] = {}
                outside = {p for rid in valid_ids for p in parents_of.get(rid, [])
                           if p not in valid_ids}
                if outside:
                    oat = ",".join("?" * len(outside))
                    ostatus = {r["id"]: r["status"] for r in self._conn.execute(
                        f"SELECT id, status FROM records WHERE id IN ({oat})",
                        tuple(outside))}
                    for rid in valid_ids:
                        if rid == target_id:
                            continue
                        bad = [p for p in parents_of.get(rid, [])
                               if p in outside and ostatus.get(p) != "valid"]
                        if bad:
                            invalid_basis[rid] = bad
                if invalid_basis:
                    flat = sorted({p for ps in invalid_basis.values()
                                   for p in ps})
                    raise StoreError(
                        "REBUILD_BASIS_INVALID",
                        "替代重建写入前复查发现受影响推导的未替换直接依据"
                        "已失效，整笔重建拒绝（不产生任何副本）",
                        status=409,
                        details={"record_id": target_id,
                                 "invalid_parent_ids": flat,
                                 "affected_record_ids":
                                     sorted(invalid_basis)})

                # 5) 按依赖层级（Kahn 波次：所有闭包内依据就绪才进入下一层）
                #    创建副本。已重建依据替换为新编号，其余依据指向原记录。
                now = _utcnow()
                mapping: dict[str, str] = {}
                mapping_entries: list[dict[str, Any]] = []
                rebuilt_entries: list[dict[str, Any]] = []
                remaining = {rid: sum(1 for p in parents_of.get(rid, [])
                                      if p in valid_ids)
                             for rid in valid_ids}
                frontier = [target_id]
                level = 0
                while frontier:
                    frontier.sort(key=lambda r: (rows[r]["created_at"], r))
                    nxt: set[str] = set()
                    for old_id in frontier:
                        new_id = self._allocate_id_locked()
                        mapping[old_id] = new_id
                        payload = (replacement_payload if old_id == target_id
                                   else json.loads(rows[old_id]["payload"]))
                        self._conn.execute(
                            "INSERT INTO records (id, kind, payload, status, "
                            "invalidated_by, invalidated_at, created_at) "
                            "VALUES (?, ?, ?, 'valid', NULL, NULL, ?)",
                            (new_id, rows[old_id]["kind"],
                             json.dumps(payload, ensure_ascii=False), now))
                        new_parents: list[str] = []
                        for seq, p in enumerate(parents_of.get(old_id, [])):
                            np = mapping.get(p, p)  # 已重建->新编号，否则原记录
                            new_parents.append(np)
                            self._conn.execute(
                                "INSERT INTO edges (child_id, parent_id, seq) "
                                "VALUES (?, ?, ?)", (new_id, np, seq))
                        mapping_entries.append({
                            "old_id": old_id, "new_id": new_id,
                            "level": level, "status": "valid",
                            "parent_ids": new_parents})
                        rebuilt_entries.append({
                            "id": new_id, "status": "valid",
                            "parent_ids": new_parents})
                        for ch in children_of.get(old_id, []):
                            if ch in valid_ids:
                                remaining[ch] -= 1
                                if remaining[ch] == 0:
                                    nxt.add(ch)
                    frontier = list(nxt)
                    level += 1

                if len(mapping) != len(valid_ids):
                    # 理论上不可达（有效节点不可能经由失效节点仍可达），
                    # 防御性兜底：宁可回滚也不留半成品。
                    raise StoreError(
                        "REBUILD_INTERNAL_ERROR",
                        "依赖层级遍历未覆盖全部有效节点，已回滚", status=500,
                        details={"target_record_id": target_id,
                                 "expected": len(valid_ids),
                                 "mapped": len(mapping)})

                # 6) 旧根与旧下游整体失效（早已失效者保留其首次稳定来源）
                invalidated = [
                    {"id": rid, "invalidated_by": target_id}
                    for rid in closure if rows[rid]["status"] == "valid"]
                self._conn.execute(
                    "UPDATE records SET status='invalid', "
                    "invalidated_by=?, invalidated_at=? "
                    "WHERE id IN (%s) AND status='valid'"
                    % ",".join("?" * len(closure)),
                    (target_id, now, *closure))

                response = {
                    "operation_id": operation_id,
                    "result": "completed",
                    "replayed": False,
                    "target_record_id": target_id,
                    "replacement_record_id": mapping[target_id],
                    "mapping": mapping_entries,
                    "rebuilt": rebuilt_entries,
                    "invalidated": invalidated,
                }
                self._conn.execute(
                    "INSERT INTO operations (operation_id, kind, "
                    "target_record_id, params_json, response_json, created_at) "
                    "VALUES (?, 'rebuild', ?, ?, ?, ?)",
                    (operation_id, target_id, params_json,
                     json.dumps(response, ensure_ascii=False), now))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return response

    # ------------------------------------------------------------------ #
    # 完整性自检（验收用）
    # ------------------------------------------------------------------ #
    def assert_invariants(self) -> None:
        """有效记录不得依赖失效记录。"""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT c.id AS child_id, p.id AS parent_id
                FROM records c
                JOIN edges e ON e.child_id = c.id
                JOIN records p ON p.id = e.parent_id
                WHERE c.status='valid' AND p.status='invalid'
                LIMIT 1
                """).fetchone()
        if row is not None:
            raise AssertionError(
                f"不变量被破坏：有效记录 {row['child_id']} "
                f"依赖失效记录 {row['parent_id']}")
