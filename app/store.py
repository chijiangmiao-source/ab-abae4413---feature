"""标定谱系存储层。

核心不变量（均在单个 SQLite 写事务内保证）：

1. 一条失效裁决使目标记录及全部可达下游记录在同一持久化提交中失效；
2. 操作标识幂等：重复裁决返回首次结果；同一操作标识改换目标 -> 冲突且不改状态；
3. 新建推导记录时，任一前序不存在 / 已失效 / 自引用 / 成环 -> 整笔拒绝，既有结论不变；
4. 写事务串行化（BEGIN IMMEDIATE），因此“新推导”与“失效裁决”竞争后，
   不可能存在有效记录依赖失效记录；
5. 替代重建在同一写事务内复查目标仍为有效原始记录、确认受影响推导的未替换直接
   依据仍有效，随后按依赖层级为目标及全部可达的有效推导创建副本（已重建依据指向
   新编号，其余依据继续指向原记录），再使旧根与旧下游整体失效；同一修复标识携带
   相同目标与替代读数重试时重放首次映射，改换任一参数冲突且不留副本。重建与失效
   裁决串行竞争，结局只能是“完整重建”或“无状态变化的拒绝”。
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
    rebuilt_from    TEXT,
    rebuilt_by      TEXT,
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
    parameters_json  TEXT NOT NULL DEFAULT '{}',
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
            self._migrate_locked()

    def _migrate_locked(self) -> None:
        """对既有数据库渐进加列；列已存在时跳过。"""
        op_cols = {r["name"] for r in self._conn.execute(
            "PRAGMA table_info(operations)")}
        rec_cols = {r["name"] for r in self._conn.execute(
            "PRAGMA table_info(records)")}
        if "parameters_json" not in op_cols:
            self._conn.execute(
                "ALTER TABLE operations ADD COLUMN parameters_json "
                "TEXT NOT NULL DEFAULT '{}'")
        if "rebuilt_from" not in rec_cols:
            self._conn.execute(
                "ALTER TABLE records ADD COLUMN rebuilt_from TEXT")
        if "rebuilt_by" not in rec_cols:
            self._conn.execute(
                "ALTER TABLE records ADD COLUMN rebuilt_by TEXT")

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
            "rebuilt_from": row["rebuilt_from"],
            "rebuilt_by": row["rebuilt_by"],
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
                    "SELECT kind, target_record_id, parameters_json, "
                    "response_json FROM operations WHERE operation_id=?",
                    (operation_id,)).fetchone()
                if op is not None:
                    if (op["kind"] == "invalidate"
                            and op["target_record_id"] == target_id):
                        # 重复同一裁决：原样返回首次结果，不改状态
                        response = json.loads(op["response_json"])
                        response["replayed"] = True
                        self._conn.execute("COMMIT")
                        return response
                    raise self._operation_conflict_error(
                        operation_id, op, "invalidate", target_id)

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
                self._conn.execute(
                    "INSERT INTO operations (operation_id, kind, "
                    "target_record_id, parameters_json, response_json, "
                    "created_at) "
                    "VALUES (?, 'invalidate', ?, '{}', ?, ?)",
                    (operation_id, target_id,
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
    # 操作标识冲突（裁决与重建共用同一命名空间）
    # ------------------------------------------------------------------ #
    def _operation_conflict_error(self, operation_id: str, op: sqlite3.Row,
                                  conflict_kind: str,
                                  conflict_target: str) -> StoreError:
        return StoreError(
            "OPERATION_CONFLICT",
            f"操作标识 {operation_id} 已用于 "
            f"{op['kind']}({op['target_record_id']})，不能改用于 "
            f"{conflict_kind}({conflict_target})",
            status=409,
            details={"operation_id": operation_id,
                     "original_kind": op["kind"],
                     "original_target": op["target_record_id"],
                     "conflicting_kind": conflict_kind,
                     "conflicting_target": conflict_target})

    # ------------------------------------------------------------------ #
    # 替代重建（按依赖层级整支复制，单事务）
    # ------------------------------------------------------------------ #
    def rebuild(self, operation_id: str, target_id: str,
                replacement_payload: dict[str, Any]) -> dict[str, Any]:
        """以替代读数重建有效原始记录及其全部可达有效推导。

        单事务内：
          1. 复查目标仍为有效原始记录；
          2. 确认受影响推导的“未替换直接依据”仍全部有效；
          3. 按依赖层级（深度）为目标及全部可达有效推导创建副本——副本保留
             原有业务内容，已重建的直接依据指向对应新编号，其余依据继续指向
             原记录；
          4. 旧根与旧下游整体失效（失效来源稳定为旧根编号）。
        """
        if not operation_id:
            raise StoreError("OPERATION_ID_REQUIRED",
                             "替代重建必须携带稳定修复操作标识", status=400)
        if not isinstance(replacement_payload, dict):
            raise StoreError("INVALID_PAYLOAD",
                             "替代读数 replacement_payload 必须是对象",
                             status=400)

        parameters = {"target_record_id": target_id,
                      "replacement_payload": replacement_payload}

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # 1) 操作标识幂等 / 冲突判定（同命名空间，含失效裁决）
                op = self._conn.execute(
                    "SELECT operation_id, kind, target_record_id, "
                    "parameters_json, response_json FROM operations "
                    "WHERE operation_id=?", (operation_id,)).fetchone()
                if op is not None:
                    same = (op["kind"] == "rebuild"
                            and json.loads(op["parameters_json"] or "{}")
                            == parameters)
                    if same:
                        # 同修复标识 + 同目标 + 同替代内容：重放首次映射
                        response = json.loads(op["response_json"])
                        response["replayed"] = True
                        self._conn.execute("COMMIT")
                        return response
                    raise self._operation_conflict_error(
                        operation_id, op, "rebuild", target_id)

                # 2) 复查目标仍存在且为“有效原始记录”
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
                        "REBUILD_TARGET_NOT_RAW",
                        f"记录 {target_id} 不是原始读数，"
                        "替代重建只能针对有效的原始读数",
                        status=422,
                        details={"record_id": target_id, "kind": target["kind"]})
                if target["status"] != "valid":
                    # 与失效裁决竞争失败：无状态变化地拒绝
                    raise StoreError(
                        "RECORD_ALREADY_INVALID",
                        f"记录 {target_id} 已失效，不能发起替代重建；"
                        "请直接以替代读数建立新原始记录",
                        status=409,
                        details={"record_id": target_id,
                                 "operation_id": operation_id})

                # 3) 受影响集合：目标 + 沿依据边可达的“有效”推导闭包。
                #    已失效节点不复制（其结论早已不可用），且不会成为副本依据。
                affected = self._valid_downstream_locked(target_id)
                affected_set = set(affected)

                # 4) 写入前确认：受影响推导的未替换直接依据仍全部有效。
                #    写事务串行化下此快照即提交前的最终判定。
                stale: list[dict[str, str]] = []
                parent_rows = self._conn.execute(
                    f"""
                    SELECT e.child_id, e.parent_id
                    FROM edges e
                    JOIN records c ON c.id = e.child_id
                    JOIN records p ON p.id = e.parent_id
                    WHERE e.child_id IN ({','.join('?' * len(affected))})
                      AND c.status='valid'
                    ORDER BY e.child_id, e.seq
                    """, affected).fetchall()
                for r in parent_rows:
                    pid = r["parent_id"]
                    if pid in affected_set:
                        continue  # 已重建依据 -> 将替换为对应新编号
                    if self._conn.execute(
                            "SELECT status FROM records WHERE id=?",
                            (pid,)).fetchone()["status"] != "valid":
                        stale.append({"child_id": r["child_id"],
                                      "parent_id": pid})
                if stale:
                    raise StoreError(
                        "BASIS_ALREADY_INVALID",
                        "受影响推导存在已失效且不在重建范围内的直接依据，"
                        "不能整体替代重建",
                        details={"stale_basis": stale})

                # 5) 按依赖层级（深度）分配新编号并建副本。
                #    层级保证每个副本创建时，其已重建依据均已落库。
                layers = self._dependency_layers_locked(target_id, affected)
                now = _utcnow()
                mapping: dict[str, str] = {}
                copies: list[dict[str, Any]] = []
                for layer in layers:
                    for old_id in layer:
                        old = self._conn.execute(
                            "SELECT id, kind, payload FROM records "
                            "WHERE id=?", (old_id,)).fetchone()
                        new_id = self._allocate_id_locked()
                        if old["kind"] == "raw":
                            new_payload = replacement_payload
                            new_parents: list[str] = []
                        else:
                            new_payload = json.loads(old["payload"])  # 保留业务内容
                            old_parents = [r["parent_id"] for r in
                                           self._conn.execute(
                                               "SELECT parent_id FROM edges "
                                               "WHERE child_id=? ORDER BY seq",
                                               (old_id,))]
                            # 已重建依据 -> 新编号；其余依据 -> 继续指向原记录
                            new_parents = [mapping.get(p, p) for p in old_parents]
                        self._conn.execute(
                            "INSERT INTO records (id, kind, payload, status, "
                            "invalidated_by, invalidated_at, rebuilt_from, "
                            "rebuilt_by, created_at) "
                            "VALUES (?, ?, ?, 'valid', NULL, NULL, ?, ?, ?)",
                            (new_id, old["kind"],
                             json.dumps(new_payload, ensure_ascii=False),
                             old_id, operation_id, now))
                        for seq, pid in enumerate(new_parents):
                            self._conn.execute(
                                "INSERT INTO edges (child_id, parent_id, seq) "
                                "VALUES (?, ?, ?)", (new_id, pid, seq))
                        mapping[old_id] = new_id
                        copies.append({
                            "id": new_id,
                            "rebuilt_from": old_id,
                            "kind": old["kind"],
                            "status": "valid",
                            "parent_ids": new_parents,
                        })

                # 6) 旧根与旧下游（受影响有效集合）在同一提交内整体失效，
                #    失效来源稳定为旧根编号；此时所有新依据均已就位。
                self._conn.execute(
                    "UPDATE records SET status='invalid', "
                    "invalidated_by=?, invalidated_at=? "
                    "WHERE id IN (%s)" % ",".join("?" * len(affected)),
                    (target_id, now, *affected))

                response = {
                    "operation_id": operation_id,
                    "result": "completed",
                    "replayed": False,
                    "kind": "rebuild",
                    "target_record_id": target_id,
                    "replacement_record_id": mapping[target_id],
                    "mapping": [
                        {"old_id": old_id, "new_id": mapping[old_id]}
                        for old_id in affected
                    ],
                    "records": copies,
                }
                self._conn.execute(
                    "INSERT INTO operations (operation_id, kind, "
                    "target_record_id, parameters_json, response_json, "
                    "created_at) "
                    "VALUES (?, 'rebuild', ?, ?, ?, ?)",
                    (operation_id, target_id,
                     json.dumps(parameters, ensure_ascii=False),
                     json.dumps(response, ensure_ascii=False), now))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return response

    def _valid_downstream_locked(self, target_id: str) -> list[str]:
        """目标 + 仅经过有效节点可达的下游闭包（按编号排序）。"""
        rows = self._conn.execute(
            """
            WITH RECURSIVE reach(id) AS (
                SELECT ?
                UNION
                SELECT e.child_id
                FROM edges e
                JOIN reach r ON e.parent_id = r.id
                JOIN records c ON c.id = e.child_id AND c.status='valid'
            )
            SELECT id FROM reach ORDER BY id
            """, (target_id,)).fetchall()
        return [r["id"] for r in rows]

    def _dependency_layers_locked(
            self, root_id: str, ids: list[str]) -> list[list[str]]:
        """将闭包按“距目标根的依赖深度”分层，同层按编号排序。

        深度只统计落在闭包内部的依据边；副本的外部依据保持原编号，
        不影响建副本的先后顺序。
        """
        id_set = set(ids)
        depth: dict[str, int] = {root_id: 0}
        remaining = id_set - {root_id}
        # 迭代推进：深度 = 1 + max(闭包内依据的深度)
        while remaining:
            progressed = False
            for cid in list(remaining):
                parents = [r["parent_id"] for r in self._conn.execute(
                    "SELECT parent_id FROM edges WHERE child_id=?", (cid,))]
                inner = [p for p in parents if p in id_set]
                if all(p in depth for p in inner):
                    depth[cid] = 1 + max((depth[p] for p in inner),
                                         default=-1)
                    remaining.discard(cid)
                    progressed = True
            if not progressed:
                # 理论不可达：有效记录构成有向无环图（建记录时已拦环）
                raise StoreError("CYCLE_DETECTED",
                                 "重建闭包内出现无法分层的依赖环",
                                 details={"unresolved": sorted(remaining)})
        max_d = max(depth.values())
        layers: list[list[str]] = []
        for d in range(max_d + 1):
            layer = sorted(cid for cid, dep in depth.items() if dep == d)
            if layer:
                layers.append(layer)
        return layers

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
