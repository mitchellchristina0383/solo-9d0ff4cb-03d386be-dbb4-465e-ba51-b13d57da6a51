"""路由查询缓存实例（cache）。

两个实例共享同一个 SQLite 数据库（docker volume / 本地共享目录），通过
SQLite 的行级事务协调"跨实例并发未命中合并（singleflight）"。

关键保证：
1. 修订单调：每次查询先向配置源确认当前修订；新修订提交后发起的查询
   不可能返回旧修订的节点。
2. 同修订并发未命中合并：同一 (service, revision) 同一时刻只允许一个实例
   回源，其余实例在库内等待结果（snapshot 或 failure 记录）。
3. 租约接管：回源持有者周期性续租；持有者超时或进程退出后，其他实例
   可在租约过期后接管回源。
4. 慢回源跨修订不写脏数据：回源返回后再次核对"该修订仍是当前修订"，
   过期修订只标记为 stale，绝不作为有效缓存。
5. 回源失败明确 503：失败记录带冷却期，冷却期内快速失败；旧快照
   仍可通过 /snapshots 与故障响应中的 last_good 排障。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_PATH = os.environ.get("CACHE_DB", "/data/cache.db")
SOURCE_BASE = os.environ.get("SOURCE_BASE", "http://source:8080")
INSTANCE_ID = os.environ.get("INSTANCE_ID", f"cache-{uuid.uuid4().hex[:8]}")
HTTP_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "6.0"))
LEASE_TTL = float(os.environ.get("LEASE_TTL", "2.0"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "0.7"))
FAILURE_COOLDOWN = float(os.environ.get("FAILURE_COOLDOWN", "3.0"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "0.15"))
# 不可用事件：异常持续达到该秒数且期间没有成功返回现役路由，才开启事件。
INCIDENT_THRESHOLD = float(os.environ.get("INCIDENT_THRESHOLD", "10.0"))

logger = logging.getLogger("cache.incidents")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = _connect()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            service     TEXT NOT NULL,
            revision    INTEGER NOT NULL,
            nodes       TEXT NOT NULL,
            status      TEXT NOT NULL,            -- valid | stale
            source_published_at REAL,
            cached_at   REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inflight (
            service     TEXT NOT NULL,
            revision    INTEGER NOT NULL,
            leader      TEXT NOT NULL,
            lease_expires_at REAL NOT NULL,
            started_at  REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS failures (
            service    TEXT NOT NULL,
            revision   INTEGER NOT NULL,
            error      TEXT NOT NULL,
            cooldown_until REAL NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )

    # ---- 不可用事件（incident）相关表 -------------------------------------
    # 当前尚未达到 10s 判定阈值的异常轮次（每个服务最多一轮）。
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS anomaly_state (
            service          TEXT PRIMARY KEY,
            round_started_at REAL NOT NULL,   -- 本轮首个异常响应的时间
            first_reason     TEXT NOT NULL,
            last_reason      TEXT NOT NULL,
            last_anomaly_at  REAL NOT NULL,
            incident_id      INTEGER          -- 达到阈值后关联的事件 id
        )
        """
    )
    # 事件：started_at=本轮首个异常；detected_at=达到阈值/落库时间；ended_at=恢复时间。
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS incidents (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            service        TEXT NOT NULL,
            started_at     REAL NOT NULL,
            detected_at    REAL NOT NULL,
            ended_at       REAL,
            first_reason   TEXT NOT NULL,
            last_reason    TEXT NOT NULL,
            last_anomaly_at REAL NOT NULL,
            opened_by      TEXT NOT NULL
        )
        """
    )
    # 同一服务同一时刻至多一条未结案事件（跨实例原子约束）。
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS incidents_one_open "
        "ON incidents(service) WHERE ended_at IS NULL"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS incidents_by_service "
        "ON incidents(service, started_at)"
    )
    # 最近一次"成功返回已核实现役路由"的时间，用于识别恢复前启动、恢复后才
    # 返回的迟到异常请求（不能据此重新开单）。
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_health (
            service         TEXT PRIMARY KEY,
            last_success_at REAL NOT NULL
        )
        """
    )
    conn.close()


# ---------------------------------------------------------------------------
# 配置源客户端
# ---------------------------------------------------------------------------
class SourceUnavailable(Exception):
    pass


async def _source_current(service: str) -> int:
    try:
        async with httpx.AsyncClient(base_url=SOURCE_BASE, timeout=HTTP_TIMEOUT) as c:
            r = await c.get(f"/services/{service}/current")
            if r.status_code == 404:
                raise HTTPException(status_code=404, detail=f"service '{service}' not found")
            r.raise_for_status()
            return int(r.json()["revision"])
    except HTTPException:
        raise
    except Exception as exc:  # 连不上 / 超时 / 5xx
        raise SourceUnavailable(f"current revision lookup failed: {exc}") from exc


async def _source_fetch(service: str, revision: int) -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=SOURCE_BASE, timeout=HTTP_TIMEOUT) as c:
        r = await c.get(f"/services/{service}/revisions/{revision}")
        if r.status_code >= 400:
            raise SourceUnavailable(f"origin returned HTTP {r.status_code}: {r.text[:200]}")
        return r.json()


# ---------------------------------------------------------------------------
# DB 辅助
# ---------------------------------------------------------------------------
def _get_snapshot(conn: sqlite3.Connection, service: str, revision: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM snapshots WHERE service=? AND revision=?", (service, revision)
    ).fetchone()


def _last_good(conn: sqlite3.Connection, service: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM snapshots WHERE service=? AND status='valid' "
        "ORDER BY revision DESC LIMIT 1",
        (service,),
    ).fetchone()
    if row is None:
        return None
    return {
        "service": row["service"],
        "revision": row["revision"],
        "nodes": json.loads(row["nodes"]),
        "cached_at": row["cached_at"],
    }


def _try_acquire_lease(conn: sqlite3.Connection, service: str, revision: int) -> bool:
    """尝试成为 (service, revision) 的回源 leader；已有未过期租约则失败。"""
    now = time.time()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT leader, lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, revision),
        ).fetchone()
        if row is not None and row["lease_expires_at"] > now:
            conn.commit()
            return False
        # 无记录，或租约已过期 -> 接管
        conn.execute(
            "INSERT INTO inflight(service, revision, leader, lease_expires_at, started_at) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(service, revision) DO UPDATE SET "
            "leader=excluded.leader, lease_expires_at=excluded.lease_expires_at, "
            "started_at=excluded.started_at",
            (service, revision, INSTANCE_ID, now + LEASE_TTL, now),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


def _renew_lease(conn: sqlite3.Connection, service: str, revision: int) -> bool:
    now = time.time()
    row = conn.execute(
        "SELECT leader FROM inflight WHERE service=? AND revision=?", (service, revision)
    ).fetchone()
    if row is None or row["leader"] != INSTANCE_ID:
        return False
    conn.execute(
        "UPDATE inflight SET lease_expires_at=? WHERE service=? AND revision=? AND leader=?",
        (now + LEASE_TTL, service, revision, INSTANCE_ID),
    )
    return True


def _release_lease(conn: sqlite3.Connection, service: str, revision: int) -> None:
    conn.execute(
        "DELETE FROM inflight WHERE service=? AND revision=? AND leader=?",
        (service, revision, INSTANCE_ID),
    )


def _record_failure(conn: sqlite3.Connection, service: str, revision: int, error: str) -> None:
    now = time.time()
    conn.execute(
        "INSERT INTO failures(service, revision, error, cooldown_until, updated_at) "
        "VALUES(?,?,?,?,?) "
        "ON CONFLICT(service, revision) DO UPDATE SET error=excluded.error, "
        "cooldown_until=excluded.cooldown_until, updated_at=excluded.updated_at",
        (service, revision, error[:500], now + FAILURE_COOLDOWN, now),
    )


def _save_snapshot(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    nodes: list[str],
    source_published_at: float | None,
    still_current: bool,
) -> str:
    """写入快照；仅当修订仍为当前修订时标记 valid，否则标记 stale。"""
    now = time.time()
    status = "valid" if still_current else "stale"
    conn.execute(
        "INSERT INTO snapshots(service, revision, nodes, status, source_published_at, cached_at) "
        "VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(service, revision) DO UPDATE SET nodes=excluded.nodes, "
        "status=excluded.status, source_published_at=excluded.source_published_at, "
        "cached_at=excluded.cached_at",
        (service, revision, json.dumps(nodes), status, source_published_at, now),
    )
    return status


# ---------------------------------------------------------------------------
# 不可用事件（incident）记录
# ---------------------------------------------------------------------------
# 设计要点：
# - 只有"最终未能返回已核实的现役路由"的请求才进入本模块：降级路由（源整体
#   不可达但有本地快照）与 503 都算异常；409（慢回源跨修订）是"重试"语义，
#   不属于用户可见的不可用，不参与判定。
# - 每个服务在共享库中最多有一轮"进行中异常"（anomaly_state），跨实例共享：
#   任意实例观察到首个异常即记录 round_started_at / first_reason / last_reason。
# - 持续达到 INCIDENT_THRESHOLD 且期间没有任何成功返回现役路由，才把这一轮
#   落成一条事件（同一事务内写 anomaly_state.incident_id + incidents 行）。
# - 成功返回现役路由即恢复：更新 service_health.last_success_at、结案未结案
#   事件、清空异常轮次。恢复前启动、恢复后才返回的迟到异常请求以其"请求开始
#   时间"早于 last_success_at，不会重新开单。
# - 所有写入在单个 BEGIN IMMEDIATE 事务内完成；失败则整体回滚，绝不留下
#   "有异常轮次但无事件"或"事件写入一半"的记录。异常记录失败不影响路由响应本身。


def _request_started_at(started_at: float | None) -> float:
    """调用方可传入请求开始时间；未传入则取当前时间。"""
    return time.time() if started_at is None else started_at


def _report_anomaly(
    service: str, reason: str, started_at: float | None = None
) -> int | None:
    """记录一次"最终未返回已核实现役路由"的请求（降级路由或 503）。

    started_at 是请求开始时间，仅用于识别"恢复前启动、恢复后才返回"的迟到
    请求；异常观察时间（首次/最近异常）取本次记录的时刻。

    返回本轮已开启事件的 id（若达到阈值并成功落库），否则 None。
    写入失败只记日志，不影响路由响应。
    """
    request_started_at = _request_started_at(started_at)
    now = time.time()
    reason = (reason or "unavailable")[:500]
    try:
        conn = _connect()
    except Exception:
        logger.exception("incident record: cannot open db for service=%s", service)
        return None
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 恢复后启动的新请求属于新一轮；恢复前启动、恢复后才返回的迟到
            # 异常（request_started_at < last_success_at）只能补充历史原因，
            # 不能重新开单。
            health = conn.execute(
                "SELECT last_success_at FROM service_health WHERE service=?", (service,)
            ).fetchone()
            last_success_at = health["last_success_at"] if health else None
            late = last_success_at is not None and request_started_at < last_success_at

            state = conn.execute(
                "SELECT * FROM anomaly_state WHERE service=?", (service,)
            ).fetchone()

            if late:
                # 恢复前启动、恢复后才返回的迟到异常：本轮已随恢复结案，
                # 不能再开单，也不去污染恢复后新一轮的事件原因。
                conn.commit()
                return None

            if state is None:
                # 新一轮：首个异常，记录首次异常时间与最近原因（尚未落事件）。
                conn.execute(
                    "INSERT INTO anomaly_state(service, round_started_at, first_reason, "
                    "last_reason, last_anomaly_at, incident_id) VALUES(?,?,?,?,?,NULL)",
                    (service, now, reason, reason, now),
                )
                conn.commit()
                return None

            round_started_at = state["round_started_at"]
            last_anomaly_at = max(now, state["last_anomaly_at"])

            if state["incident_id"] is not None:
                # 本轮事件已开启：只更新最近原因。
                conn.execute(
                    "UPDATE anomaly_state SET last_reason=?, last_anomaly_at=? "
                    "WHERE service=? AND round_started_at=?",
                    (reason, last_anomaly_at, service, round_started_at),
                )
                conn.execute(
                    "UPDATE incidents SET last_reason=?, last_anomaly_at=? "
                    "WHERE id=? AND ended_at IS NULL",
                    (reason, last_anomaly_at, state["incident_id"]),
                )
                conn.commit()
                return int(state["incident_id"])

            # 本轮仍在阈值内：更新最近异常。
            conn.execute(
                "UPDATE anomaly_state SET last_reason=?, last_anomaly_at=? "
                "WHERE service=? AND round_started_at=?",
                (reason, last_anomaly_at, service, round_started_at),
            )

            # 到期判定：持续达到阈值才开启事件（查询事件时也会再次判定）。
            if now - round_started_at >= INCIDENT_THRESHOLD:
                # 防御：若已存在未结案事件（例如两实例并发到期），本轮关联到它，
                # 不再插入第二条（也受部分唯一索引保护）。
                existing = conn.execute(
                    "SELECT id FROM incidents WHERE service=? AND ended_at IS NULL "
                    "ORDER BY started_at DESC LIMIT 1",
                    (service,),
                ).fetchone()
                if existing is not None:
                    conn.execute(
                        "UPDATE anomaly_state SET incident_id=? WHERE service=?",
                        (existing["id"], service),
                    )
                    conn.commit()
                    return int(existing["id"])

                cur = conn.execute(
                    "INSERT INTO incidents(service, started_at, detected_at, ended_at, "
                    "first_reason, last_reason, last_anomaly_at, opened_by) "
                    "VALUES(?,?,?,NULL,?,?,?,?)",
                    (service, round_started_at, now, state["first_reason"],
                     reason, last_anomaly_at, INSTANCE_ID),
                )
                incident_id = cur.lastrowid
                conn.execute(
                    "UPDATE anomaly_state SET incident_id=?, last_reason=?, "
                    "last_anomaly_at=? WHERE service=? AND round_started_at=?",
                    (incident_id, reason, last_anomaly_at, service, round_started_at),
                )
                conn.commit()
                logger.info("incident opened id=%s service=%s started=%.3f",
                            incident_id, service, round_started_at)
                return incident_id
            conn.commit()
            return None
        except Exception:
            conn.rollback()
            raise
    except Exception:
        logger.exception("incident record failed for service=%s; rolled back", service)
        return None
    finally:
        conn.close()


def _report_success(service: str) -> None:
    """成功返回现役路由：记录恢复时间并结案。

    last_success_at 为本请求完成（现役路由被核实）的时刻，只前进不后退。
    请求开始时间早于它的异常响应即"恢复前的在途请求"，不会重新开单。
    """
    now = time.time()
    try:
        conn = _connect()
    except Exception:
        logger.exception("incident success record: cannot open db for service=%s", service)
        return
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            health = conn.execute(
                "SELECT last_success_at FROM service_health WHERE service=?", (service,)
            ).fetchone()
            last_success = now
            if health is not None:
                last_success = max(health["last_success_at"], now)

            conn.execute(
                "INSERT INTO service_health(service, last_success_at) VALUES(?,?) "
                "ON CONFLICT(service) DO UPDATE SET last_success_at=excluded.last_success_at",
                (service, last_success),
            )
            # 结案该服务全部未结案事件（正常情况至多一条；跨实例兜底）。
            conn.execute(
                "UPDATE incidents SET ended_at=? WHERE service=? AND ended_at IS NULL",
                (now, service),
            )
            # 清理所有异常轮次：已开启的轮次随事件结案；未达阈值的轮次作废。
            conn.execute("DELETE FROM anomaly_state WHERE service=?", (service,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    except Exception:
        logger.exception("incident success record failed for service=%s; rolled back", service)
    finally:
        conn.close()


def _check_due_incidents(service: str | None = None) -> list[dict[str, Any]]:
    """到期判定：对持续时间已达阈值的异常轮次开启事件。

    同时被查询事件接口（/events）与异常记录路径调用，因此"查询事件时也能
    完成到期判定"；即使期间没有新的异常请求，持续异常也会在查询时落成事件。
    """
    now = time.time()
    opened: list[dict[str, Any]] = []
    try:
        conn = _connect()
    except Exception:
        logger.exception("incident due-check: cannot open db")
        return opened
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if service is None:
                states = conn.execute(
                    "SELECT * FROM anomaly_state WHERE incident_id IS NULL"
                ).fetchall()
            else:
                states = conn.execute(
                    "SELECT * FROM anomaly_state WHERE service=? AND incident_id IS NULL",
                    (service,),
                ).fetchall()
            for state in states:
                if now - state["round_started_at"] < INCIDENT_THRESHOLD:
                    continue
                # 防御：若已有未结案事件（不应发生），本轮直接关联到它。
                existing = conn.execute(
                    "SELECT id FROM incidents WHERE service=? AND ended_at IS NULL "
                    "ORDER BY started_at DESC LIMIT 1",
                    (state["service"],),
                ).fetchone()
                if existing is not None:
                    conn.execute(
                        "UPDATE anomaly_state SET incident_id=? WHERE service=?",
                        (existing["id"], state["service"]),
                    )
                    continue
                cur = conn.execute(
                    "INSERT INTO incidents(service, started_at, detected_at, ended_at, "
                    "first_reason, last_reason, last_anomaly_at, opened_by) "
                    "VALUES(?,?,?,NULL,?,?,?,?)",
                    (state["service"], state["round_started_at"], now,
                     state["first_reason"], state["last_reason"],
                     state["last_anomaly_at"], INSTANCE_ID),
                )
                incident_id = cur.lastrowid
                conn.execute(
                    "UPDATE anomaly_state SET incident_id=? WHERE service=? AND round_started_at=?",
                    (incident_id, state["service"], state["round_started_at"]),
                )
                opened.append({"id": incident_id, "service": state["service"]})
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    except Exception:
        logger.exception("incident due-check failed; rolled back")
        return opened
    finally:
        conn.close()
    return opened


def _incident_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "service": row["service"],
        "status": "open" if row["ended_at"] is None else "closed",
        "started_at": row["started_at"],
        "detected_at": row["detected_at"],
        "ended_at": row["ended_at"],
        "duration_seconds": round(
            (row["ended_at"] if row["ended_at"] is not None else time.time())
            - row["started_at"], 3),
        "first_reason": row["first_reason"],
        "last_reason": row["last_reason"],
        "last_anomaly_at": row["last_anomaly_at"],
        "opened_by": row["opened_by"],
    }


# ---------------------------------------------------------------------------
# 核心：singleflight 查询
# ---------------------------------------------------------------------------
async def _query(service: str, started_at: float | None = None) -> dict[str, Any]:
    # 第一步：确认当前修订。配置源整体不可达时降级：
    # 有本地有效快照则降级服务（标记 degraded），无快照才明确 503。
    # 降级路由不再视为"已核实现役"，按异常参与事件判定。
    try:
        current = await _source_current(service)
    except SourceUnavailable:
        conn = _connect()
        try:
            good = _last_good(conn, service)
        finally:
            conn.close()
        if good is None:
            reason = "origin unavailable and no local snapshot"
            _report_anomaly(service, reason, started_at)
            raise HTTPException(
                status_code=503,
                detail={
                    "error": reason,
                    "service": service,
                    "last_good": None,
                },
            )
        result = {
            "service": service,
            "revision": good["revision"],
            "nodes": good["nodes"],
            "served_by": INSTANCE_ID,
            "from_cache": True,
            "degraded": True,
            "cached_at": good["cached_at"],
        }
        _report_anomaly(
            service,
            "origin unreachable; serving degraded last-known snapshot "
            f"(revision {result['revision']})",
            started_at,
        )
        return result

    conn = _connect()
    try:
        snap = _get_snapshot(conn, service, current)
        if snap is not None and snap["status"] == "valid":
            result = {
                "service": service,
                "revision": current,
                "nodes": json.loads(snap["nodes"]),
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "cached_at": snap["cached_at"],
            }
            _report_success(service)
            return result

        # 失败冷却期内快速明确失败（旧快照仍可单独查询）。
        # 若仍有 leader 持有效租约（其回源可能仍在途），不提前失败，转入等待/接管逻辑。
        fail = conn.execute(
            "SELECT * FROM failures WHERE service=? AND revision=?", (service, current)
        ).fetchone()
        inflight_row = conn.execute(
            "SELECT lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, current),
        ).fetchone()
        lease_alive = inflight_row is not None and inflight_row["lease_expires_at"] > time.time()
        if fail is not None and fail["cooldown_until"] > time.time() and not lease_alive:
            _report_anomaly(service, fail["error"], started_at)
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "origin unavailable (failure cooldown)",
                    "reason": fail["error"],
                    "retry_after": round(fail["cooldown_until"] - time.time(), 2),
                    "last_good": _last_good(conn, service),
                },
            )

        is_leader = _try_acquire_lease(conn, service, current)

        if is_leader:
            result = await _run_as_leader(conn, service, current, started_at)
        else:
            result = await _wait_as_follower(conn, service, current, started_at)
        _report_success(service)
        return result
    finally:
        conn.close()


async def _run_as_leader(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    started_at: float | None = None,
) -> dict[str, Any]:
    heartbeat_task = asyncio.create_task(_heartbeat(service, revision))
    try:
        # 硬时限：回源不能无限占用回源资格；超时后停心跳、放租约，
        # 其他实例可在租约过期后接管（即使本进程仍然存活）。
        payload = await asyncio.wait_for(
            _source_fetch(service, revision), timeout=HTTP_TIMEOUT
        )
    except asyncio.TimeoutError:
        heartbeat_task.cancel()
        conn2 = _connect()
        try:
            conn2.execute("BEGIN IMMEDIATE")
            _record_failure(conn2, service, revision, f"origin fetch timeout after {HTTP_TIMEOUT}s")
            _release_lease(conn2, service, revision)
            conn2.commit()
        finally:
            conn2.close()
        reason = f"origin fetch timeout after {HTTP_TIMEOUT}s"
        _report_anomaly(service, reason, started_at)
        raise HTTPException(
            status_code=503,
            detail={
                "error": "origin unavailable",
                "reason": reason,
                "service": service,
                "requested_revision": revision,
                "last_good": _last_good(conn, service),
            },
        )
    except SourceUnavailable as exc:
        heartbeat_task.cancel()
        conn2 = _connect()
        try:
            conn2.execute("BEGIN IMMEDIATE")
            _record_failure(conn2, service, revision, str(exc))
            _release_lease(conn2, service, revision)
            conn2.commit()
        finally:
            conn2.close()
        _report_anomaly(service, str(exc), started_at)
        raise HTTPException(
            status_code=503,
            detail={
                "error": "origin unavailable",
                "reason": str(exc),
                "service": service,
                "requested_revision": revision,
                "last_good": _last_good(conn, service),
            },
        )
    except Exception as exc:
        heartbeat_task.cancel()
        conn2 = _connect()
        try:
            conn2.execute("BEGIN IMMEDIATE")
            _record_failure(conn2, service, revision, f"unexpected: {exc!r}")
            _release_lease(conn2, service, revision)
            conn2.commit()
        finally:
            conn2.close()
        _report_anomaly(service, f"unexpected: {exc!r}", started_at)
        raise HTTPException(
            status_code=503,
            detail={"error": "origin unavailable", "reason": repr(exc),
                    "last_good": _last_good(conn, service)},
        )
    else:
        heartbeat_task.cancel()

    # 回源成功：核对该修订是否仍是当前修订，防止慢回源把旧响应写成有效缓存
    try:
        latest = await _source_current(service)
    except SourceUnavailable:
        latest = revision  # 当前修订查询可靠；极端情况下不降级，按当前值处理
    still_current = latest == revision

    conn3 = _connect()
    try:
        conn3.execute("BEGIN IMMEDIATE")
        status = _save_snapshot(
            conn3,
            service,
            revision,
            payload["nodes"],
            payload.get("published_at"),
            still_current,
        )
        # 成功后清除失败记录与租约
        conn3.execute(
            "DELETE FROM failures WHERE service=? AND revision=?", (service, revision)
        )
        _release_lease(conn3, service, revision)
        conn3.commit()
    finally:
        conn3.close()

    if not still_current:
        # 旧修订已过期：不把它当有效结果返回，让请求走新修订
        raise HTTPException(
            status_code=409,
            detail={
                "error": "revision advanced during slow origin fetch; retry for latest",
                "fetched_revision": revision,
                "current_revision": latest,
            },
        )

    return {
        "service": service,
        "revision": revision,
        "nodes": payload["nodes"],
        "served_by": INSTANCE_ID,
        "from_cache": False,
        "cached_at": time.time(),
        "snapshot_status": status,
    }


async def _heartbeat(service: str, revision: int) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            conn = _connect()
            try:
                if not _renew_lease(conn, service, revision):
                    return  # 已被接管
            finally:
                conn.close()
        except Exception:
            pass


async def _wait_as_follower(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    started_at: float | None = None,
) -> dict[str, Any]:
    # 覆盖：leader 硬超时 + 租约 TTL + 接管者自己的回源时间，再加余量
    deadline = time.time() + HTTP_TIMEOUT * 3 + LEASE_TTL * 2
    while time.time() < deadline:
        await asyncio.sleep(POLL_INTERVAL)

        snap = _get_snapshot(conn, service, revision)
        if snap is not None and snap["status"] == "valid":
            return {
                "service": service,
                "revision": revision,
                "nodes": json.loads(snap["nodes"]),
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "coalesced": True,
                "cached_at": snap["cached_at"],
            }

        # leader 失败：共享失败记录生效，冷却期内明确 503。
        # 但若仍有 leader 持有有效租约（例如其回源挂起、只是客户端先超时），
        # 不能直接失败，应等待租约过期后接管。
        fail = conn.execute(
            "SELECT * FROM failures WHERE service=? AND revision=?", (service, revision)
        ).fetchone()
        inflight_row = conn.execute(
            "SELECT lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, revision),
        ).fetchone()
        lease_alive = inflight_row is not None and inflight_row["lease_expires_at"] > time.time()
        if fail is not None and not lease_alive:
            now = time.time()
            if fail["cooldown_until"] > now:
                _report_anomaly(service, fail["error"], started_at)
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "origin unavailable (coalesced leader failure)",
                        "reason": fail["error"],
                        "retry_after": round(fail["cooldown_until"] - now, 2),
                        "last_good": _last_good(conn, service),
                    },
                )
            # 冷却已过：清掉旧失败记录，尝试接管重新回源
            conn.execute(
                "DELETE FROM failures WHERE service=? AND revision=?", (service, revision)
            )
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(conn, service, revision, started_at)

        # 租约过期（leader 超时/退出）：接管
        if inflight_row is None:
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(conn, service, revision, started_at)
        elif inflight_row["lease_expires_at"] <= time.time():
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(conn, service, revision, started_at)

    reason = "timed out waiting for in-flight origin fetch"
    _report_anomaly(service, reason, started_at)
    raise HTTPException(
        status_code=503,
        detail={"error": reason, "last_good": _last_good(conn, service)},
    )


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(
    title=f"路由查询缓存 API（{INSTANCE_ID}）",
    description=(
        "两个缓存实例共享 SQLite 状态，提供跨实例并发合并、租约接管与"
        "修订一致性保证。/docs 可直接操作查询、快照排障与状态检查。"
    ),
    version="1.0.0",
)


@app.on_event("startup")
async def _startup() -> None:
    init_db()


class QueryResult(BaseModel):
    service: str
    revision: int
    nodes: list[str]
    served_by: str
    from_cache: bool
    coalesced: bool | None = None
    degraded: bool | None = None
    cached_at: float | None = None
    snapshot_status: str | None = None


class Incident(BaseModel):
    id: int
    service: str
    status: str
    started_at: float
    detected_at: float
    ended_at: float | None = None
    duration_seconds: float
    first_reason: str
    last_reason: str
    last_anomaly_at: float
    opened_by: str


class PendingAnomaly(BaseModel):
    round_started_at: float
    first_reason: str
    last_reason: str
    last_anomaly_at: float
    seconds_elapsed: float
    threshold_seconds: float


class IncidentList(BaseModel):
    service: str | None = None
    threshold_seconds: float
    events: list[Incident]
    open_event: Incident | None = None
    pending_anomaly: PendingAnomaly | None = None


def _pending_anomaly(conn: sqlite3.Connection, service: str, now: float) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM anomaly_state WHERE service=? AND incident_id IS NULL", (service,)
    ).fetchone()
    if row is None:
        return None
    return {
        "round_started_at": row["round_started_at"],
        "first_reason": row["first_reason"],
        "last_reason": row["last_reason"],
        "last_anomaly_at": row["last_anomaly_at"],
        "seconds_elapsed": round(now - row["round_started_at"], 3),
        "threshold_seconds": INCIDENT_THRESHOLD,
    }


def _list_incidents(
    service: str | None, status: str | None, limit: int
) -> dict[str, Any]:
    # 查询即完成到期判定（即使期间没有新的异常请求）。
    _check_due_incidents(service)

    conn = _connect()
    try:
        sql = "SELECT * FROM incidents"
        clauses: list[str] = []
        params: list[Any] = []
        if service is not None:
            clauses.append("service=?")
            params.append(service)
        if status == "open":
            clauses.append("ended_at IS NULL")
        elif status == "closed":
            clauses.append("ended_at IS NOT NULL")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY started_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(limit, 500)))
        rows = conn.execute(sql, params).fetchall()

        events = [_incident_row(r) for r in rows]

        # open_event / pending_anomaly 只按服务暴露，避免全局列表产生歧义。
        open_event = None
        pending = None
        if service is not None:
            open_row = conn.execute(
                "SELECT * FROM incidents WHERE service=? AND ended_at IS NULL "
                "ORDER BY started_at DESC LIMIT 1",
                (service,),
            ).fetchone()
            open_event = _incident_row(open_row) if open_row is not None else None
            pending = _pending_anomaly(conn, service, time.time())
        return {
            "service": service,
            "threshold_seconds": INCIDENT_THRESHOLD,
            "events": events,
            "open_event": open_event,
            "pending_anomaly": pending,
        }
    finally:
        conn.close()


@app.get("/routes/{service}", response_model=QueryResult, summary="查询服务当前路由（含修订）")
async def query_route(service: str) -> dict[str, Any]:
    return await _query(service, started_at=time.time())


@app.get(
    "/events",
    response_model=IncidentList,
    summary="列出不可用事件（可按服务/状态过滤；查询时完成到期判定）",
)
async def list_events(
    service: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    if status not in (None, "open", "closed"):
        raise HTTPException(status_code=400, detail="status must be open/closed")
    return await asyncio.to_thread(_list_incidents, service, status, limit)


@app.get(
    "/events/{service}",
    response_model=IncidentList,
    summary="按服务列出不可用事件（含进行中异常与未结案事件；查询时完成到期判定）",
)
async def list_service_events(
    service: str,
    status: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    if status not in (None, "open", "closed"):
        raise HTTPException(status_code=400, detail="status must be open/closed")
    return await asyncio.to_thread(_list_incidents, service, status, limit)


@app.get("/snapshots/{service}", summary="列出该服务全部缓存快照（含 stale，供排障）")
async def list_snapshots(service: str) -> dict[str, Any]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT service, revision, nodes, status, source_published_at, cached_at "
            "FROM snapshots WHERE service=? ORDER BY revision",
            (service,),
        ).fetchall()
        return {
            "service": service,
            "snapshots": [
                {
                    "revision": r["revision"],
                    "nodes": json.loads(r["nodes"]),
                    "status": r["status"],
                    "source_published_at": r["source_published_at"],
                    "cached_at": r["cached_at"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/snapshots/{service}/revisions/{revision}", summary="查看指定修订的历史快照（排障）")
async def get_snapshot(service: str, revision: int) -> dict[str, Any]:
    conn = _connect()
    try:
        r = _get_snapshot(conn, service, revision)
        if r is None:
            raise HTTPException(status_code=404, detail="snapshot not found")
        return {
            "service": service,
            "revision": revision,
            "nodes": json.loads(r["nodes"]),
            "status": r["status"],
            "source_published_at": r["source_published_at"],
            "cached_at": r["cached_at"],
        }
    finally:
        conn.close()


@app.get("/debug/state", summary="共享数据库内部状态（inflight/failures/快照）")
async def debug_state() -> dict[str, Any]:
    conn = _connect()
    try:
        inflight = [
            dict(r)
            for r in conn.execute(
                "SELECT service, revision, leader, lease_expires_at, started_at FROM inflight"
            ).fetchall()
        ]
        failures = [dict(r) for r in conn.execute("SELECT * FROM failures").fetchall()]
        snaps = [
            {
                "service": r["service"],
                "revision": r["revision"],
                "status": r["status"],
                "cached_at": r["cached_at"],
            }
            for r in conn.execute(
                "SELECT service, revision, status, cached_at FROM snapshots "
                "ORDER BY service, revision"
            ).fetchall()
        ]
        return {
            "instance_id": INSTANCE_ID,
            "now": time.time(),
            "inflight": inflight,
            "failures": failures,
            "snapshots": snaps,
        }
    finally:
        conn.close()


@app.post("/debug/reset", summary="清空共享缓存状态（测试用）")
async def debug_reset() -> dict[str, Any]:
    conn = _connect()
    try:
        conn.execute("DELETE FROM snapshots")
        conn.execute("DELETE FROM inflight")
        conn.execute("DELETE FROM failures")
        conn.execute("DELETE FROM anomaly_state")
        conn.execute("DELETE FROM incidents")
        conn.execute("DELETE FROM service_health")
    finally:
        conn.close()
    return {"reset": True, "by": INSTANCE_ID}


@app.get("/health", summary="健康检查")
async def health() -> dict[str, str]:
    return {"status": "ok", "instance_id": INSTANCE_ID}
