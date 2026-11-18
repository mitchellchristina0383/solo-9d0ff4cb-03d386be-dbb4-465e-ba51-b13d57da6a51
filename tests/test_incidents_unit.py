"""不可用事件的进程内单元校验（不启动 HTTP 服务）。

覆盖难以在端到端场景里稳定构造的边界：
U1 事务原子性：事件插入失败时整体回滚，不留下 incident_id 指向不存在事件的半份记录
U2 同一轮异常重复上报只开一条事件（并发线程也如此）
U3 到期判定幂等：多个实例/多次查询并发判定只落一条
U4 恢复后的新一轮与新事件；结案保留起止时间
U5 恢复前启动的迟到异常不重开、不污染
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time

os.environ["CACHE_DB"] = tempfile.mkdtemp() + "/unit_cache.db"
os.environ.setdefault("INSTANCE_ID", "unit-cache")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3  # noqa: E402

import app.cache as c  # noqa: E402

failures = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global failures
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        failures += 1


def rows(sql: str) -> list[sqlite3.Row]:
    conn = sqlite3.connect(c.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def reset() -> None:
    conn = sqlite3.connect(c.DB_PATH)
    try:
        for t in ("snapshots", "inflight", "failures", "anomaly_state",
                  "incidents", "service_health"):
            conn.execute(f"DELETE FROM {t}")
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    c.init_db()
    threshold = c.INCIDENT_THRESHOLD

    # ---------------------------------------------------------------- U1
    reset()
    conn = sqlite3.connect(c.DB_PATH)
    conn.execute("CREATE TRIGGER IF NOT EXISTS block_inc BEFORE INSERT ON incidents "
                 "BEGIN SELECT RAISE(ABORT, 'blocked'); END")
    conn.commit()
    conn.close()

    c.INCIDENT_THRESHOLD = 0.0
    c._report_anomaly("svc", "first")
    opened = c._report_anomaly("svc", "second")  # 插入事件被触发器拒绝
    anomalies = rows("SELECT * FROM anomaly_state")
    incidents = rows("SELECT * FROM incidents")
    check("U1 事件写入失败整体回滚，无半份记录", opened is None and len(incidents) == 0
          and len(anomalies) == 1 and anomalies[0]["incident_id"] is None,
          f"opened={opened} incidents={len(incidents)} "
          f"anomaly_incident_id={anomalies[0]['incident_id'] if anomalies else None}")

    # 到期判定同样失败并回滚
    c._check_due_incidents("svc")
    check("U1b 到期判定写入失败同样回滚", len(rows("SELECT * FROM incidents")) == 0
          and len(rows("SELECT * FROM anomaly_state")) == 1)

    conn = sqlite3.connect(c.DB_PATH)
    conn.execute("DROP TRIGGER block_inc")
    conn.commit()
    conn.close()

    # ---------------------------------------------------------------- U2 / U3
    reset()
    c.INCIDENT_THRESHOLD = 0.25
    errors: list[Exception] = []

    def hammer(i: int) -> None:
        try:
            for _ in range(15):
                c._report_anomaly("svc", f"reason-{i}")
                if i % 3 == 0:
                    c._check_due_incidents("svc")
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=hammer, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(0.3)
    c._check_due_incidents(None)
    events = c._list_incidents("svc", None, 50)
    check("U2 并发重复上报只开一条事件", not errors and len(events["events"]) == 1
          and events["events"][0]["status"] == "open"
          and events["events"][0]["started_at"] <= events["events"][0]["detected_at"],
          f"errors={errors} events={len(events['events'])}")

    # ---------------------------------------------------------------- U4
    c._report_success("svc")
    first = c._list_incidents("svc", None, 50)["events"][0]
    check("U4 恢复结案并保留起止时间", first["status"] == "closed"
          and first["ended_at"] is not None and first["ended_at"] >= first["started_at"],
          f"started={first['started_at']:.3f} ended={first['ended_at']:.3f}")

    # 恢复后的新异常（请求开始时间晚于恢复）开启下一轮
    time.sleep(0.05)
    c._report_anomaly("svc", "round2")
    time.sleep(0.3)
    events = c._list_incidents("svc", None, 50)
    check("U4b 恢复后的新异常开启下一轮", len(events["events"]) == 2
          and events["events"][0]["status"] == "open"
          and events["events"][1]["status"] == "closed",
          f"events={[(e['id'], e['status']) for e in events['events']]}")

    # ---------------------------------------------------------------- U5
    c._report_success("svc")
    latest_success = rows("SELECT last_success_at FROM service_health")[0][0]
    # 恢复前启动、恢复后才返回的异常请求
    c._report_anomaly("svc", "late", started_at=latest_success - 0.5)
    time.sleep(0.35)
    c._check_due_incidents(None)
    events = c._list_incidents("svc", None, 50)
    check("U5 恢复前启动的迟到异常不重开、不污染", all(
        e["status"] == "closed" for e in events["events"])
        and len(events["events"]) == 2 and events["pending_anomaly"] is None
        and all(e["last_reason"] != "late" for e in events["events"]),
        f"events={[(e['id'], e['status']) for e in events['events']]} "
        f"pending={events['pending_anomaly']}")

    # 清理阈值，避免影响后续（本进程内不启动服务）
    c.INCIDENT_THRESHOLD = threshold
    print(f"\nUNIT FAILURES: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
