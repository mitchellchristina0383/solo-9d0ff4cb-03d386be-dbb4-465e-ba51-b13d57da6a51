"""不可用事件并发语义的端到端验证（两实例共享同一 SQLite）。

在单元测试（进程内多线程）之外，再以两个真实的 cache 进程并发压测：
E1 两实例同时持续异常：只产生一条事件
E2 两实例同时恢复：事件被结案一次，起止时间有效
E3 交替异常/恢复多轮：轮次边界清晰，每轮至多一条事件
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
DATA = ROOT / "data"
SRC, C1, C2 = "http://127.0.0.1:8080", "http://127.0.0.1:8081", "http://127.0.0.1:8082"
SERVICE = "ledger"
THRESHOLD = 0.9

results: list[tuple[str, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append(("PASS" if ok else "FAIL", name, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


async def main() -> int:
    DATA.mkdir(exist_ok=True)
    (DATA / "logs").mkdir(parents=True, exist_ok=True)
    db, sdb = DATA / "conc_cache.db", DATA / "conc_source.db"
    for f in (db, sdb):
        for s in ("", "-wal", "-shm"):
            Path(str(f) + s).unlink(missing_ok=True)

    env = {"SOURCE_DB": str(sdb), "CACHE_DB": str(db)}
    shared = {**env, "SOURCE_BASE": SRC, "INCIDENT_THRESHOLD": str(THRESHOLD),
              "FAILURE_COOLDOWN": "0.3", "FETCH_TIMEOUT": "2",
              "LEASE_TTL": "1", "HEARTBEAT_INTERVAL": "0.3"}
    procs: list[subprocess.Popen] = []

    def start(mod: str, port: int, extra: dict[str, str]) -> subprocess.Popen:
        log = open(DATA / "logs" / f"conc_{mod}_{port}.log", "ab")
        p = subprocess.Popen(
            [PY, "-m", "uvicorn", f"app.{mod}:app", "--host", "127.0.0.1",
             "--port", str(port)],
            cwd=str(ROOT), env={**os.environ, **extra},
            stdout=log, stderr=subprocess.STDOUT, preexec_fn=os.setsid,
        )
        procs.append(p)
        return p

    start("source", 8080, env)
    start("cache", 8081, {**shared, "INSTANCE_ID": "cache1"})
    start("cache", 8082, {**shared, "INSTANCE_ID": "cache2"})

    try:
        async with httpx.AsyncClient(timeout=45) as c:
            for _ in range(100):
                try:
                    if all((await c.get(u)).status_code == 200 for u in
                           (f"{SRC}/docs", f"{C1}/health", f"{C2}/health")):
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.2)

            async def publish(n: str) -> None:
                await c.post(f"{SRC}/admin/publish", json={"service": SERVICE, "nodes": [n]})

            async def fault(mode: str, code: int = 500) -> None:
                await c.post(f"{SRC}/admin/fault", json={"mode": mode, "code": code})

            async def events() -> dict:
                return (await c.get(f"{C1}/events/{SERVICE}")).json()

            async def burst(duration: float) -> None:
                end = time.time() + duration
                while time.time() < end:
                    await asyncio.gather(
                        c.get(f"{C1}/routes/{SERVICE}"),
                        c.get(f"{C2}/routes/{SERVICE}"),
                        return_exceptions=True,
                    )
                    await asyncio.sleep(0.05)

            async def recover() -> bool:
                """等冷却过去后两实例同时查询，直到成功返回现役路由。"""
                await asyncio.sleep(0.45)
                end = time.time() + 5
                while time.time() < end:
                    r1, r2 = await asyncio.gather(c.get(f"{C1}/routes/{SERVICE}"),
                                                  c.get(f"{C2}/routes/{SERVICE}"))
                    if r1.status_code == 200 and r2.status_code == 200:
                        return True
                    await asyncio.sleep(0.3)
                return False

            await publish("h1")
            await c.get(f"{C1}/routes/{SERVICE}")

            # ---------------------------------------------------------- E1
            await fault("error")
            await publish("h2")
            await burst(THRESHOLD + 1.0)
            ev = await events()
            ok1 = (len(ev["events"]) == 1 and ev["events"][0]["status"] == "open")
            record("E1 两实例并发持续异常只产生一条事件", ok1,
                   f"events={[(e['id'], e['status']) for e in ev['events']]}")

            await fault("error")
            await publish("h3")
            await burst(THRESHOLD + 0.4)
            ev = await events()
            ok1b = len(ev["events"]) == 1
            record("E1b 持续异常期间并发请求不重复开单", ok1b,
                   f"events={len(ev['events'])}")

            # ---------------------------------------------------------- E2
            await fault("ok")
            ok_recovered = await recover()
            ev = await events()
            ev0 = ev["events"][0]
            ok2 = (ok_recovered and len(ev["events"]) == 1
                   and ev0["status"] == "closed"
                   and ev0["ended_at"] >= ev0["started_at"]
                   and ev["open_event"] is None)
            record("E2 两实例同时恢复只结案一次且起止时间有效", ok2,
                   f"recovered={ok_recovered} status={ev0['status']} "
                   f"dur={ev0['duration_seconds']}s")

            # ---------------------------------------------------------- E3
            for round_no in (2, 3):
                await fault("error")
                await publish(f"h{round_no}0")
                await burst(THRESHOLD + 0.6)
                await fault("ok")
                ok_round = await recover()
                await asyncio.sleep(0.2)
                if not ok_round:
                    break
            ev = await events()
            ids = [e["id"] for e in ev["events"]]
            ok3 = (len(ev["events"]) == 3 and len(set(ids)) == 3
                   and all(e["status"] == "closed" for e in ev["events"])
                   and all(e["started_at"] <= e["ended_at"] for e in ev["events"]))
            starts = sorted(e["started_at"] for e in ev["events"])
            ok3 = ok3 and all(starts[i] < starts[i + 1] for i in range(len(starts) - 1))
            record("E3 交替异常/恢复多轮：每轮一条事件且时间不重叠", ok3,
                   f"events={[(e['id'], e['status']) for e in ev['events']]}")
    finally:
        for p in procs:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                pass

    failed = [r for r in results if r[0] == "FAIL"]
    print("\n========== 事件并发自测汇总 ==========")
    for st, name, detail in results:
        print(f"  [{st}] {name} {detail}")
    print(f"\n通过 {len(results) - len(failed)}/{len(results)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
