"""交错、失联与恢复下不可用事件（incident）的端到端自测。

不依赖 Docker，直接驱动 source + 两个共享 SQLite 的 cache 进程。
通过缩短阈值/超时（默认 10s/6s）加速验证，判定逻辑与生产参数一致。

覆盖：
I1 短暂异常（未达阈值即恢复）不开启事件，且不残留 pending 轮次
I2 持续异常达到阈值：仅一条事件；started_at=首次异常，首/最近原因保留
I3 两个实例交错产生异常：仍只有一条事件（跨实例共享同一轮次）
I3b 同一轮异常持续时只更新最近原因，不重复开单
I4 恢复：成功返回现役路由后事件结案并保留起止时间
I5 恢复前启动、恢复后才返回的迟到异常不能重新开单
I6 恢复后的新异常可开启下一轮（独立事件）
I7 查询事件接口自身完成到期判定（期间无新异常请求，仅靠查询落事件）
I8 源整体失联时的降级路由也按异常开单，恢复后成功返回现役路由并结案
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
SERVICE = "payments"
THRESHOLD = 0.8     # 测试加速阈值（生产默认 10s）
COOLDOWN = 0.4      # 失败冷却
FETCH_TIMEOUT = 2.5

results: list[tuple[str, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append(("PASS" if ok else "FAIL", name, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


class Stack:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def start(self, what: str, port: int, env: dict[str, str]) -> subprocess.Popen:
        log = open(DATA / "logs" / f"incident_{what}_{port}.log", "ab")
        p = subprocess.Popen(
            [PY, "-m", "uvicorn", f"app.{what}:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(ROOT), env={**os.environ, **env},
            stdout=log, stderr=subprocess.STDOUT, preexec_fn=os.setsid,
        )
        self.procs.append(p)
        return p

    def kill(self, p: subprocess.Popen) -> None:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass

    def stop_all(self) -> None:
        for p in self.procs:
            self.kill(p)
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                pass


async def wait_ready(client: httpx.AsyncClient, urls: list[str], timeout: float = 20) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for u in urls:
            try:
                if (await client.get(u)).status_code != 200:
                    ok = False
            except Exception:
                ok = False
        if ok:
            return
        await asyncio.sleep(0.2)
    raise RuntimeError(f"not ready: {urls}")


async def events(client: httpx.AsyncClient, service: str = SERVICE) -> dict:
    r = await client.get(f"{C1}/events/{service}")
    r.raise_for_status()
    return r.json()


async def main() -> int:
    DATA.mkdir(exist_ok=True)
    (DATA / "logs").mkdir(parents=True, exist_ok=True)
    db = DATA / "incident_cache.db"
    sdb = DATA / "incident_source.db"
    for f in (db, sdb):
        for s in ("", "-wal", "-shm"):
            Path(str(f) + s).unlink(missing_ok=True)

    stack = Stack()
    base = {"SOURCE_DB": str(sdb), "CACHE_DB": str(db)}
    cache_env = {
        **base, "SOURCE_BASE": SRC,
        "INCIDENT_THRESHOLD": str(THRESHOLD),
        "FAILURE_COOLDOWN": str(COOLDOWN),
        "FETCH_TIMEOUT": str(FETCH_TIMEOUT),
        "LEASE_TTL": "1",
        "HEARTBEAT_INTERVAL": "0.3",
    }
    src = stack.start("source", 8080, base)
    stack.start("cache", 8081, {**cache_env, "INSTANCE_ID": "cache1"})
    stack.start("cache", 8082, {**cache_env, "INSTANCE_ID": "cache2"})
    rev_counter = 0

    async def publish(c: httpx.AsyncClient) -> int:
        nonlocal rev_counter
        rev_counter += 1
        r = await c.post(f"{SRC}/admin/publish",
                         json={"service": SERVICE, "nodes": [f"10.1.0.{rev_counter}:80"]})
        r.raise_for_status()
        return r.json()["revision"]

    try:
        async with httpx.AsyncClient(timeout=45) as c:
            await wait_ready(c, [f"{SRC}/docs", f"{C1}/health", f"{C2}/health"])
            await c.post(f"{C1}/debug/reset")

            async def fault(mode: str, code: int = 500) -> None:
                await c.post(f"{SRC}/admin/fault", json={"mode": mode, "code": code})

            async def settle() -> None:
                """等待失败冷却过去，避免快速失败干扰恢复判定。"""
                await asyncio.sleep(COOLDOWN + 0.15)

            # ---------------------------------------------------------- I1
            await fault("error", 500)
            await publish(c)                     # rev1，缓存未命中 -> 回源失败
            r1 = await c.get(f"{C1}/routes/{SERVICE}")
            await fault("ok")
            await settle()
            rr = await c.get(f"{C2}/routes/{SERVICE}")
            ev = await events(c)
            ok1 = (r1.status_code == 503 and rr.status_code == 200
                   and ev["events"] == [] and ev["pending_anomaly"] is None)
            record("I1 未达阈值的短暂异常不开启事件且无残留轮次", ok1,
                   f"err={r1.status_code} recover={rr.status_code} "
                   f"events={len(ev['events'])} pending={ev['pending_anomaly']}")

            # ---------------------------------------------------------- I2 / I3
            await fault("error", 500)
            await publish(c)                     # rev2 同样失败
            t_start = time.time()
            first_reason = None
            last_status = None
            while time.time() - t_start < THRESHOLD + 1.2:
                target = C1 if int((time.time() - t_start) * 10) % 2 else C2
                rr = await c.get(f"{target}/routes/{SERVICE}")
                last_status = rr.status_code
                if first_reason is None and rr.status_code == 503:
                    first_reason = rr.json()["detail"].get("reason")
                await asyncio.sleep(0.12)
            ev = await events(c)
            opened = [e for e in ev["events"] if e["status"] == "open"]
            ok2 = (last_status == 503 and len(opened) == 1
                   and opened[0]["first_reason"] == first_reason
                   and opened[0]["started_at"] <= opened[0]["detected_at"])
            ok3 = len(ev["events"]) == 1
            record("I2 持续异常达阈值后开启一条事件（首次异常与最近原因）", ok2,
                   f"open={len(opened)} first={opened[0]['first_reason'][:50] if opened else None}")
            record("I3 两实例交错异常共享同一轮次，仅一条事件", ok3,
                   f"total_events={len(ev['events'])}")

            incident_id = opened[0]["id"] if opened else None
            started_at = opened[0]["started_at"] if opened else None

            # ---------------------------------------------------------- I3b
            await fault("error", 502)
            await settle()                       # 冷却后再回源，记录新原因
            await c.get(f"{C2}/routes/{SERVICE}")
            ev = await events(c)
            still_one = [e for e in ev["events"] if e["status"] == "open"]
            ok_merge = (len(still_one) == 1 and still_one[0]["id"] == incident_id
                        and "502" in still_one[0]["last_reason"])
            record("I3b 同一轮异常持续时只更新最近原因，不重复开单", ok_merge,
                   f"open={len(still_one)} "
                   f"last={still_one[0]['last_reason'][:70] if still_one else None}")

            # ---------------------------------------------------------- I4
            await fault("ok")
            await settle()
            rr = await c.get(f"{C1}/routes/{SERVICE}")
            ev = await events(c)
            closed = [e for e in ev["events"] if e["id"] == incident_id]
            ok4 = (rr.status_code == 200 and rr.json().get("degraded") is not True
                   and len(closed) == 1 and closed[0]["status"] == "closed"
                   and closed[0]["ended_at"] is not None
                   and closed[0]["ended_at"] >= closed[0]["started_at"]
                   and ev["open_event"] is None)
            record("I4 成功返回现役路由后结案并保留起止时间", ok4,
                   f"status={closed[0]['status'] if closed else None} "
                   f"started={started_at and round(started_at, 2)} "
                   f"ended={closed[0]['ended_at'] and round(closed[0]['ended_at'], 2)}")

            # ---------------------------------------------------------- I5
            # 恢复前启动、恢复后才返回的迟到异常不能重新开单。
            await fault("hang")
            await publish(c)                     # rev3：C1 回源挂起
            late_task = asyncio.create_task(c.get(f"{C1}/routes/{SERVICE}"))
            await asyncio.sleep(0.4)             # 确保首轮异常请求已发出并挂起
            await fault("ok")
            await publish(c)                     # rev4：新修订，C2 可立即回源成功
            rr = await c.get(f"{C2}/routes/{SERVICE}")
            late_resp = await late_task          # 迟到请求在硬超时后返回 503
            await asyncio.sleep(THRESHOLD + 0.6)
            ev = await events(c)
            ok5 = (late_resp.status_code == 503 and rr.status_code == 200
                   and all(e["status"] == "closed" for e in ev["events"])
                   and ev["pending_anomaly"] is None)
            record("I5 恢复前启动、恢复后返回的迟到异常不能重开事件", ok5,
                   f"late={late_resp.status_code} recover={rr.status_code} "
                   f"events={[(e['id'], e['status']) for e in ev['events']]}")

            # ---------------------------------------------------------- I6
            await fault("error", 500)
            await publish(c)
            t0 = time.time()
            while time.time() - t0 < THRESHOLD + 1.0:
                target = C1 if int((time.time() - t0) * 8) % 2 else C2
                await c.get(f"{target}/routes/{SERVICE}")
                await asyncio.sleep(0.12)
            ev = await events(c)
            opened2 = [e for e in ev["events"] if e["status"] == "open"]
            ok6 = (len(opened2) == 1 and opened2[0]["id"] != incident_id
                   and len(ev["events"]) == 2)
            record("I6 恢复后的新异常开启下一轮（独立事件）", ok6,
                   f"events={[(e['id'], e['status']) for e in ev['events']]}")

            # 恢复并结案
            await fault("ok")
            await settle()
            await c.get(f"{C1}/routes/{SERVICE}")
            ev = await events(c)
            assert all(e["status"] == "closed" for e in ev["events"]), ev

            # ---------------------------------------------------------- I7
            # 单次异常后不再发路由请求，仅靠 /events 查询完成到期判定。
            await fault("error", 500)
            await publish(c)
            r7 = await c.get(f"{C1}/routes/{SERVICE}")
            ev_before = await events(c)          # 此时不足阈值 -> pending
            await asyncio.sleep(THRESHOLD + 0.4)
            ev_after = (await c.get(f"{C2}/events/{SERVICE}")).json()
            ok7 = (r7.status_code == 503
                   and ev_before["pending_anomaly"] is not None
                   and ev_before["open_event"] is None
                   and ev_after["open_event"] is not None
                   and len(ev_after["events"]) == 3)
            record("I7 仅查询事件（无新路由请求）也能完成到期判定", ok7,
                   f"pending_before={ev_before['pending_anomaly'] is not None} "
                   f"open_after={ev_after['open_event'] is not None} "
                   f"events={len(ev_after['events'])}")

            # 恢复，为 I8 增加一条已结案事件
            await fault("ok")
            await settle()
            await c.get(f"{C1}/routes/{SERVICE}")
            ev = await events(c)
            assert all(e["status"] == "closed" for e in ev["events"]), ev

            # ---------------------------------------------------------- I8
            stack.kill(src)
            await asyncio.sleep(0.4)
            t0 = time.time()
            degraded_seen = False
            while time.time() - t0 < THRESHOLD + 1.2:
                target = C2 if int((time.time() - t0) * 8) % 2 else C1
                r = await c.get(f"{target}/routes/{SERVICE}")
                if r.status_code == 200 and r.json().get("degraded"):
                    degraded_seen = True
                await asyncio.sleep(0.12)
            ev = await events(c)
            open8 = [e for e in ev["events"] if e["status"] == "open"]
            ok8a = (degraded_seen and len(open8) == 1 and len(ev["events"]) == 4
                    and "degraded" in open8[0]["last_reason"])
            record("I8 源失联期间降级路由达到阈值后开启事件", ok8a,
                   f"degraded_seen={degraded_seen} events={len(ev['events'])} "
                   f"last={open8[0]['last_reason'][:50] if open8 else None}")

            src2 = stack.start("source", 8080, base)
            await wait_ready(c, [f"{SRC}/docs"])
            r = await c.get(f"{C1}/routes/{SERVICE}")
            ev = await events(c)
            ok8b = (r.status_code == 200 and r.json().get("degraded") is not True
                    and all(e["status"] == "closed" for e in ev["events"])
                    and ev["open_event"] is None)
            record("I8b 源恢复后成功返回现役路由，失联事件结案", ok8b,
                   f"status={r.status_code} events={[(e['id'], e['status']) for e in ev['events']]}")

            # ---------------------------------------------------------- F
            for name, url in [("cache1 /docs", f"{C1}/docs"), ("cache2 /docs", f"{C2}/docs"),
                              ("cache1 /events", f"{C1}/events"),
                              ("cache2 /events/{service}", f"{C2}/events/{SERVICE}"),
                              ("cache /events?status=closed", f"{C1}/events?status=closed")]:
                rd = await c.get(url)
                record(f"F {name} 可访问", rd.status_code == 200, f"status={rd.status_code}")

            stack.kill(src2)
    finally:
        stack.stop_all()

    failed = [r for r in results if r[0] == "FAIL"]
    print("\n============ 不可用事件自测汇总 ============")
    for st, name, detail in results:
        print(f"  [{st}] {name} {detail}")
    print(f"\n通过 {len(results) - len(failed)}/{len(results)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
