# 路由查询服务：双缓存实例 + 本地配置源（FastAPI + SQLite）

值班工程师更新服务发现节点列表后，入口实例不能继续把已下线节点从缓存里吐出来。
本项目用 FastAPI + SQLite 实现带**修订一致性、跨实例并发合并、租约接管**的路由查询层。

## 架构

```
                         ┌─────────────────────────────┐
  publish / fault  ───►  │ config-source (FastAPI)     │  SQLite: releases / current_rev
  /docs 可操作           │ :8080  修订按服务单调递增     │  故障注入: ok/delay/error/hang
                         └──────────────┬──────────────┘
                                        │ HTTP（current 轻量查询始终可靠；全量拉取可被故障注入）
                     ┌──────────────────┴──────────────────┐
                     ▼                                      ▼
          ┌────────────────────┐                ┌────────────────────┐
          │ cache1  :8081      │                │ cache2  :8082      │
          │ app/cache.py       │   共享 SQLite   │ app/cache.py       │
          └─────────┬──────────┘  (WAL, 卷挂载)  └─────────┬──────────┘
                    └──────────────► cache.db ◄────────────┘
                  表: snapshots(valid/stale) / inflight(租约) / failures(冷却)
                      / incidents(不可用事件) / anomaly_state / service_health
```

- **配置源** (`app/source.py`)：`POST /admin/publish` 每发一次，该服务 `revision + 1`；
  `GET /services/{s}/current` 返回当前修订（轻量、不受故障注入影响）；
  `GET /services/{s}/revisions/{r}` 返回指定修订全量节点（慢/错/挂起只作用在这里）。
- **缓存实例** (`app/cache.py`)：两个实例共享一个 SQLite 文件，无独立缓存进程，
  协调状态全部落在共享库中（快照/租约/失败冷却/不可用事件）。

## 关键保证与实现方式

1. **新修订提交后的查询拿不到旧路由**
   每次查询先问配置源当前修订，只返回/写入该修订的快照；旧修订快照即使存在也不会被命中。

2. **同一修订的并发未命中合并（跨实例 singleflight）**
   `inflight` 表以 `(service, revision)` 为主键，`BEGIN IMMEDIATE` 事务抢租约，
   只有一个实例成为 leader 发起回源；其余实例轮询共享库，直接复用 leader 写入的快照
   （响应里 `coalesced=true`）。配置源 `/admin/metrics` 可核对实际回源次数。

3. **leader 超时/退出后其他实例接手**
   leader 每 0.7s 续租（TTL 2s）。回源有 6s 硬时限：超时即停心跳、释放租约并记录失败；
   进程被 kill 时租约自然过期。follower 发现租约过期即接管。
   等待中的 follower 在租约仍有效时不会被失败记录提前打回 503。

4. **慢回源跨过修订更新，不把旧响应写成有效缓存**
   回源返回后再次核对当前修订：若已推进，旧修订快照只写为 `status=stale`，
   本次请求返回 409 提示重试；后续查询走新修订。stale 快照保留供排障。

5. **回源失败明确不可用，旧快照可排障**
   回源失败统一 `503`，body 含 `reason` 与 `last_good`（最近一次有效快照）；
   失败有 3s 冷却（共享，两个实例都快速失败，避免雪崩），冷却过后自动重试。
   `GET /snapshots/{service}` 与 `/snapshots/{service}/revisions/{r}` 在源故障期间仍可读。
   源整体宕机时，已有有效快照的服务降级返回（`degraded=true`），无快照才 503。

6. **不可用事件（incident）：按服务记录"持续不可用"与恢复**
   两台缓存实例的 `/routes/{service}` 最终返回**降级路由**（`degraded=true`）或
   `503` 时，在共享库记录该服务的异常轮次：首次异常时间、首次原因、最近原因。
   持续达到 `INCIDENT_THRESHOLD`（默认 10s）且期间**没有任何一次成功返回已核实
   的现役路由**，才开启一条事件；同一轮异常只开一条。成功返回现役路由即结案，
   保留 `started_at`（首次异常）与 `ended_at`（恢复），可计算持续时长。

   - 异常轮次跨实例共享（`anomaly_state` 每个服务一行）：两实例交错请求也只会
     合成一条事件。
   - **查询事件时也完成到期判定**：`GET /events`、`GET /events/{service}` 会先对
     已达阈值的轮次落事件，因此即便期间不再有路由请求，持续异常仍会在查询时
     被记录。
   - **迟到异常不开单**：以请求开始时间对比最近一次成功返回现役路由的完成时间，
     恢复前启动、恢复后才返回的异常不会重新开启事件；恢复后新发起的异常则进入
     下一轮，可开启新事件。
   - **写入原子**：异常轮次与事件在同一个 `BEGIN IMMEDIATE` 事务内更新，
     失败整体回滚，不会留下"有轮次无事件"或半条事件；事件写入失败也不影响
     路由响应本身。
   - `409`（慢回源跨修订，提示重试）不属于用户可见的不可用，不参与事件判定。
   - 现有 `/routes` 响应字段与 `/snapshots` 历史快照的返回语义保持不变。

## 运行

### Docker Compose（目标部署方式）

```bash
docker compose up --build
# source  http://localhost:8080/docs
# cache1  http://localhost:8081/docs
# cache2  http://localhost:8082/docs
```
两个 cache 容器挂载同一个命名卷 `cache-shared`，SQLite 以 WAL 模式并发访问。

### 本地多进程（无 Docker 环境的等价方式）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt   # 若环境无 pip，先 bootstrap（见下）
bash scripts/run_local.sh
```

> 极简环境（无 ensurepip/pip）可先 `curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && .venv/bin/python /tmp/get-pip.py`。

## /docs 可操作的完整流程

1. source `/docs` → `POST /admin/publish` 发布 `payments` 的节点（重复发布观察 revision 递增）
2. cache `/docs` → `GET /routes/payments` 查询，观察 `revision / nodes / from_cache / served_by`
3. source → `POST /admin/fault` 设 `{"mode":"delay","delay":2}` 模拟慢响应；
   `error` 模拟 5xx；`hang` 模拟挂起；`ok` 恢复
4. cache → `GET /debug/state` 看 inflight 租约、failures 冷却、snapshots 状态
5. cache → `GET /snapshots/payments` 看全部历史快照（含 stale）
6. cache → 持续故障 10s 后 `GET /events/payments` 看不可用事件
   （`started_at / ended_at / first_reason / last_reason / duration_seconds`），
   `GET /events?status=open` 看当前未结案事件；恢复后事件自动结案。

## 自测

```bash
.venv/bin/python -I tests/test_integration.py   # 13 项：修订一致性/并发合并/接管/跨修订/故障
.venv/bin/python -I tests/test_edge.py          # 边界：源宕机降级、404、发布交错单调性
.venv/bin/python -I tests/test_incidents.py     # 不可用事件：交错/失联/迟到/恢复/到期判定
.venv/bin/python -I tests/test_incidents_concurrency.py  # 两实例并发：单事件/单次结案/多轮
.venv/bin/python -I tests/test_incidents_unit.py  # 不可用事件：事务回滚/并发幂等/迟到边界
```

## 环境变量（cache）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `CACHE_DB` | `/data/cache.db` | 共享 SQLite 路径 |
| `SOURCE_BASE` | `http://source:8080` | 配置源地址 |
| `INSTANCE_ID` | 自动生成 | 实例标识（租约 leader 字段） |
| `FETCH_TIMEOUT` | `6` | 回源硬超时秒数 |
| `LEASE_TTL` | `2` | 租约有效期秒数 |
| `HEARTBEAT_INTERVAL` | `0.7` | 续租间隔秒数 |
| `FAILURE_COOLDOWN` | `3` | 失败冷却秒数 |
| `INCIDENT_THRESHOLD` | `10` | 不可用事件判定阈值（持续异常秒数；期间无成功才开单） |
