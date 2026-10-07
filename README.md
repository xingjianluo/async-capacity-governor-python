# Async Capacity Governor（请求容量治理与热点观测）

面向 ASGI/FastAPI 服务的本地可复现请求容量治理库，解决三类问题：

1. **突发并发**：按「接口 × 调用方」维度做并发准入与有界排队，容量到顶时
   立即拒绝或返回降级值，而不是无限堆积；
2. **执行阻塞 / 客户端取消**：排队超时、主动取消、并发冲突有可区分的响应；
   客户端断开不会杀死共享中的后端执行，也不会造成计数漂移；
3. **热点只读请求**：相同 key 的并发请求合并为一次后端调用（singleflight），
   结果按 TTL 短期复用；失败 / 取消 / 过期均不污染后续请求。

## 容量模型

每个 `(endpoint, caller)` 拥有一个独立的准入闸门（`AdmissionGate`）：

```
 到达 ──> 并发 < max_concurrency 且队列空？
           │是                         │否
           ▼                           ▼
      立即放行              队列长度 >= max_queue？
                                     │是             │否
                                     ▼               ▼
                              立即拒绝 429      进入 FIFO 队列
                                                ├─ 等待 > queue_timeout -> 503
                                                ├─ 客户端断开            -> 499
                                                └─ 队首放行，进入执行
```

- **严格 FIFO**：容量热更新上调时按入队顺序补放，不允许后来者插队；
- **下调不抢占**：把 `max_concurrency` 调小不会取消在途请求，闸门自然收敛；
- **策略快照**：每个请求在准入时刻取一份不可变 `Policy`，全流程按旧策略走完，
  配置更新只影响之后到达的请求（不会让在途请求“语义突变”）。

### 与合并 / 缓存的协同

```
准入（占并发/排队槽位） ──> coalescer
                             ├─ key 有在途执行：作为 follower 等待（不重复执行）
                             ├─ key 有 TTL 内结果：直接复用（cache hit）
                             └─ 否则成为 leader：真正调用后端 factory 一次
```

- follower / cache 请求**同样占用准入槽位**，因此准入上限永远是真实硬上限；
- leader 的执行被 `asyncio.shield` 保护：leader 或 follower 的客户端取消
  只让自己离开，leader 执行继续，其他 follower 仍能拿到结果；
- 后端异常会广播给所有等待者，flight 立即清理，**不缓存错误**；
- TTL 由 leader 的策略固定，在途/缓存中的响应不会被中途的配置切换改变。

### 主动作废 / 强制刷新

上游数据变更时，可主动作废某 key 的只读结果，强制后续请求重新计算：

```bash
curl -sX POST localhost:8000/admin/invalidate -H 'content-type: application/json' -d '{
  "endpoint": "items", "caller": "*", "coalesce_key": "items:k1"
}'
# -> {"status":"ok","applied":true,"coalesce_key":"items:k1"}
```

语义（与合并、缓存的兼容范围）：

- **在途合并执行中作废**：flight 立即从注册表摘除并打作废标记。后端执行
  **不被取消**（跑完即丢弃，不进缓存、不广播旧值），所有等待者（含 leader）
  收到 `409 result_invalidated` —— 与 `429 capacity_rejected`、
  `503 queue_timeout`、`499 client_canceled` 明确区分；
- **已缓存未过期作废**：直接摘除，下一个请求成为新 leader 重新执行，
  绝不会读到作废前的旧值；
- **作废后到达的请求**（包括作废时还在准入队列里、尚未进入合并层的请求）
  视为新请求：成为新 flight 的 leader 重新执行，拿到最新结果；
- **幂等无副作用**：重复作废、作废不存在的 key 都安全返回 `applied=false`；
  作废本身不触发任何后端执行、不泄漏槽位、不影响其它 key 的在途请求；
- **不触发降级**：作废不是容量/故障信号，配置了 `fallback_value` 也不会
  返回兜底值（兜底值同样是旧数据）。

指标：`invalidated`（请求侧收到作废结果的次数）与
`invalidations`（运维侧作废实际生效次数）分开计数，均出现在
`/admin/metrics` 的各维度中。

## 降级语义

`Policy.fallback_value != null` 即开启降级。降级在两个时机触发，且响应可区分：

| 触发点 | 未配置降级 | 配置降级 |
|---|---|---|
| 并发+队列满（capacity_full） | `429 capacity_rejected` | `200 degraded`，`degrade_reason=capacity_full` |
| 排队超时（queue_timeout） | `503 queue_timeout` | `200 degraded`，`degrade_reason=queue_timeout` |
| 后端执行异常 | `500 execution_failed` | `200 degraded`，`degrade_reason=execution_failed` |
| 客户端取消 | `499 client_canceled` | 不降级（连接已断） |

成功响应内部再细分：`success`（实际执行）、`coalesced`（合入在途）、
`cache`（TTL 复用）。每个响应都带耗时分解：

```json
{
  "request_id": "req-0",
  "status": "success",
  "data": {"key": "x", "computed": true},
  "timing": {"queue_wait_ms": 1.2, "execute_ms": 101.4, "total_ms": 103.0},
  "executed": true
}
```

拒绝响应：

```json
{"request_id": "...", "status": "rejected",
 "error": "capacity_rejected", "reason": "capacity_full",
 "timing": {"queue_wait_ms": 0.01, "total_ms": 0.01}}
```

## 配置项

| 字段 | 含义 | 默认 |
|---|---|---|
| `max_concurrency` | 单 `(接口,调用方)` 实际并发上限（>=1） | 8 |
| `max_queue` | 占满后允许排队数；0 = 立即拒绝 | 16 |
| `queue_timeout` | 排队最长秒数 | 1.0 |
| `coalesce_window` | >0 开启相同只读请求合并 | 0（关） |
| `result_ttl` | 成功结果复用秒数；0=不复用 | 0 |
| `fallback_value` | 非 null 时容量/执行异常降级为该值 | null |

匹配优先级：`(endpoint, 精确 caller)` > `(endpoint, "*")` > `default`。

### 运行时切换

`POST /admin/config` 原子替换整个快照并热更新已有闸门：

```bash
curl -sX POST localhost:8000/admin/config -H 'content-type: application/json' -d '{
  "default": {"max_concurrency": 32, "max_queue": 32, "queue_timeout": 5.0},
  "policies": [{
    "endpoint": "items", "caller": "*",
    "max_concurrency": 32, "max_queue": 32, "queue_timeout": 5.0,
    "coalesce_window": 1.0, "result_ttl": 0.5
  }]
}'
```

更新不会重复执行在途请求（leader 持有旧 flight），也不会使计数器漂移：
指标按「事件发生时的事实」累计，闸门按「新容量」放行。

## 观测：`GET /admin/metrics`

按 `total` / `endpoint:<名>` / `caller:<名>` 三维度聚合：

- `queue_wait_seconds` / `execute_seconds` / `total_seconds`：排队、执行、总耗时；
- `capacity_rejects`（429 数）与 `queue_timeouts`（503 数），
  另有 `reject_reasons` 分布；
- `executions`：**实际后端执行次数**（leader 计数，合并者/缓存不计）；
- `coalesced` / `cache_hit`：被合并、命中缓存的请求数；
- `gates`：各闸门实时 `active/waiting`；`flights`：在途/缓存 flight 数。

## 接口

| 路径 | 说明 |
|---|---|
| `GET /items/{key}?caller=&delay=` | 热点只读接口（默认开启合并 + 1s TTL，cap=2/queue=2/timeout=0.3s） |
| `GET /report?caller=&delay=&fail=` | 降级演示（cap=1/queue=1/timeout=0.2s，fallback 为 `{"degraded":true,"items":[]}`） |
| `GET /admin/metrics` | 指标与实时闸门状态 |
| `POST /admin/config` | 运行时原子替换配置 |
| `POST /admin/invalidate` | 主动作废只读结果（在途合并执行与未过期缓存），幂等 |
| `GET /health` | 健康检查 |

## 运行与验证

```bash
uv sync
uv run python main.py            # 启动服务（127.0.0.1:8000）

uv run pytest                    # 37 个测试
uv run pytest -s                 # 查看每个请求的 req= / decision= / queued= / execute= 日志
```

本地验证作废 / 强制刷新：

```bash
# 终端 1：发起一个慢请求（合并窗口内在途）
curl -s "localhost:8000/items/k1?caller=ops&delay=2" &
# 终端 2：立即作废该 key 的在途结果
curl -sX POST localhost:8000/admin/invalidate -H 'content-type: application/json' \
  -d '{"endpoint":"items","caller":"*","coalesce_key":"items:k1"}'
# 终端 1 的请求收到 409 result_invalidated；再次 GET 会重新执行拿到最新结果
```

测试覆盖（均带请求标识、准入判定与耗时分解日志）：

1. **突发并发**：8 并发打到 cap=2/queue=2 的接口 -> 1 成功 + 合并/缓存 +
   `429 capacity_rejected` + `503 queue_timeout` 同时出现，压力解除后恢复；
2. **排队超时**：等待超过 `queue_timeout` 返回可区分的 503；
3. **客户端取消**：队列中/执行中取消映射为 `ClientCanceled(499)`，
   不泄漏槽位、不杀共享 leader、不影响后续请求（含原生 ASGI 断连测试）；
4. **相同请求合并**：N 个并发只读请求仅 1 次后端执行；TTL 内缓存、
   过期重算；follower/leader 取消隔离；失败不污染后续；
5. **配置切换**：在途请求按旧策略跑完，新请求按新策略立即拒绝/放宽；
   成功 / 拒绝 / 降级三类结果在状态码与 body 中均可区分；
6. **主动作废 / 强制刷新**：合并执行中作废（所有等待者收 409、旧值丢弃、
   不重复执行）、缓存未过期作废（强制重算）、作废不存在/重复作废（安全幂等）、
   作废与容量打满同时发生（拒绝语义不走样、压力过后恢复、槽位计数归零）、
   其它 key 在途请求不受拖累。

## 代码结构

```
governor/
  config.py     # Policy/Snapshot/ConfigStore（不可变快照、原子替换）
  admission.py  # AdmissionGate（FIFO 有界队列）+ AdmissionLimiter（按 key 管理）
  coalescer.py  # singleflight + TTL 复用（leader/follower 生命周期、主动作废）
  metrics.py    # 三维度指标聚合
  governor.py   # 编排：准入 -> 合并 -> 执行 -> 降级 -> 指标/日志
  app.py        # FastAPI 装配、HTTP 响应映射、管理接口
  errors.py     # 可区分异常：QueueRejected/QueueTimeout/ClientCanceled/ResultInvalidated
tests/          # 准入 / 合并 / 编排 / 作废 / HTTP 端到端（含原生 ASGI 取消）
```
