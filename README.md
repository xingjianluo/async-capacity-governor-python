# async-capacity-governor

为 FastAPI 服务提供本地可复现的**请求容量治理**与**热点观测**：突发并发、执行阻塞、
客户端取消时行为可预期，成功 / 拒绝 / 降级 / 取消结果可区分。

## 容量模型

每个 `(route, caller)` 维度对应一条独立的准入通道（`AdmissionLane`）：

```
请求 ──> 有空闲执行槽？ ──是──> 立即执行
           │否
           ├─> 队列已满(waiting >= max_queued)？ ──是──> 拒绝 queue_full (429)
           │否
           ├─> 排队等待，超过 queue_timeout_s？ ──是──> 拒绝 queue_timeout (503)
           │        客户端取消？ ──是──> 取消 cancelled (499)
           │否
           └─> 被唤醒获得槽位 ──> 执行
```

- **有界并发**：`max_concurrent` 限制同一维度同时执行的请求数。
- **有界排队**：`max_queued` 限制排队长度，超出立即拒绝，绝不无限等待、无内存膨胀。
- **排队超时**：`queue_timeout_s` 到点未获槽位即拒绝，与队列满是可区分的两种拒绝原因。
- **取消安全**：排队或执行中被取消时，队列位置与计数严格归还（`waiting`/`running`
  不会漂移），压力解除后正常请求立即恢复。

## 相同请求合并（single-flight）

只读请求携带 `coalesce_key` 时：

- 并发相同 key 共享**一次**实际执行；结果未产生前任何调用方都不会提前返回。
- 成功结果按 `coalesce_ttl_s` 短时缓存，TTL 内命中直接复用（`shared=true`）。
- 执行**失败或被取消时不写缓存**，所有等待者看到同样的失败，后续请求重新执行，
  不污染后续请求。
- `POST /admin/invalidate/{key}` 会使在途合并执行失效：等待者收到
  `concurrency_conflict`（409），过期结果不会落入缓存，后续请求按新规则重试。

## 降级语义

调用方提供 `degraded` 回退时，准入拒绝（queue_full / queue_timeout）不返回错误，
而是执行回退逻辑并返回 **HTTP 200 + `status="degraded"`**（body 中带原始
`reason`），与 `success` 明确可区分。未提供回退时返回对应错误状态码。

| 结果 | HTTP | body.status | body.reason |
|---|---|---|---|
| 成功 | 200 | `success` | - |
| 降级 | 200 | `degraded` | `queue_full` / `queue_timeout` |
| 队列满 | 429 | `rejected` | `queue_full` |
| 排队超时 | 503 | `rejected` | `queue_timeout` |
| 合并冲突 | 409 | `rejected` | `concurrency_conflict` |
| 客户端取消 | 499 | `cancelled` | `cancelled` |

## 动态配置

`PUT /config` 整体替换配置（版本号单调递增）：

```json
{
  "default_limit": {"max_concurrent": 4, "max_queued": 8,
                    "queue_timeout_s": 2.0,
                    "coalesce_enabled": true, "coalesce_ttl_s": 0.5},
  "route_limits": {"/items/{item_id}": {"max_concurrent": 2}}
}
```

- 配置对象不可变，更新是**原子引用替换**。
- 每个请求在准入时**快照**当时的配置（响应中带 `config_version`），在途请求
  不受新配置影响：不会重复执行、不会计数漂移、不会发生语义突变。
- 新请求立即使用新配置语义。

## 观测

`GET /metrics` 返回：

- **计数器**：`admitted`、`executed`（实际执行量）、`coalesced.shared`（合并复用量）、
  `rejected.queue_full` / `rejected.queue_timeout` / `rejected.concurrency_conflict`、
  `degraded`、`cancelled`、`errors`。
- **延迟分解**：`queue_wait`（排队延迟）与 `execute`（处理延迟）的
  count / total / avg。
- **lanes**：每个 `(route, caller)` 维度的实时 `running` / `waiting`。

运行日志按请求打印：`req=<id> dim=<route:caller> decision=<admitted|queue_full|
queue_timeout|degraded|completed|cancelled-*> queue_wait=…s execute=…s cfg=<版本>`。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/items/{item_id}?delay=&fail=&degrade=` | 受治理的只读接口（可合并、可降级） |
| GET | `/metrics` | 计数器、延迟分解、通道状态 |
| GET / PUT | `/config` | 查看 / 替换配置（PUT 返回新版本号） |
| POST | `/admin/invalidate/{key}` | 使某个合并 key 的在途执行失效 |

请求头：`X-Caller`（调用方维度，默认 `anonymous`）、`X-Request-Id`（日志关联）。

## 验证方法

```bash
uv run python -m pytest -q                 # 全部 13 个测试
uv run python -m pytest -q -o log_cli=true -o log_cli_level=INFO  # 查看请求级日志
```

覆盖场景：

- `test_governor.py`：突发并发拒绝与恢复、排队超时可区分、降级回退、
  排队中客户端取消（无计数漂移）、相同请求合并且不提前返回、失败不污染、
  配置切换时在途请求保持旧快照。
- `test_http.py`：成功/拒绝/降级 HTTP 可区分、429 与 503 区分、
  HTTP 层合并、invalidate 产生 409 冲突、客户端取消后服务恢复、
  配置切换生效与指标暴露。

本地运行服务：`uv run python main.py`（监听 127.0.0.1 随机端口）。
