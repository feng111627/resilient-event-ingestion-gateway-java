# Resilient Event Ingestion Gateway (Java)

本地可运行的批量事件接入网关。以本地线程池 + 有界队列模拟上游的并发与重复投递，
保证在高并发、重复提交和失败重试场景下接入结果**稳定且可解释**。

## 功能概览

- **批量接收 + 异步处理**：`POST /api/events/batch` 受理后立即返回，后台工作线程异步处理。
- **幂等去重**：以 `dedupKey` 为去重键，同批/跨批/并发重复提交只受理一次，不重复计数、不重复处理。
- **容量保护**：处理队列有界，满则对该条明确返回 `REJECTED`，绝不无限堆积；限流触发时整请求快速返回 429。
- **失败重试与隔离**：可重试异常按指数退避重投，重试耗尽进入死信；不可重试异常直接死信；
  单条失败被捕获隔离，不拖住后续正常事件。
- **状态查询与指标**：按去重键 / 处理状态 / 失败原因查询，指标端点暴露处理耗时、队列深度、拒绝次数等。
- **失败事件重放**：按去重键单条重放、按失败原因批量重放；重放使用独立的重试次数配置；
  并发/重复重放幂等，容量不足明确拒绝且不改动原失败记录；重放轨迹（次数、最终结果）可按去重键查询。

## 接入协议

### 批量接入

```
POST /api/events/batch
Content-Type: application/json

[
  { "dedupKey": "order-001", "payload": "...", "failMode": "none" },
  { "dedupKey": "order-002", "payload": "...", "failMode": "flaky" }
]
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `dedupKey` | 是 | 去重键，全局唯一；重复提交幂等 |
| `payload` | 否 | 业务负载 |
| `failMode` | 否 | 本地模拟下游行为：`none`(默认成功) / `flaky`(前2次可重试失败后成功) / `retryable`(恒可重试失败) / `fatal`(恒不可重试失败) |

响应 `202 Accepted`（全部被拒绝时 `429`）：

```json
{
  "batchId": "...", "total": 2, "accepted": 1, "duplicates": 1, "rejected": 0, "invalid": 0,
  "items": [
    { "dedupKey": "order-001", "result": "ACCEPTED",  "message": "..." },
    { "dedupKey": "order-002", "result": "DUPLICATE", "message": "..." }
  ]
}
```

逐条结果：`ACCEPTED`(新受理) / `DUPLICATE`(幂等去重) / `REJECTED`(队列满) / `INVALID`(缺 dedupKey)。

### 状态查询与指标

| 端点 | 说明 |
|---|---|
| `GET /api/events/{dedupKey}` | 按去重键查单条状态，未知键 404 |
| `GET /api/events?status=SUCCESS` | 按状态过滤（RECEIVED/RETRYING/SUCCESS/DEAD_LETTER/REPLAYING），不传返回全部 |
| `GET /api/failures?reason=MAX_RETRIES_EXCEEDED` | 查死信记录，可按原因过滤（NON_RETRYABLE / MAX_RETRIES_EXCEEDED） |
| `GET /api/metrics` | 指标：接收/受理/去重/拒绝/成功/死信/重试次数、重放受理/拒绝/成功/失败次数、队列深度与容量、活跃 worker、平均/最大处理耗时、按原因死信分布 |

### 失败事件重放

下游恢复后，可把躺在失败记录（死信）里的事件重新拉起来走一遍处理流程。

**哪些事件可以重放**：只有处于 `DEAD_LETTER` 状态的事件可以重放。
已成功（含此前重放成功）的事件会被识别为 `ALREADY_SUCCEEDED` 幂等跳过，不重复处理也不重复计数；
`RECEIVED`/`RETRYING` 中的事件返回 `NOT_REPLAYABLE`；已有重放在途时返回 `REPLAY_IN_PROGRESS`。

#### 按去重键单条重放

```
POST /api/replays/{dedupKey}
```

响应：`202`（REPLAY_ACCEPTED / ALREADY_SUCCEEDED）、`404`（键不存在）、
`409`（已有重放在途）、`422`（状态不可重放）、`429`（容量不足拒绝或重放限流）。

```json
{ "dedupKey": "a3", "result": "REPLAY_ACCEPTED", "message": "accepted for async replay",
  "status": "REPLAYING", "replayCount": 1, "lastReplayResult": "NONE" }
```

#### 按失败原因批量重放

```
POST /api/replays/batch
Content-Type: application/json

{ "reason": "MAX_RETRIES_EXCEEDED" }
```

`reason` 可省略（重放全部死信）。逐条独立受理，互不影响：能放的放、不能放的给出各自原因，
单条被拒不影响其他条。命中数超过 `gateway.max-batch-size` 时截断（`truncated=true`），
可再次发起继续。全部因容量被拒时整体返回 `429`。

```json
{ "reason": "MAX_RETRIES_EXCEEDED", "candidates": 2, "processed": 2,
  "replayAccepted": 2, "alreadySucceeded": 0, "inProgress": 0, "rejected": 0,
  "notReplayable": 0, "truncated": false,
  "items": [ { "dedupKey": "a2", "result": "REPLAY_ACCEPTED", "message": "..." } ] }
```

#### 重放轨迹查询

```
GET /api/replays/{dedupKey}
```

```json
{ "dedupKey": "a3", "status": "SUCCESS", "replayInFlight": false,
  "replayCount": 1, "lastReplayResult": "SUCCESS",
  "lastReplayError": null, "lastReplayAt": "2026-10-08T07:00:00Z" }
```

`GET /api/events/{dedupKey}` 返回的完整记录中也包含同样的重放字段。
`lastReplayResult` 取值：`NONE`（未重放过）/ `SUCCESS` / `FAILED`。

#### 重放规则与首次接入的差异

| 维度 | 首次接入 | 重放 |
|---|---|---|
| 重试次数 | `gateway.max-retries`（默认 3） | `gateway.replay-max-retries`（默认 2），独立配置 |
| 尝试计数 | `attempts`（累计） | 重放轮内单独计 `replayAttempts`，据此判定重放重试上限 |
| 入口限流 | `gateway.rate-limit-per-second` | `gateway.replay-rate-limit-per-second`（默认 50），独立令牌桶 |
| 最终失败 | 进入死信，记 `failureReason` | 回到死信，`failureReason` 保留首次失败原因（仍可按原原因批量重放），结果记入 `lastReplayResult=FAILED` |

重放的可靠性保证：

- **并发安全**：同一事件同一时刻只允许一个重放在途（记录级守卫 + 状态机互斥），并发重放只受理一次；
- **幂等**：已重放成功的事件再次提交重放返回 `ALREADY_SUCCEEDED`，不重复处理、不重复计数；
- **容量一致**：重放复用同一个有界处理队列，队列满时明确 `REJECTED` 并回滚守卫与计数，
  原失败记录（状态、失败原因、错误信息）完全不变，容量恢复后可再次重放；
- **无中间态**：重放只有 `REPLAYING -> SUCCESS` 或 `REPLAYING -> DEAD_LETTER` 两种终态，
  不会停在既没成功、也无法再重放的状态；重投时队列满会稍后再次重投，不丢数据；
- **延迟有界**：重放入口独立令牌桶限流 + 非阻塞入队，压力上来时快速拒绝而不是排队等待。

## 去重与重试规则

- **去重**：`EventStore` 以 `dedupKey` 为主键 `putIfAbsent`，并发下同键只受理一次；
  重复提交返回 `DUPLICATE`，不入队、不计数、不处理。
- **重试**：处理抛 `RetryableProcessingException` 时，按 `retryBackoffMillis * 2^(attempt-1)` 指数退避重投，
  最多重试 `maxRetries` 次；重投时若队列满会稍后再次重投，保证不丢数据。
- **死信**：重试耗尽（`MAX_RETRIES_EXCEEDED`）或不可重试异常（`NON_RETRYABLE`）进入死信记录，
  可通过 `/api/failures` 查询；死信事件不影响后续事件处理。

## 容量与限流参数

`application.properties` 中 `gateway.*` 配置：

| 参数 | 默认 | 说明 |
|---|---|---|
| `gateway.queue-capacity` | 256 | 待处理队列容量，满则逐条 REJECTED |
| `gateway.worker-threads` | 4 | 异步处理工作线程数 |
| `gateway.max-retries` | 3 | 最大重试次数（不含首次） |
| `gateway.retry-backoff-millis` | 50 | 重试基础退避（指数增长） |
| `gateway.rate-limit-per-second` | 200 | 令牌桶限流：每秒接入请求数，超限整请求 429 快速失败 |
| `gateway.max-batch-size` | 500 | 单批最大事件数，超限 400；同时作为批量重放的单批上限 |
| `gateway.replay-max-retries` | 2 | 重放专属最大重试次数（不含重放首次），与首次接入的 `max-retries` 相互独立 |
| `gateway.replay-rate-limit-per-second` | 50 | 重放入口限流：每秒重放请求数，超限 429 快速失败，保证压力下重放延迟有界 |

队列有界 + 非阻塞入队 + 令牌桶快速拒绝，保证压力升高时响应延迟有界，不无上限增长。

## 本地验证

```bash
# 运行全部测试（27 个用例，日志含各场景覆盖与通过情况）
mvn test

# 启动服务
mvn spring-boot:run

# 提交一批事件（含重复键与失败模拟）
curl -s -X POST localhost:8080/api/events/batch -H 'Content-Type: application/json' -d '[
  {"dedupKey":"a1","payload":"hello","failMode":"none"},
  {"dedupKey":"a1","payload":"hello","failMode":"none"},
  {"dedupKey":"a2","payload":"flaky","failMode":"flaky"},
  {"dedupKey":"a3","payload":"fatal","failMode":"fatal"}
]'

# 查询处理状态 / 死信 / 指标
curl -s localhost:8080/api/events/a2
curl -s 'localhost:8080/api/failures?reason=NON_RETRYABLE'
curl -s localhost:8080/api/metrics

# 重放：a3 是 fatal 死信，单条重放后查询重放轨迹
curl -s -X POST localhost:8080/api/replays/a3
curl -s localhost:8080/api/replays/a3

# 按失败原因批量重放（例如下游恢复后重放所有重试耗尽的记录）
curl -s -X POST localhost:8080/api/replays/batch \
  -H 'Content-Type: application/json' \
  -d '{"reason":"MAX_RETRIES_EXCEEDED"}'
```

## 测试覆盖

| 测试类 | 覆盖场景 |
|---|---|
| `IngestServiceTest` | 同批/跨批重复投递幂等、非法参数部分失败、队列满拒绝、限流、批量大小校验 |
| `EventProcessorTest` | flaky 重试成功、重试耗尽死信、不可重试直接死信、失败隔离（20 正常 + 2 失败混合）、有界队列非阻塞拒绝 |
| `GatewayIntegrationTest` | 8 线程并发接收 200 条无丢失、同批三次重投仅处理一次、混合批次失败隔离与死信按原因查询、指标端点、按状态查询与 404 |
| `ReplayServiceTest` | 重放成功与轨迹记录、并发重放只受理一次、重放成功后再重放幂等跳过、容量不足拒绝且原状态不变/恢复后可再重放、重放失败回死信可再重放、按原因批量重放逐条独立、未知键/处理中事件校验、重放入口限流 |
| `ReplayIntegrationTest` | HTTP 单条重放端到端（受理->成功->轨迹查询->重复重放幂等）、HTTP 按原因批量重放与未知键 404 |

测试日志中以 `=== 场景：... ===` 与 `通过：...` 标明每个用例覆盖的业务与结果。
