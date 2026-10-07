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
- **失败事件重放**：下游恢复后，按去重键单条重放或按失败原因批量重放死信事件；
  重放使用独立的重试次数，并发/重复重放只处理一次，容量不足时明确拒绝且不改动原失败记录。

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
| `failMode` | 否 | 本地模拟下游行为：`none`(默认成功) / `flaky`(前2次可重试失败后成功) / `retryable`(恒可重试失败) / `fatal`(恒不可重试失败) / `recoverable`(首次接入恒失败、重放时成功，模拟下游抖动后恢复) |

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
| `GET /api/events?status=SUCCESS` | 按状态过滤（RECEIVED/RETRYING/SUCCESS/DEAD_LETTER），不传返回全部 |
| `GET /api/failures?reason=MAX_RETRIES_EXCEEDED` | 查死信记录，可按原因过滤（NON_RETRYABLE / MAX_RETRIES_EXCEEDED） |
| `GET /api/metrics` | 指标：接收/受理/去重/拒绝/成功/死信/重试次数、队列深度与容量、活跃 worker、平均/最大处理耗时、按原因死信分布 |

### 失败事件重放

下游恢复后，可把失败记录（死信）重新拉起来走一遍处理流程：

| 端点 | 说明 |
|---|---|
| `POST /api/replay/events/{dedupKey}` | 按去重键重放单条失败事件 |
| `POST /api/replay/failures?reason=MAX_RETRIES_EXCEEDED` | 按失败原因批量重放死信；不传 `reason` 则重放全部死信 |
| `GET /api/replay/events/{dedupKey}` | 查重放状态：是否重放过、重放次数、最近一次重放结果、是否重放中 |

单条重放响应：`202 ACCEPTED`(已受理重放) / `200 ALREADY_SUCCEEDED`(已成功无需重放) /
`404 NOT_FOUND` / `409 NOT_REPLAYABLE|ALREADY_REPLAYING` / `429 REJECTED`(容量不足)。
批量重放逐条独立受理，有的能放、有的不能放时互不影响，响应 `items` 给出逐条结果。

**哪些失败事件可以重放**：只有 `DEAD_LETTER` 状态（重试耗尽或不可重试失败）的事件可重放；
`RECEIVED`/`RETRYING` 处理中的事件返回 `NOT_REPLAYABLE`；已 `SUCCESS` 的事件识别为
`ALREADY_SUCCEEDED`，不重复处理、不重复计数。

**重放规则与首次接入的区别**：

- 重放使用独立的重试次数 `gateway.replay-max-retries`（默认 2），与首次接入的
  `gateway.max-retries`（默认 3）互不影响；重试退避算法相同（指数退避）。
- 重放成功 → 状态转 `SUCCESS`，`lastReplayResult=SUCCESS`；重放耗尽/不可重试 → 回到
  `DEAD_LETTER`，`lastReplayResult=FAILED`，之后仍可再次重放，不会停在中间状态。
- 同一事件的重放认领是原子操作：并发重放只有一个被受理，其余返回 `ALREADY_REPLAYING`。
- 容量不足时重放被明确拒绝（`REJECTED`/429），认领回滚，原失败记录的状态、失败原因、
  尝试次数均不被改动，容量恢复后可再次重放。
- 重放入口有独立限流 `gateway.replay-rate-limit-per-second`，超限快速返回 429，
  压力升高时重放入口响应延迟有界。

**重放结果查询**：`GET /api/replay/events/{dedupKey}` 返回
`{ replayed, replayCount, lastReplayResult, replayInProgress, status }`；
`GET /api/events/{dedupKey}` 的事件详情中也带 `replayCount` / `lastReplayResult` /
`replayInProgress` 字段；`/api/metrics` 增加 `totalReplayAccepted` / `totalReplayRejected` 计数。

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
| `gateway.max-batch-size` | 500 | 单批最大事件数，超限 400 |
| `gateway.replay-max-retries` | 2 | 重放轮次最大重试次数（不含重放首次），独立于 `max-retries` |
| `gateway.replay-rate-limit-per-second` | 100 | 重放入口令牌桶限流，超限 429 快速失败 |
| `gateway.replay-max-batch-size` | 1000 | 单次批量重放最大事件数，超限 400 |

队列有界 + 非阻塞入队 + 令牌桶快速拒绝，保证压力升高时响应延迟有界，不无上限增长。

## 本地验证

```bash
# 运行全部测试（30 个用例，日志含各场景覆盖与通过情况）
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

# 重放：先造一条"下游抖动"失败的死信，再模拟下游恢复后重放
curl -s -X POST localhost:8080/api/events/batch -H 'Content-Type: application/json' -d '[
  {"dedupKey":"r1","payload":"x","failMode":"recoverable"}
]'
sleep 1   # 等重试耗尽进入死信
curl -s 'localhost:8080/api/failures?reason=MAX_RETRIES_EXCEEDED'
curl -s -X POST localhost:8080/api/replay/events/r1          # 单条重放
curl -s -X POST 'localhost:8080/api/replay/failures?reason=MAX_RETRIES_EXCEEDED'  # 按原因批量重放
curl -s localhost:8080/api/replay/events/r1                  # 查重放次数与结果
```

## 测试覆盖

| 测试类 | 覆盖场景 |
|---|---|
| `IngestServiceTest` | 同批/跨批重复投递幂等、非法参数部分失败、队列满拒绝、限流、批量大小校验 |
| `EventProcessorTest` | flaky 重试成功、重试耗尽死信、不可重试直接死信、失败隔离（20 正常 + 2 失败混合）、有界队列非阻塞拒绝 |
| `GatewayIntegrationTest` | 8 线程并发接收 200 条无丢失、同批三次重投仅处理一次、混合批次失败隔离与死信按原因查询、指标端点、按状态查询与 404 |
| `ReplayServiceTest` | 死信重放受理、已成功无需重放、处理中不可重放、16 线程并发重放只受理一次、容量拒绝后原状态不变且可再重放、批量重放条目互不影响、重放状态查询、重放限流 |
| `ReplayIntegrationTest` | 端到端重放成功（recoverable 模拟下游恢复）、重放重试次数独立且耗尽可再重放、已成功事件重放幂等空操作、按原因批量重放且未命中死信不受影响、重放状态 404 |

测试日志中以 `=== 场景：... ===` 与 `通过：...` 标明每个用例覆盖的业务与结果。
