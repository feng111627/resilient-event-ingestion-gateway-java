# 弹性事件接入网关（本地模拟版）

批量事件接入网关：本地模拟上游的并发与重复投递，保证高并发、重复提交和失败重试下
接入结果稳定且可解释。

## 架构与模块

```
api/         IngestionController   接入与查询 REST API
service/     IngestionService      去重受理、异步分发、重试与死信隔离（有界队列）
store/       EventStore/InMemory   以去重键为唯一约束的记录存储（ConcurrentHashMap）
processing/  EventProcessor        业务处理器接口 + 本地模拟实现（可注入失败/耗时）
             RetryPolicy           重试参数（次数、退避）
ratelimit/   RateLimiter           令牌桶限流（取不到令牌立即拒绝，不排队）
metrics/     GatewayMetrics        计数、耗时、队列深度、拒绝次数
```

处理流水线：`批量受理(去重) -> 有界队列 -> 工作线程处理 -> 成功/重试/死信`

## 接入协议

### POST /api/events/batch

请求体为事件数组，每个事件必须携带上游生成的去重键 `dedupKey`：

```json
[
  {"dedupKey": "order-001", "eventType": "ORDER", "payload": "..."},
  {"dedupKey": "order-002", "eventType": "ORDER", "payload": "..."}
]
```

响应 `202 Accepted`，逐条给出受理结果：

```json
{
  "accepted": 1, "duplicated": 1, "rejected": 0,
  "results": [
    {"dedupKey": "order-001", "outcome": "ACCEPTED"},
    {"dedupKey": "order-002", "outcome": "DUPLICATE"}
  ]
}
```

- `ACCEPTED`：首次受理，进入异步处理
- `DUPLICATE`：去重键已存在，幂等忽略，不重复计数
- `REJECTED`：队列容量不足或键非法，**不留占位记录**，上游可直接重试
- 触发限流时整个请求返回 `429 Too Many Requests`

### 查询与指标

| 接口 | 说明 |
|---|---|
| `GET /api/events/{dedupKey}` | 按去重键查询处理记录（状态、尝试次数、失败原因、耗时） |
| `GET /api/events?status=DEAD_LETTER` | 按状态查询（RECEIVED/PROCESSING/SUCCESS/FAILED_RETRYING/DEAD_LETTER） |
| `GET /api/events?failureReason=...` | 按失败原因查询 |
| `GET /api/dead-letters` | 失败记录（死信）列表 |
| `GET /api/metrics` | 指标：received/duplicated/rejected/succeeded/retried/deadLettered、queueDepth、avg/maxLatencyMillis |

## 去重与重试规则

- **幂等去重**：以 `dedupKey` 为唯一约束（`putIfAbsent`），同批重复、跨批重复、
  乱序重投都只受理一次，业务效果只应用一次。
- **可重试异常**（`RetryableEventException` 及其他未分类异常）：按
  `gateway.retry.max-attempts`（含首次，默认 3）重试，退避为
  `backoffMillis × 第n次尝试`，由调度线程延迟投递，不阻塞工作线程。
- **不可重试异常**（`NonRetryableEventException`）：不重试，直接进死信。
- **重试耗尽**：进入死信（`DEAD_LETTER`），可通过死信与失败原因接口查询。
- **失败隔离**：失败事件的重试在独立调度线程等待，不拖住队列中后续正常事件。

## 容量与限流参数（application.properties）

| 参数 | 默认 | 说明 |
|---|---|---|
| `gateway.worker-threads` | 4 | 处理线程数 |
| `gateway.queue-capacity` | 100 | 队列容量，满则该条拒绝（明确拒绝而非无限堆积） |
| `gateway.rate-limit-per-second` | 200 | 接入限流速率（令牌桶，取不到令牌立即 429） |
| `gateway.retry.max-attempts` | 3 | 最大尝试次数（含首次） |
| `gateway.retry.backoff-millis` | 50 | 重试退避基数（毫秒） |

限流与队列拒绝都是**立即失败**，不排队等待，因此压力升高时响应延迟有界。

## 本地验证

### 运行单测（日志可见各用例覆盖的业务与通过情况）

```bash
mvn test
```

覆盖场景：同批/跨批/乱序重复投递幂等、8 线程并发重叠批次守恒、可重试重试成功、
不可重试直进死信、重试耗尽进死信、失败不拖住后续事件、队列满明确拒绝、
限流突发拒绝与恢复、HTTP 全链路查询。

### 启动并手动验证

```bash
mvn spring-boot:run
# 默认端口 8080，被占用时可覆盖：
# mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=18099"
```

```bash
# 1. 批量接入（含重复键与失败注入指令）
curl -s -X POST localhost:8080/api/events/batch -H 'Content-Type: application/json' -d '[
  {"dedupKey":"k1","eventType":"ORDER","payload":"normal"},
  {"dedupKey":"k1","eventType":"ORDER","payload":"normal"},
  {"dedupKey":"k2","eventType":"ORDER","payload":"RETRYABLE:2"},
  {"dedupKey":"k3","eventType":"ORDER","payload":"FATAL"}
]'

# 2. 查询处理结果与死信
curl -s localhost:8080/api/events/k2
curl -s localhost:8080/api/dead-letters

# 3. 查看指标（耗时、队列深度、拒绝次数）
curl -s localhost:8080/api/metrics
```

模拟下游通过 payload 指令注入行为：`RETRYABLE:n`（前 n 次可重试失败）、
`RETRYABLE`（一直失败）、`FATAL`（不可重试失败）、`SLOW:毫秒`（模拟耗时）。
