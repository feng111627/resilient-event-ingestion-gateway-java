package com.github.feng111627.resilienteventingestiongatewayjava.api;

import com.github.feng111627.resilienteventingestiongatewayjava.domain.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventMessage;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.metrics.GatewayMetrics;
import com.github.feng111627.resilienteventingestiongatewayjava.ratelimit.RateLimiter;
import com.github.feng111627.resilienteventingestiongatewayjava.service.IngestionService;
import com.github.feng111627.resilienteventingestiongatewayjava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 接入与查询 API。
 */
@RestController
@RequestMapping("/api")
public class IngestionController {

    private static final Logger log = LoggerFactory.getLogger(IngestionController.class);

    private final IngestionService ingestionService;
    private final EventStore store;
    private final GatewayMetrics metrics;
    private final RateLimiter rateLimiter;

    public IngestionController(IngestionService ingestionService, EventStore store,
                               GatewayMetrics metrics, RateLimiter rateLimiter) {
        this.ingestionService = ingestionService;
        this.store = store;
        this.metrics = metrics;
        this.rateLimiter = rateLimiter;
    }

    /** 批量接入：限流不通过返回 429，受理结果按去重键逐条返回。 */
    @PostMapping("/events/batch")
    public ResponseEntity<BatchIngestResponse> ingestBatch(@RequestBody List<EventMessage> events) {
        if (!rateLimiter.tryAcquire()) {
            metrics.recordRejected();
            log.warn("[接入] 限流拒绝批量请求 size={}", events.size());
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(new BatchIngestResponse(0, 0, events.size(), List.of()));
        }
        BatchIngestResponse response = ingestionService.ingestBatch(events);
        log.info("[接入] 批量受理 size={} accepted={} duplicated={} rejected={}",
                events.size(), response.accepted(), response.duplicated(), response.rejected());
        return ResponseEntity.accepted().body(response);
    }

    /** 按去重键查询处理结果。 */
    @GetMapping("/events/{dedupKey}")
    public ResponseEntity<EventRecord> findByKey(@PathVariable String dedupKey) {
        return store.findByKey(dedupKey)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** 按处理状态或失败原因查询。 */
    @GetMapping("/events")
    public List<EventRecord> query(@RequestParam(required = false) EventStatus status,
                                   @RequestParam(required = false) String failureReason) {
        if (status != null) {
            return store.findByStatus(status);
        }
        if (failureReason != null) {
            return store.findByFailureReason(failureReason);
        }
        return store.findAll();
    }

    /** 失败记录（死信）查询。 */
    @GetMapping("/dead-letters")
    public List<EventRecord> deadLetters() {
        return store.findByStatus(EventStatus.DEAD_LETTER);
    }

    /** 运行指标：处理耗时、队列深度、拒绝次数等。 */
    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        return metrics.snapshot();
    }
}
