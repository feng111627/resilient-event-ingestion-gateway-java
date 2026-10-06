package com.github.feng111627.resilienteventingestiongatewayjava.web;

import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.model.GatewayMetricsSnapshot;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventStore;
import com.github.feng111627.resilienteventingestiongatewayjava.service.GatewayMetrics;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/** 状态查询与指标端点 */
@RestController
@RequestMapping("/api")
public class QueryController {

    private final EventStore store;
    private final EventProcessor processor;
    private final GatewayMetrics metrics;

    public QueryController(EventStore store, EventProcessor processor, GatewayMetrics metrics) {
        this.store = store;
        this.processor = processor;
        this.metrics = metrics;
    }

    /** 按去重键查询单条事件处理状态 */
    @GetMapping("/events/{dedupKey}")
    public ResponseEntity<EventRecord> getByKey(@PathVariable String dedupKey) {
        return store.findByKey(dedupKey)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /** 按处理状态查询，如 ?status=DEAD_LETTER；不传则返回全部 */
    @GetMapping("/events")
    public List<EventRecord> queryByStatus(@RequestParam(required = false) String status) {
        if (status == null || status.isBlank()) {
            return store.findAll();
        }
        return store.findByStatus(EventStatus.valueOf(status.toUpperCase()));
    }

    /** 查询失败（死信）记录，可按失败原因过滤，如 ?reason=MAX_RETRIES_EXCEEDED */
    @GetMapping("/failures")
    public List<EventRecord> queryFailures(@RequestParam(required = false) String reason) {
        return store.findDeadLettersByReason(reason);
    }

    /** 网关指标：处理耗时、队列深度、拒绝次数等 */
    @GetMapping("/metrics")
    public GatewayMetricsSnapshot metrics() {
        return metrics.snapshot(processor.getQueueDepth(), processor.getQueueCapacity(),
                processor.getActiveWorkers());
    }
}
