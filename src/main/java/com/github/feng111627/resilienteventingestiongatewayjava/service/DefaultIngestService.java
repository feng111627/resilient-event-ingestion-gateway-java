package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchItemResult;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRequest;
import com.github.feng111627.resilienteventingestiongatewayjava.web.RateLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 默认接入服务。
 * 处理顺序：限流 -> 批量大小校验 -> 逐条校验 -> 幂等写入 -> 非阻塞入队。
 * 队列满时该条明确拒绝（REJECTED），不阻塞、不堆积。
 */
@Service
public class DefaultIngestService implements IngestService {

    private static final Logger log = LoggerFactory.getLogger(DefaultIngestService.class);

    private final GatewayProperties props;
    private final EventStore store;
    private final EventProcessor processor;
    private final GatewayMetrics metrics;
    private final TokenBucket rateLimiter;

    public DefaultIngestService(GatewayProperties props, EventStore store,
                                EventProcessor processor, GatewayMetrics metrics) {
        this.props = props;
        this.store = store;
        this.processor = processor;
        this.metrics = metrics;
        this.rateLimiter = new TokenBucket(props.getRateLimitPerSecond());
    }

    @Override
    public BatchIngestResponse ingest(List<EventRequest> events) {
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("event list must not be empty");
        }
        if (events.size() > props.getMaxBatchSize()) {
            throw new IllegalArgumentException(
                    "batch size " + events.size() + " exceeds maxBatchSize " + props.getMaxBatchSize());
        }
        if (!rateLimiter.tryAcquire()) {
            metrics.onRejected();
            throw new RateLimitExceededException("rate limit exceeded: "
                    + props.getRateLimitPerSecond() + " req/s");
        }

        metrics.onReceived(events.size());
        String batchId = UUID.randomUUID().toString();
        List<BatchItemResult> items = new ArrayList<>(events.size());
        int accepted = 0, duplicates = 0, rejected = 0, invalid = 0;

        for (EventRequest req : events) {
            if (req == null || req.dedupKey() == null || req.dedupKey().isBlank()) {
                items.add(BatchItemResult.invalid(req == null ? null : req.dedupKey(), "dedupKey is required"));
                invalid++;
                continue;
            }
            EventRecord record = new EventRecord(req.dedupKey(), req.payload(), req.failMode());
            if (!store.insertIfAbsent(record)) {
                // 同一去重键重复提交：幂等跳过，不重复计数、不重复入队
                items.add(BatchItemResult.duplicate(req.dedupKey()));
                metrics.onDuplicate();
                duplicates++;
                continue;
            }
            if (!processor.trySubmit(record)) {
                // 容量不足：明确拒绝，不无限堆积
                items.add(BatchItemResult.rejected(req.dedupKey(), "processing queue is full, retry later"));
                metrics.onRejected();
                rejected++;
                continue;
            }
            items.add(BatchItemResult.accepted(req.dedupKey()));
            metrics.onAccepted();
            accepted++;
        }

        log.info("Batch {} ingested: total={}, accepted={}, duplicates={}, rejected={}, invalid={}",
                batchId, events.size(), accepted, duplicates, rejected, invalid);
        return new BatchIngestResponse(batchId, events.size(), accepted, duplicates, rejected, invalid, items);
    }

    /** 简单令牌桶限流器：限制每秒请求数，拒绝快速返回，保证压力下延迟有界 */
    static class TokenBucket {
        private final double permitsPerSecond;
        private final double maxPermits;
        private double availablePermits;
        private long lastRefillNanos;

        TokenBucket(double permitsPerSecond) {
            this.permitsPerSecond = permitsPerSecond;
            this.maxPermits = Math.max(1.0, permitsPerSecond);
            this.availablePermits = this.maxPermits;
            this.lastRefillNanos = System.nanoTime();
        }

        synchronized boolean tryAcquire() {
            refill();
            if (availablePermits >= 1.0) {
                availablePermits -= 1.0;
                return true;
            }
            return false;
        }

        private void refill() {
            long now = System.nanoTime();
            double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000.0;
            availablePermits = Math.min(maxPermits, availablePermits + elapsedSeconds * permitsPerSecond);
            lastRefillNanos = now;
        }
    }
}
