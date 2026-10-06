package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.model.GatewayMetricsSnapshot;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** 网关指标采集（线程安全，基于 LongAdder 降低并发竞争） */
@Component
public class GatewayMetrics {

    private final LongAdder totalReceived = new LongAdder();
    private final LongAdder totalAccepted = new LongAdder();
    private final LongAdder totalDuplicates = new LongAdder();
    private final LongAdder totalRejected = new LongAdder();
    private final LongAdder totalSuccess = new LongAdder();
    private final LongAdder totalDeadLetter = new LongAdder();
    private final LongAdder totalRetries = new LongAdder();
    private final LongAdder processingMillisSum = new LongAdder();
    private final AtomicLong maxProcessingMillis = new AtomicLong();
    private final ConcurrentHashMap<String, LongAdder> deadLetterByReason = new ConcurrentHashMap<>();

    public void onReceived(int count) { totalReceived.add(count); }
    public void onAccepted() { totalAccepted.increment(); }
    public void onDuplicate() { totalDuplicates.increment(); }
    public void onRejected() { totalRejected.increment(); }
    public void onRetry() { totalRetries.increment(); }

    public void onSuccess(long processingMillis) {
        totalSuccess.increment();
        processingMillisSum.add(processingMillis);
        maxProcessingMillis.accumulateAndGet(processingMillis, Math::max);
    }

    public void onDeadLetter(String reason) {
        totalDeadLetter.increment();
        deadLetterByReason.computeIfAbsent(reason, k -> new LongAdder()).increment();
    }

    public GatewayMetricsSnapshot snapshot(int queueDepth, int queueCapacity, int activeWorkers) {
        long success = totalSuccess.sum();
        double avg = success == 0 ? 0.0 : (double) processingMillisSum.sum() / success;
        Map<String, Long> byReason = new ConcurrentHashMap<>();
        deadLetterByReason.forEach((k, v) -> byReason.put(k, v.sum()));
        return new GatewayMetricsSnapshot(
                totalReceived.sum(),
                totalAccepted.sum(),
                totalDuplicates.sum(),
                totalRejected.sum(),
                success,
                totalDeadLetter.sum(),
                totalRetries.sum(),
                queueDepth,
                queueCapacity,
                activeWorkers,
                avg,
                maxProcessingMillis.get(),
                Map.copyOf(byReason)
        );
    }
}
