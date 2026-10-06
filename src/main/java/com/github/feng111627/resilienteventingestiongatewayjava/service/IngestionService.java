package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventMessage;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.metrics.GatewayMetrics;
import com.github.feng111627.resilienteventingestiongatewayjava.processing.EventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.processing.NonRetryableEventException;
import com.github.feng111627.resilienteventingestiongatewayjava.processing.RetryPolicy;
import com.github.feng111627.resilienteventingestiongatewayjava.store.EventStore;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 接入核心服务：去重受理、异步分发、重试与死信隔离。
 * <p>
 * 有界队列 + 拒绝策略保证容量不足时明确拒绝而非无限堆积；
 * 可重试失败经延迟调度重试，不阻塞工作线程，失败事件不会拖住后续事件。
 */
@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final EventStore store;
    private final EventProcessor processor;
    private final RetryPolicy retryPolicy;
    private final GatewayMetrics metrics;
    private final ThreadPoolExecutor workers;
    private final ScheduledExecutorService retryScheduler;
    private final AtomicInteger inFlight = new AtomicInteger();

    public IngestionService(EventStore store, EventProcessor processor,
                            RetryPolicy retryPolicy, GatewayMetrics metrics,
                            GatewayProperties properties) {
        this.store = store;
        this.processor = processor;
        this.retryPolicy = retryPolicy;
        this.metrics = metrics;
        this.workers = new ThreadPoolExecutor(
                properties.getWorkerThreads(), properties.getWorkerThreads(),
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(properties.getQueueCapacity()),
                r -> {
                    Thread t = new Thread(r, "event-worker");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());
        this.retryScheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "event-retry");
            t.setDaemon(true);
            return t;
        });
    }

    /** 批量受理：幂等去重后投递异步队列，队列满则该条拒绝并移除占位记录。 */
    public BatchIngestResponse ingestBatch(List<EventMessage> events) {
        int accepted = 0, duplicated = 0, rejected = 0;
        List<BatchIngestResponse.ItemResult> results = new ArrayList<>(events.size());
        for (EventMessage event : events) {
            if (event.dedupKey() == null || event.dedupKey().isBlank()) {
                rejected++;
                metrics.recordRejected();
                results.add(new BatchIngestResponse.ItemResult(event.dedupKey(),
                        BatchIngestResponse.ItemResult.REJECTED));
                continue;
            }
            EventRecord record = new EventRecord(event);
            if (!store.saveIfAbsent(record)) {
                duplicated++;
                metrics.recordDuplicate();
                log.info("[接入] 重复提交被去重: key={}", event.dedupKey());
                results.add(new BatchIngestResponse.ItemResult(event.dedupKey(),
                        BatchIngestResponse.ItemResult.DUPLICATE));
                continue;
            }
            try {
                submit(record);
                accepted++;
                results.add(new BatchIngestResponse.ItemResult(event.dedupKey(),
                        BatchIngestResponse.ItemResult.ACCEPTED));
            } catch (RejectedExecutionException e) {
                store.remove(event.dedupKey());
                rejected++;
                metrics.recordRejected();
                log.warn("[接入] 队列已满拒绝受理: key={}", event.dedupKey());
                results.add(new BatchIngestResponse.ItemResult(event.dedupKey(),
                        BatchIngestResponse.ItemResult.REJECTED));
            }
        }
        metrics.recordReceived(accepted);
        return new BatchIngestResponse(accepted, duplicated, rejected, results);
    }

    private void submit(EventRecord record) {
        inFlight.incrementAndGet();
        try {
            workers.execute(() -> processSafely(record));
        } catch (RejectedExecutionException e) {
            inFlight.decrementAndGet();
            throw e;
        }
        metrics.setQueueDepth(workers.getQueue().size());
    }

    private void processSafely(EventRecord record) {
        try {
            processOnce(record);
        } finally {
            inFlight.decrementAndGet();
            metrics.setQueueDepth(workers.getQueue().size());
        }
    }

    private void processOnce(EventRecord record) {
        String key = record.getMessage().dedupKey();
        record.markProcessing();
        long start = System.nanoTime();
        try {
            processor.process(record.getMessage());
            long latency = elapsedMillis(start);
            record.markSuccess(latency);
            metrics.recordSuccess(latency);
            log.info("[处理] 成功: key={} 第{}次尝试 耗时{}ms", key, record.getAttempts(), latency);
        } catch (NonRetryableEventException e) {
            long latency = elapsedMillis(start);
            record.markDeadLetter(e.getMessage(), latency);
            metrics.recordDeadLetter();
            log.warn("[处理] 不可重试失败进入死信: key={} 原因={}", key, e.getMessage());
        } catch (Exception e) {
            long latency = elapsedMillis(start);
            if (record.getAttempts() < retryPolicy.getMaxAttempts()) {
                record.markRetrying(e.getMessage(), latency);
                metrics.recordRetry();
                long delay = retryPolicy.getBackoffMillis() * record.getAttempts();
                log.warn("[处理] 可重试失败: key={} 第{}次尝试 {}ms后重试 原因={}",
                        key, record.getAttempts(), delay, e.getMessage());
                scheduleRetry(record, delay);
            } else {
                record.markDeadLetter(e.getMessage(), latency);
                metrics.recordDeadLetter();
                log.warn("[处理] 重试耗尽进入死信: key={} 共{}次尝试 原因={}",
                        key, record.getAttempts(), e.getMessage());
            }
        }
    }

    private void scheduleRetry(EventRecord record, long delayMillis) {
        inFlight.incrementAndGet();
        retryScheduler.schedule(() -> {
            try {
                workers.execute(() -> processSafely(record));
            } catch (RejectedExecutionException e) {
                inFlight.decrementAndGet();
                record.markDeadLetter("RETRY_DISPATCH_REJECTED: 重试投递时队列已满", 0);
                metrics.recordDeadLetter();
                log.warn("[处理] 重试投递被拒进入死信: key={}", record.getMessage().dedupKey());
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    private long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    /** 等待队列与在途任务排空（本地验证与测试用）。 */
    public void awaitQuiescence(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (inFlight.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        workers.shutdown();
        retryScheduler.shutdown();
    }
}
