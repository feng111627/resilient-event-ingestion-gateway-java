package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 默认异步事件处理器。
 * - 有界队列：trySubmit 非阻塞入队，满则返回 false 由接入侧拒绝，绝不无限堆积；
 * - 固定工作线程消费，单条事件异常被捕获隔离，不影响后续事件；
 * - 可重试异常按指数退避重投，重试耗尽进入死信；不可重试异常直接死信。
 */
@Component
public class DefaultEventProcessor implements EventProcessor {

    private static final Logger log = LoggerFactory.getLogger(DefaultEventProcessor.class);

    public static final String REASON_NON_RETRYABLE = "NON_RETRYABLE";
    public static final String REASON_MAX_RETRIES = "MAX_RETRIES_EXCEEDED";
    /** 重放轮内重试耗尽（仅用于指标区分，记录上的 failureReason 保留首次失败原因） */
    public static final String REASON_REPLAY_MAX_RETRIES = "REPLAY_MAX_RETRIES_EXCEEDED";
    public static final String REASON_REPLAY_NON_RETRYABLE = "REPLAY_NON_RETRYABLE";

    private final GatewayProperties props;
    private final GatewayMetrics metrics;

    private BlockingQueue<EventRecord> queue;
    private Thread[] workers;
    private ScheduledExecutorService retryScheduler;
    private final AtomicInteger activeWorkers = new AtomicInteger();
    private volatile boolean running = false;

    public DefaultEventProcessor(GatewayProperties props, GatewayMetrics metrics) {
        this.props = props;
        this.metrics = metrics;
    }

    @PostConstruct
    @Override
    public void start() {
        this.queue = new ArrayBlockingQueue<>(props.getQueueCapacity());
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "event-retry-scheduler");
            t.setDaemon(true);
            return t;
        });
        this.workers = new Thread[props.getWorkerThreads()];
        this.running = true;
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(this::consumeLoop, "event-worker-" + i);
            workers[i].setDaemon(true);
            workers[i].start();
        }
        log.info("EventProcessor started: workers={}, queueCapacity={}, maxRetries={}",
                props.getWorkerThreads(), props.getQueueCapacity(), props.getMaxRetries());
    }

    @PreDestroy
    @Override
    public void shutdown() {
        running = false;
        if (workers != null) {
            for (Thread w : workers) {
                w.interrupt();
            }
        }
        if (retryScheduler != null) {
            retryScheduler.shutdownNow();
        }
        log.info("EventProcessor shutdown");
    }

    @Override
    public boolean trySubmit(EventRecord record) {
        return queue.offer(record);
    }

    @Override
    public int getQueueDepth() {
        return queue == null ? 0 : queue.size();
    }

    @Override
    public int getQueueCapacity() {
        return props.getQueueCapacity();
    }

    @Override
    public int getActiveWorkers() {
        return activeWorkers.get();
    }

    private void consumeLoop() {
        while (running || !queue.isEmpty()) {
            EventRecord record;
            try {
                record = queue.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (record == null) {
                continue;
            }
            activeWorkers.incrementAndGet();
            try {
                processOnce(record);
            } catch (Throwable t) {
                // 兜底隔离：任何未预期异常都不能拖垮工作线程
                log.error("Unexpected error processing {}, dead-lettering", record.getDedupKey(), t);
                if (record.isReplayInFlight()) {
                    record.markReplayFailed("unexpected: " + t);
                    metrics.onReplayFailed();
                } else {
                    record.markDeadLetter(REASON_NON_RETRYABLE, "unexpected: " + t);
                }
                metrics.onDeadLetter(REASON_NON_RETRYABLE);
            } finally {
                activeWorkers.decrementAndGet();
            }
        }
    }

    /** 单次处理 + 重试/死信判定；重放中的记录使用独立的 replayMaxRetries 与重放内尝试计数 */
    private void processOnce(EventRecord record) {
        record.markAttempt();
        boolean replay = record.isReplayInFlight();
        int maxRetries = replay ? props.getReplayMaxRetries() : props.getMaxRetries();
        int attemptNo = replay ? record.getReplayAttempts() : record.getAttempts();
        try {
            doProcess(record);
            long elapsed = Duration.between(record.getReceivedAt(), Instant.now()).toMillis();
            if (replay) {
                record.markReplaySuccess(elapsed);
                metrics.onReplaySuccess();
                log.info("Replay of {} succeeded (replayAttempts={})",
                        record.getDedupKey(), record.getReplayAttempts());
            } else {
                record.markSuccess(elapsed);
            }
            metrics.onSuccess(elapsed);
            log.debug("Event {} succeeded after {} attempts", record.getDedupKey(), record.getAttempts());
        } catch (RetryableProcessingException e) {
            if (attemptNo <= maxRetries) {
                record.markRetrying(e.getMessage());
                metrics.onRetry();
                long delay = props.getRetryBackoffMillis() * (1L << (attemptNo - 1));
                log.debug("Event {} retryable failure (attempt {}), retry in {}ms: {}",
                        record.getDedupKey(), attemptNo, delay, e.getMessage());
                retryScheduler.schedule(() -> requeue(record), delay, TimeUnit.MILLISECONDS);
            } else if (replay) {
                // 重放轮内重试耗尽：回到 DEAD_LETTER，保留首次失败原因，之后仍可再次重放
                record.markReplayFailed(e.getMessage());
                metrics.onReplayFailed();
                metrics.onDeadLetter(REASON_REPLAY_MAX_RETRIES);
                log.warn("Replay of {} exhausted {} replay retries, back to dead-letter: {}",
                        record.getDedupKey(), props.getReplayMaxRetries(), e.getMessage());
            } else {
                record.markDeadLetter(REASON_MAX_RETRIES, e.getMessage());
                metrics.onDeadLetter(REASON_MAX_RETRIES);
                log.warn("Event {} exhausted {} retries, dead-lettered: {}",
                        record.getDedupKey(), props.getMaxRetries(), e.getMessage());
            }
        } catch (NonRetryableProcessingException e) {
            if (replay) {
                record.markReplayFailed(e.getMessage());
                metrics.onReplayFailed();
                metrics.onDeadLetter(REASON_REPLAY_NON_RETRYABLE);
                log.warn("Replay of {} hit non-retryable failure, back to dead-letter: {}",
                        record.getDedupKey(), e.getMessage());
            } else {
                record.markDeadLetter(REASON_NON_RETRYABLE, e.getMessage());
                metrics.onDeadLetter(REASON_NON_RETRYABLE);
                log.warn("Event {} non-retryable failure, dead-lettered: {}", record.getDedupKey(), e.getMessage());
            }
        }
    }

    /** 重投回队列；若队列满则稍后再次重投，保证不丢数据 */
    private void requeue(EventRecord record) {
        if (!running) {
            return;
        }
        if (!queue.offer(record)) {
            retryScheduler.schedule(() -> requeue(record), props.getRetryBackoffMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 本地模拟下游处理。failMode:
     * none=成功；retryable=恒可重试失败；fatal=恒不可重试失败；flaky=前两次可重试失败后成功。
     */
    private void doProcess(EventRecord record) {
        String mode = record.getFailMode();
        switch (mode) {
            case "retryable" -> throw new RetryableProcessingException("simulated transient downstream error");
            case "fatal" -> throw new NonRetryableProcessingException("simulated permanent validation error");
            case "flaky" -> {
                if (record.getAttempts() < 3) {
                    throw new RetryableProcessingException("simulated flaky error, attempt=" + record.getAttempts());
                }
            }
            default -> {
                // none: 直接成功
            }
        }
    }
}
