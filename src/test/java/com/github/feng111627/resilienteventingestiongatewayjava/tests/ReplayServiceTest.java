package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayBatchResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayItemResult;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayStatusView;
import com.github.feng111627.resilienteventingestiongatewayjava.service.DefaultEventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.service.DefaultReplayService;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventStore;
import com.github.feng111627.resilienteventingestiongatewayjava.service.GatewayMetrics;
import com.github.feng111627.resilienteventingestiongatewayjava.web.RateLimitExceededException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重放服务单元测试：重放受理、无需重放识别、并发重放去重、容量拒绝后原状态不变且可再重放、
 * 批量重放条目互不影响、重放状态查询、重放限流。
 */
class ReplayServiceTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayServiceTest.class);

    private GatewayProperties props;
    private EventStore store;
    private GatewayMetrics metrics;
    private StubProcessor processor;
    private DefaultReplayService service;

    /** 可控处理器桩：capacity<=已提交数时模拟队列满；不真正处理，重放认领保持悬挂 */
    static class StubProcessor implements EventProcessor {
        final AtomicInteger submitted = new AtomicInteger();
        volatile int capacity = 100;

        @Override
        public boolean trySubmit(EventRecord record) {
            if (submitted.get() >= capacity) {
                return false;
            }
            submitted.incrementAndGet();
            return true;
        }

        @Override public int getQueueDepth() { return submitted.get(); }
        @Override public int getQueueCapacity() { return capacity; }
        @Override public int getActiveWorkers() { return 0; }
        @Override public void start() { }
        @Override public void shutdown() { }
    }

    @BeforeEach
    void setUp() {
        props = new GatewayProperties();
        props.setReplayRateLimitPerSecond(10000);
        props.setReplayMaxRetries(2);
        store = new EventStore();
        metrics = new GatewayMetrics();
        processor = new StubProcessor();
        service = new DefaultReplayService(props, store, processor, metrics);
    }

    /** 造一条已进入失败记录的死信事件 */
    private EventRecord deadLetter(String key, String reason) {
        EventRecord record = new EventRecord(key, "payload-of-" + key, "retryable");
        store.insertIfAbsent(record);
        record.markAttempt();
        record.markDeadLetter(reason, "simulated error of " + key);
        return record;
    }

    @Test
    @DisplayName("[重放受理] 死信事件重放被受理：计数+1、状态回到处理中、进入处理队列")
    void replayDeadLetterAccepted() {
        log.info("=== 场景：死信事件单条重放受理 ===");
        EventRecord record = deadLetter("r-accept", DefaultEventProcessor.REASON_MAX_RETRIES);

        ReplayItemResult result = service.replayByKey("r-accept");

        assertEquals("ACCEPTED", result.result());
        assertEquals(1, record.getReplayCount(), "重放次数应+1");
        assertEquals(EventStatus.RETRYING, record.getStatus(), "重放后应重新进入处理流程");
        assertTrue(record.isReplayInProgress());
        assertEquals(1, processor.submitted.get(), "应重新入队处理");
        log.info("通过：result={}, replayCount={}, status={}", result.result(),
                record.getReplayCount(), record.getStatus());
    }

    @Test
    @DisplayName("[无需重放] 已成功事件识别为 ALREADY_SUCCEEDED，不重复处理不重复计数")
    void succeededEventNeedsNoReplay() {
        log.info("=== 场景：已成功事件无需重放 ===");
        EventRecord record = new EventRecord("r-ok", "p", "none");
        store.insertIfAbsent(record);
        record.markAttempt();
        record.markSuccess(5);

        ReplayItemResult result = service.replayByKey("r-ok");

        assertEquals("ALREADY_SUCCEEDED", result.result());
        assertEquals(0, record.getReplayCount(), "不得重复计数");
        assertEquals(1, record.getAttempts(), "不得重复处理");
        assertEquals(0, processor.submitted.get(), "不得重新入队");
        assertEquals(EventStatus.SUCCESS, record.getStatus());
        log.info("通过：result={}, replayCount={}, attempts={}", result.result(),
                record.getReplayCount(), record.getAttempts());
    }

    @Test
    @DisplayName("[不可重放] 处理中的事件返回 NOT_REPLAYABLE，未知键返回 NOT_FOUND")
    void inFlightEventNotReplayable() {
        log.info("=== 场景：处理中事件不可重放 ===");
        EventRecord record = new EventRecord("r-inflight", "p", "none");
        store.insertIfAbsent(record);

        ReplayItemResult result = service.replayByKey("r-inflight");
        assertEquals("NOT_REPLAYABLE", result.result());
        assertEquals(0, processor.submitted.get());

        ReplayItemResult missing = service.replayByKey("no-such-key");
        assertEquals("NOT_FOUND", missing.result());
        log.info("通过：inflight={}, missing={}", result.result(), missing.result());
    }

    @Test
    @DisplayName("[并发重放] 同一失败事件并发重放只受理一次，其余 ALREADY_REPLAYING")
    void concurrentReplayClaimedOnlyOnce() throws InterruptedException {
        log.info("=== 场景：并发重放同一事件只处理一次 ===");
        EventRecord record = deadLetter("r-race", DefaultEventProcessor.REASON_MAX_RETRIES);
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger alreadyReplaying = new AtomicInteger();
        ConcurrentLinkedQueue<String> unexpected = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    ReplayItemResult r = service.replayByKey("r-race");
                    switch (r.result()) {
                        case "ACCEPTED" -> accepted.incrementAndGet();
                        case "ALREADY_REPLAYING" -> alreadyReplaying.incrementAndGet();
                        default -> unexpected.add(r.result());
                    }
                } catch (Exception e) {
                    unexpected.add(e.toString());
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        assertTrue(unexpected.isEmpty(), "不应出现其他结果: " + unexpected);
        assertEquals(1, accepted.get(), "并发下只能有一个重放被受理");
        assertEquals(threads - 1, alreadyReplaying.get());
        assertEquals(1, record.getReplayCount(), "只计数一次");
        assertEquals(1, processor.submitted.get(), "只入队一次，不重复处理");
        log.info("通过：accepted={}, alreadyReplaying={}, replayCount={}",
                accepted.get(), alreadyReplaying.get(), record.getReplayCount());
    }

    @Test
    @DisplayName("[容量拒绝] 队列满时重放被拒，原失败记录不被改动，容量恢复后可再次重放")
    void replayRejectedWhenFullThenRetryable() {
        log.info("=== 场景：容量不足拒绝重放且原状态不变 ===");
        EventRecord record = deadLetter("r-full", DefaultEventProcessor.REASON_MAX_RETRIES);
        String errorBefore = record.getLastError();
        int attemptsBefore = record.getAttempts();
        processor.capacity = 0; // 模拟队列满

        ReplayItemResult rejected = service.replayByKey("r-full");

        assertEquals("REJECTED", rejected.result());
        assertEquals(EventStatus.DEAD_LETTER, record.getStatus(), "状态必须保持死信不变");
        assertEquals(DefaultEventProcessor.REASON_MAX_RETRIES, record.getFailureReason(),
                "失败原因必须保持不变");
        assertEquals(errorBefore, record.getLastError(), "错误信息必须保持不变");
        assertEquals(attemptsBefore, record.getAttempts(), "尝试次数必须保持不变");
        assertEquals(0, record.getReplayCount(), "被拒不得计入重放次数");
        assertFalse(record.isReplayInProgress(), "认领必须被释放，不得卡在重放中");
        log.info("通过（拒绝部分）：status={}, reason={}, replayCount={}",
                record.getStatus(), record.getFailureReason(), record.getReplayCount());

        // 容量恢复后可再次重放
        processor.capacity = 10;
        ReplayItemResult second = service.replayByKey("r-full");
        assertEquals("ACCEPTED", second.result());
        assertEquals(1, record.getReplayCount());
        assertEquals(1, processor.submitted.get());
        log.info("通过（再重放部分）：result={}, replayCount={}", second.result(), record.getReplayCount());
    }

    @Test
    @DisplayName("[批量重放] 按失败原因批量重放，可放/不可放条目互不影响")
    void batchReplayByReasonMixedItems() {
        log.info("=== 场景：按失败原因批量重放，条目互不影响 ===");
        deadLetter("b-max-1", DefaultEventProcessor.REASON_MAX_RETRIES);
        deadLetter("b-max-2", DefaultEventProcessor.REASON_MAX_RETRIES);
        deadLetter("b-fatal-1", DefaultEventProcessor.REASON_NON_RETRYABLE);
        // 一条已被并发认领悬挂中的死信
        EventRecord hanging = deadLetter("b-max-3", DefaultEventProcessor.REASON_MAX_RETRIES);
        assertTrue(hanging.tryClaimReplay());

        ReplayBatchResponse resp = service.replayByReason(DefaultEventProcessor.REASON_MAX_RETRIES);

        assertEquals(3, resp.total(), "只应命中该原因的死信");
        assertEquals(2, resp.accepted());
        assertEquals(1, resp.alreadyReplaying());
        assertEquals(2, processor.submitted.get());
        // 另一条原因的死信不受影响
        assertEquals(EventStatus.DEAD_LETTER, store.findByKey("b-fatal-1").orElseThrow().getStatus());
        log.info("通过：total={}, accepted={}, alreadyReplaying={}",
                resp.total(), resp.accepted(), resp.alreadyReplaying());

        // 批量中容量耗尽：部分受理部分拒绝，互不影响
        EventStore store2 = new EventStore();
        GatewayMetrics metrics2 = new GatewayMetrics();
        StubProcessor p2 = new StubProcessor();
        p2.capacity = 1;
        DefaultReplayService service2 = new DefaultReplayService(props, store2, p2, metrics2);
        for (String k : List.of("c-1", "c-2", "c-3")) {
            EventRecord r = new EventRecord(k, "p", "retryable");
            store2.insertIfAbsent(r);
            r.markAttempt();
            r.markDeadLetter(DefaultEventProcessor.REASON_MAX_RETRIES, "e");
        }
        ReplayBatchResponse resp2 = service2.replayByReason(null);
        assertEquals(3, resp2.total());
        assertEquals(1, resp2.accepted(), "容量只够一条");
        assertEquals(2, resp2.rejected(), "其余明确拒绝");
        long stillDead = store2.findDeadLettersByReason(null).stream()
                .filter(r -> r.getStatus() == EventStatus.DEAD_LETTER).count();
        assertEquals(2, stillDead, "被拒条目必须保持死信可再次重放");
        log.info("通过（容量混合）：accepted={}, rejected={}, 仍为死信={}",
                resp2.accepted(), resp2.rejected(), stillDead);
    }

    @Test
    @DisplayName("[状态查询] 按去重键查询重放次数与结果，未知键为空")
    void replayStatusView() {
        log.info("=== 场景：重放状态查询 ===");
        EventRecord record = deadLetter("r-view", DefaultEventProcessor.REASON_MAX_RETRIES);
        service.replayByKey("r-view");

        Optional<ReplayStatusView> view = service.replayStatusOf("r-view");
        assertTrue(view.isPresent());
        assertTrue(view.get().replayed());
        assertEquals(1, view.get().replayCount());
        assertTrue(view.get().replayInProgress());
        assertEquals(EventStatus.RETRYING.name(), view.get().status());

        assertTrue(service.replayStatusOf("no-such-key").isEmpty());
        log.info("通过：replayCount={}, inProgress={}, status={}",
                view.get().replayCount(), view.get().replayInProgress(), view.get().status());
    }

    @Test
    @DisplayName("[限流] 重放入口超过速率限制快速拒绝，延迟有界")
    void replayRateLimited() {
        log.info("=== 场景：重放入口限流 ===");
        props.setReplayRateLimitPerSecond(1);
        service = new DefaultReplayService(props, store, processor, metrics);
        deadLetter("r-limited", DefaultEventProcessor.REASON_MAX_RETRIES);

        service.replayByKey("r-limited");
        RateLimitExceededException ex = assertThrows(RateLimitExceededException.class,
                () -> service.replayByKey("r-limited"));
        log.info("通过：限流异常信息={}", ex.getMessage());
    }
}
