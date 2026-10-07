package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchReplayResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.service.DefaultEventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.service.DefaultReplayService;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventStore;
import com.github.feng111627.resilienteventingestiongatewayjava.service.GatewayMetrics;
import com.github.feng111627.resilienteventingestiongatewayjava.web.RateLimitExceededException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 失败事件重放测试：重放成功、并发/重复重放只处理一次、容量拒绝后原状态不变可再重放、
 * 重放失败回到死信可再重放、按原因批量重放、重放入口限流。
 */
class ReplayServiceTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayServiceTest.class);

    private GatewayProperties props;
    private EventStore store;
    private GatewayMetrics metrics;
    private DefaultEventProcessor processor;
    private DefaultReplayService replayService;

    /** 可控处理器桩：capacity 之外的提交一律拒绝，入队后不处理（用于确定性的并发/容量场景） */
    static class StubProcessor implements EventProcessor {
        final AtomicInteger submitted = new AtomicInteger();
        volatile int capacity = 10;

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
        props.setQueueCapacity(64);
        props.setWorkerThreads(2);
        props.setMaxRetries(0);          // 首次接入不重试，便于快速制造死信
        props.setReplayMaxRetries(2);    // 重放专属重试次数，与首次接入独立
        props.setRetryBackoffMillis(10);
        props.setReplayRateLimitPerSecond(1000);
        store = new EventStore();
        metrics = new GatewayMetrics();
        processor = new DefaultEventProcessor(props, metrics);
        processor.start();
        replayService = new DefaultReplayService(props, store, processor, metrics);
    }

    @AfterEach
    void tearDown() {
        processor.shutdown();
    }

    /** 直接接入一条事件并等待其进入死信 */
    private EventRecord ingestToDeadLetter(String key, String failMode) {
        EventRecord record = new EventRecord(key, "payload-of-" + key, failMode);
        store.insertIfAbsent(record);
        assertTrue(processor.trySubmit(record));
        awaitStatus(record, EventStatus.DEAD_LETTER, Duration.ofSeconds(5));
        return record;
    }

    private void awaitStatus(EventRecord record, EventStatus expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (record.getStatus() == expected) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while awaiting status");
            }
        }
        fail("timeout awaiting status " + expected + " for " + record.getDedupKey()
                + ", actual=" + record.getStatus());
    }

    @Test
    @DisplayName("[重放成功] flaky 死信事件重放后成功，重放轨迹可查")
    void replaySucceedsAndTracksResult() {
        log.info("=== 场景：失败事件重放成功 ===");
        // flaky 前两次尝试失败：首次接入(maxRetries=0)1 次失败进死信；重放第 1 次失败重试，第 2 次成功
        EventRecord record = ingestToDeadLetter("replay-ok-1", "flaky");
        assertEquals(1, record.getAttempts());
        assertEquals(0, record.getReplayCount());
        assertEquals("NONE", record.getLastReplayResult());

        ReplayResponse resp = replayService.replayByKey("replay-ok-1");
        assertEquals("REPLAY_ACCEPTED", resp.result());
        awaitStatus(record, EventStatus.SUCCESS, Duration.ofSeconds(5));

        assertEquals(1, record.getReplayCount(), "重放次数应记为 1");
        assertEquals("SUCCESS", record.getLastReplayResult(), "最终重放结果应为 SUCCESS");
        assertEquals(2, record.getReplayAttempts(), "重放轮内第 2 次尝试成功");
        assertEquals(3, record.getAttempts(), "总尝试次数=首次1+重放2");
        assertNotNull(record.getLastReplayAt());
        assertFalse(record.isReplayInFlight());
        log.info("通过：replayCount={}, lastReplayResult={}, replayAttempts={}, totalAttempts={}",
                record.getReplayCount(), record.getLastReplayResult(),
                record.getReplayAttempts(), record.getAttempts());
    }

    @Test
    @DisplayName("[并发重放] 同一死信事件并发重放只受理一次，其余返回在途")
    void concurrentReplayAcceptedOnlyOnce() throws InterruptedException {
        log.info("=== 场景：并发重放只处理一次 ===");
        // 使用桩处理器：受理后不处理，事件停在 REPLAYING，确定性验证并发守卫
        StubProcessor stub = new StubProcessor();
        DefaultReplayService svc = new DefaultReplayService(props, store, stub, metrics);
        EventRecord record = new EventRecord("conc-replay", "p", "fatal");
        record.markDeadLetter(DefaultEventProcessor.REASON_NON_RETRYABLE, "simulated");
        store.insertIfAbsent(record);

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger inProgress = new AtomicInteger();
        ConcurrentLinkedQueue<String> unexpected = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    ReplayResponse resp = svc.replayByKey("conc-replay");
                    switch (resp.result()) {
                        case "REPLAY_ACCEPTED" -> accepted.incrementAndGet();
                        case "REPLAY_IN_PROGRESS" -> inProgress.incrementAndGet();
                        default -> unexpected.add(resp.result());
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
        assertEquals(1, accepted.get(), "并发重放只应受理一次");
        assertEquals(threads - 1, inProgress.get(), "其余并发请求应识别为在途");
        assertEquals(1, stub.submitted.get(), "事件只应入队一次，不重复处理");
        assertEquals(1, record.getReplayCount(), "重放计数只应增加一次");
        log.info("通过：accepted={}, inProgress={}, submitted={}, replayCount={}",
                accepted.get(), inProgress.get(), stub.submitted.get(), record.getReplayCount());
    }

    @Test
    @DisplayName("[重复重放] 已重放成功的事件再次重放被识别为无需重放，不重复处理不计数")
    void replayOfAlreadySucceededIsNoop() {
        log.info("=== 场景：重放成功后再次重放幂等跳过 ===");
        EventRecord record = ingestToDeadLetter("replay-noop", "flaky");
        replayService.replayByKey("replay-noop");
        awaitStatus(record, EventStatus.SUCCESS, Duration.ofSeconds(5));
        int attemptsAfterSuccess = record.getAttempts();

        ReplayResponse again = replayService.replayByKey("replay-noop");
        assertEquals("ALREADY_SUCCEEDED", again.result(), "已成功事件应识别为无需重放");
        assertEquals(1, record.getReplayCount(), "不重复计数");
        assertEquals(attemptsAfterSuccess, record.getAttempts(), "不重复处理");
        assertEquals("SUCCESS", record.getLastReplayResult());
        log.info("通过：result={}, replayCount={}, attempts={}",
                again.result(), record.getReplayCount(), record.getAttempts());
    }

    @Test
    @DisplayName("[容量拒绝] 队列满时重放被拒，原失败记录不变，容量恢复后可再次重放")
    void replayRejectedWhenFullThenReplayableAgain() {
        log.info("=== 场景：容量不足拒绝重放且原状态不变 ===");
        StubProcessor stub = new StubProcessor();
        stub.capacity = 0; // 模拟处理能力不足
        DefaultReplayService svc = new DefaultReplayService(props, store, stub, metrics);
        EventRecord record = new EventRecord("replay-full", "p", "retryable");
        record.markAttempt();
        record.markDeadLetter(DefaultEventProcessor.REASON_MAX_RETRIES, "simulated transient downstream error");
        store.insertIfAbsent(record);
        String errorBefore = record.getLastError();

        ReplayResponse rejected = svc.replayByKey("replay-full");
        assertEquals("REJECTED", rejected.result(), "容量不足应明确拒绝");

        // 原失败记录保持不变，且没有停在中间状态
        assertEquals(EventStatus.DEAD_LETTER, record.getStatus());
        assertEquals(DefaultEventProcessor.REASON_MAX_RETRIES, record.getFailureReason());
        assertEquals(errorBefore, record.getLastError());
        assertEquals(0, record.getReplayCount(), "被拒的重放不计数");
        assertEquals("NONE", record.getLastReplayResult());
        assertFalse(record.isReplayInFlight(), "守卫必须释放，不能卡在重放中");

        // 容量恢复后同一事件可再次重放
        stub.capacity = 10;
        ReplayResponse accepted = svc.replayByKey("replay-full");
        assertEquals("REPLAY_ACCEPTED", accepted.result());
        assertEquals(1, record.getReplayCount());
        assertEquals(1, stub.submitted.get());
        log.info("通过：拒绝时状态={} replayCount={}，恢复后 result={} replayCount={}",
                EventStatus.DEAD_LETTER, 0, accepted.result(), record.getReplayCount());
    }

    @Test
    @DisplayName("[重放失败] 重放重试耗尽回到死信，保留首次失败原因，可再次重放")
    void failedReplayReturnsToDeadLetterAndReplayableAgain() {
        log.info("=== 场景：重放失败回到死信可再重放 ===");
        // retryable 恒可重试失败；重放专属 replayMaxRetries=2，与首次接入 maxRetries=0 独立
        EventRecord record = ingestToDeadLetter("replay-fail", "retryable");
        assertEquals(1, record.getAttempts(), "首次接入不重试(maxRetries=0)");

        replayService.replayByKey("replay-fail");
        awaitStatus(record, EventStatus.DEAD_LETTER, Duration.ofSeconds(5));

        assertEquals("FAILED", record.getLastReplayResult());
        assertEquals(1, record.getReplayCount());
        assertEquals(3, record.getReplayAttempts(), "重放首次 + replayMaxRetries=2 次重试");
        assertEquals(DefaultEventProcessor.REASON_MAX_RETRIES, record.getFailureReason(),
                "首次失败原因保留，可按原原因再次批量重放");
        assertNotNull(record.getLastReplayError());
        assertFalse(record.isReplayInFlight(), "不能停在既没成功也没法再重放的中间状态");

        // 再次重放仍然可以受理
        ReplayResponse second = replayService.replayByKey("replay-fail");
        assertEquals("REPLAY_ACCEPTED", second.result());
        awaitStatus(record, EventStatus.DEAD_LETTER, Duration.ofSeconds(5));
        assertEquals(2, record.getReplayCount());
        assertEquals(EventStatus.DEAD_LETTER, record.getStatus());
        log.info("通过：replayCount={}, replayAttempts/轮={}, lastReplayResult={}, failureReason={}",
                record.getReplayCount(), 3, record.getLastReplayResult(), record.getFailureReason());
    }

    @Test
    @DisplayName("[批量重放] 按失败原因批量重放，逐条独立互不影响")
    void batchReplayByReason() {
        log.info("=== 场景：按失败原因批量重放 ===");
        EventRecord r1 = ingestToDeadLetter("batch-retry-1", "retryable");
        EventRecord r2 = ingestToDeadLetter("batch-retry-2", "retryable");
        EventRecord fatal = ingestToDeadLetter("batch-fatal", "fatal");

        // 只重放 MAX_RETRIES_EXCEEDED：fatal(NON_RETRYABLE) 不应被拉起
        BatchReplayResponse resp = replayService.replayByReason(
                DefaultEventProcessor.REASON_MAX_RETRIES);
        assertEquals(2, resp.candidates());
        assertEquals(2, resp.replayAccepted());
        assertEquals(0, resp.rejected());
        assertFalse(resp.truncated());
        assertEquals(EventStatus.DEAD_LETTER, fatal.getStatus(), "其他原因的失败记录不受影响");
        assertEquals(0, fatal.getReplayCount());

        awaitStatus(r1, EventStatus.DEAD_LETTER, Duration.ofSeconds(5));
        awaitStatus(r2, EventStatus.DEAD_LETTER, Duration.ofSeconds(5));
        assertEquals(1, r1.getReplayCount());
        assertEquals(1, r2.getReplayCount());

        // 重放全部（reason=null）：此时 r1/r2 已回到死信，fatal 也可重放，逐条独立受理
        BatchReplayResponse all = replayService.replayByReason(null);
        assertEquals(3, all.candidates());
        assertEquals(3, all.replayAccepted());
        awaitStatus(fatal, EventStatus.DEAD_LETTER, Duration.ofSeconds(5));
        assertEquals(1, fatal.getReplayCount());
        assertEquals("FAILED", fatal.getLastReplayResult(), "fatal 重放仍失败但流程正常终态");
        log.info("通过：reason 过滤命中 {} 条，全量重放命中 {} 条，fatal.replayCount={}",
                resp.candidates(), all.candidates(), fatal.getReplayCount());
    }

    @Test
    @DisplayName("[重放校验] 未知键 NOT_FOUND，处理中事件 NOT_REPLAYABLE")
    void replayValidation() {
        log.info("=== 场景：重放请求校验 ===");
        ReplayResponse missing = replayService.replayByKey("no-such-key");
        assertEquals("NOT_FOUND", missing.result());

        // 处理中（非死信）事件不可重放
        EventRecord processing = new EventRecord("still-running", "p", "none");
        store.insertIfAbsent(processing);
        ReplayResponse notReplayable = replayService.replayByKey("still-running");
        assertEquals("NOT_REPLAYABLE", notReplayable.result());
        assertEquals(0, processing.getReplayCount());
        assertEquals(EventStatus.RECEIVED, processing.getStatus(), "不可重放判定不得改动原状态");
        log.info("通过：未知键={}, 处理中事件={}", missing.result(), notReplayable.result());
    }

    @Test
    @DisplayName("[重放限流] 重放入口超限快速拒绝，压力下延迟有界")
    void replayRateLimited() {
        log.info("=== 场景：重放入口限流 ===");
        props.setReplayRateLimitPerSecond(1);
        DefaultReplayService limited = new DefaultReplayService(props, store, processor, metrics);

        ReplayResponse first = limited.replayByKey("any-key");
        assertEquals("NOT_FOUND", first.result(), "首个请求消耗令牌");
        assertThrows(RateLimitExceededException.class,
                () -> limited.replayByKey("any-key"), "超限应快速拒绝而非排队等待");
        assertThrows(RateLimitExceededException.class,
                () -> limited.replayByReason(null));
        log.info("通过：重放限流触发快速失败");
    }
}
