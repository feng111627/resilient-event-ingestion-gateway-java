package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.service.DefaultEventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventStore;
import com.github.feng111627.resilienteventingestiongatewayjava.service.GatewayMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 异步处理器测试：重试退避、死信隔离、可重试/不可重试分流、失败不拖住后续事件。
 */
class EventProcessorTest {

    private static final Logger log = LoggerFactory.getLogger(EventProcessorTest.class);

    private GatewayProperties props;
    private EventStore store;
    private GatewayMetrics metrics;
    private DefaultEventProcessor processor;

    @BeforeEach
    void setUp() {
        props = new GatewayProperties();
        props.setQueueCapacity(64);
        props.setWorkerThreads(2);
        props.setMaxRetries(3);
        props.setRetryBackoffMillis(10);
        store = new EventStore();
        metrics = new GatewayMetrics();
        processor = new DefaultEventProcessor(props, metrics);
        processor.start();
    }

    @AfterEach
    void tearDown() {
        processor.shutdown();
    }

    private EventRecord submit(String key, String failMode) {
        EventRecord record = new EventRecord(key, "payload-of-" + key, failMode);
        store.insertIfAbsent(record);
        assertTrue(processor.trySubmit(record));
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
                + ", actual=" + record.getStatus() + ", attempts=" + record.getAttempts());
    }

    @Test
    @DisplayName("[重试] flaky 事件前两次失败第三次成功，最终 SUCCESS")
    void flakyEventSucceedsAfterRetries() {
        log.info("=== 场景：可重试失败后重试成功 ===");
        EventRecord record = submit("flaky-1", "flaky");
        awaitStatus(record, EventStatus.SUCCESS, Duration.ofSeconds(5));

        assertEquals(3, record.getAttempts(), "flaky 模式应在第 3 次尝试成功");
        assertTrue(record.getProcessingMillis() >= 0);
        log.info("通过：attempts={}, processingMillis={}", record.getAttempts(), record.getProcessingMillis());
    }

    @Test
    @DisplayName("[死信] 恒可重试失败重试耗尽后进入 DEAD_LETTER，原因 MAX_RETRIES_EXCEEDED")
    void retryableExhaustionGoesToDeadLetter() {
        log.info("=== 场景：重试耗尽进入死信 ===");
        EventRecord record = submit("always-retryable", "retryable");
        awaitStatus(record, EventStatus.DEAD_LETTER, Duration.ofSeconds(5));

        assertEquals(props.getMaxRetries() + 1, record.getAttempts(), "首次 + maxRetries 次重试");
        assertEquals(DefaultEventProcessor.REASON_MAX_RETRIES, record.getFailureReason());
        assertNotNull(record.getLastError());
        log.info("通过：attempts={}, reason={}", record.getAttempts(), record.getFailureReason());
    }

    @Test
    @DisplayName("[死信] 不可重试异常不重试，直接 DEAD_LETTER，原因 NON_RETRYABLE")
    void nonRetryableGoesStraightToDeadLetter() {
        log.info("=== 场景：不可重试异常直接死信 ===");
        EventRecord record = submit("fatal-1", "fatal");
        awaitStatus(record, EventStatus.DEAD_LETTER, Duration.ofSeconds(5));

        assertEquals(1, record.getAttempts(), "不可重试异常只应尝试一次");
        assertEquals(DefaultEventProcessor.REASON_NON_RETRYABLE, record.getFailureReason());
        log.info("通过：attempts={}, reason={}", record.getAttempts(), record.getFailureReason());
    }

    @Test
    @DisplayName("[失败隔离] 死信事件不拖住后续正常事件，全部处理完成")
    void failureDoesNotBlockSubsequentEvents() {
        log.info("=== 场景：失败隔离 ===");
        List<EventRecord> records = new ArrayList<>();
        records.add(submit("bad-1", "fatal"));
        records.add(submit("bad-2", "retryable"));
        for (int i = 0; i < 20; i++) {
            records.add(submit("good-" + i, "none"));
        }

        for (EventRecord r : records) {
            awaitStatus(r, r.getFailMode().equals("none") ? EventStatus.SUCCESS : EventStatus.DEAD_LETTER,
                    Duration.ofSeconds(10));
        }
        long good = records.stream().filter(r -> r.getStatus() == EventStatus.SUCCESS).count();
        long dead = records.stream().filter(r -> r.getStatus() == EventStatus.DEAD_LETTER).count();
        assertEquals(20, good, "正常事件必须全部成功");
        assertEquals(2, dead, "失败事件必须全部进入死信且可查询");
        assertEquals(2, store.findDeadLettersByReason(null).size());
        log.info("通过：success={}, deadLetter={}", good, dead);
    }

    @Test
    @DisplayName("[容量] trySubmit 在队列满时返回 false，不阻塞调用方")
    void trySubmitRejectsWhenFull() {
        log.info("=== 场景：有界队列非阻塞拒绝 ===");
        // workerThreads=0：没有消费者，队列只进不出，可确定性验证容量拒绝语义
        GatewayProperties p2 = new GatewayProperties();
        p2.setQueueCapacity(4);
        p2.setWorkerThreads(0);
        DefaultEventProcessor full = new DefaultEventProcessor(p2, metrics);
        full.start();
        int accepted = 0;
        for (int i = 0; i < 10; i++) {
            if (full.trySubmit(new EventRecord("k" + i, "p", "retryable"))) {
                accepted++;
            }
        }
        full.shutdown();
        assertEquals(4, accepted, "容量为 4 的队列只应接受 4 条");
        assertEquals(4, full.getQueueDepth(), "未被消费的事件应保留在队列中不丢失");
        log.info("通过：capacity=4, accepted={}, queueDepth={}", accepted, 4);
    }
}
