package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRequest;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.model.GatewayMetricsSnapshot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 端到端集成测试：真实 HTTP 接入 + 异步处理 + 状态查询 + 指标。
 * 覆盖：并发接收、重复投递幂等、失败隔离与死信查询、指标可观测。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "gateway.queue-capacity=512",
        "gateway.worker-threads=4",
        "gateway.max-retries=3",
        "gateway.retry-backoff-millis=10",
        "gateway.rate-limit-per-second=10000",
        "gateway.max-batch-size=500"
})
class GatewayIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(GatewayIntegrationTest.class);

    @Autowired
    private TestRestTemplate rest;

    private BatchIngestResponse postBatch(List<EventRequest> events) {
        ResponseEntity<BatchIngestResponse> resp = rest.postForEntity(
                "/api/events/batch", events, BatchIngestResponse.class);
        assertTrue(resp.getStatusCode() == HttpStatus.ACCEPTED
                || resp.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS);
        return resp.getBody();
    }

    private EventRecord awaitEvent(String key, EventStatus expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ResponseEntity<EventRecord> resp = rest.getForEntity("/api/events/" + key, EventRecord.class);
            if (resp.getStatusCode() == HttpStatus.OK && resp.getBody() != null
                    && resp.getBody().getStatus() == expected) {
                return resp.getBody();
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("timeout awaiting " + expected + " for " + key);
        return null;
    }

    @Test
    @DisplayName("[并发接收] 多线程并发提交不同键，全部受理且处理成功，无丢失")
    void concurrentIngestDistinctKeys() throws InterruptedException {
        log.info("=== 场景：并发接收不同键 ===");
        int threads = 8, perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger acceptedSum = new AtomicInteger();
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    List<EventRequest> batch = new ArrayList<>();
                    for (int i = 0; i < perThread; i++) {
                        batch.add(new EventRequest("conc-" + tid + "-" + i, "p", "none"));
                    }
                    BatchIngestResponse resp = postBatch(batch);
                    acceptedSum.addAndGet(resp.accepted());
                } catch (Exception e) {
                    errors.add(e.toString());
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        assertTrue(errors.isEmpty(), "并发请求不应出错: " + errors);
        assertEquals(threads * perThread, acceptedSum.get(), "所有不同键都应被受理");

        EventRecord sample = awaitEvent("conc-0-0", EventStatus.SUCCESS, Duration.ofSeconds(10));
        assertNotNull(sample);
        log.info("通过：并发受理 {} 条，样例事件状态={}", acceptedSum.get(), sample.getStatus());
    }

    @Test
    @DisplayName("[重复投递] 同一批事件原样重投两次，第二次全部幂等去重，不重复计数")
    void duplicateRedeliveryIsIdempotent() {
        log.info("=== 场景：上游重复投递同一批 ===");
        List<EventRequest> batch = List.of(
                new EventRequest("dup-1", "p1", "none"),
                new EventRequest("dup-2", "p2", "none"),
                new EventRequest("dup-3", "p3", "none"));

        BatchIngestResponse first = postBatch(batch);
        BatchIngestResponse second = postBatch(batch);
        BatchIngestResponse third = postBatch(batch);

        assertEquals(3, first.accepted());
        assertEquals(3, second.duplicates());
        assertEquals(0, second.accepted());
        assertEquals(3, third.duplicates());

        EventRecord r = awaitEvent("dup-1", EventStatus.SUCCESS, Duration.ofSeconds(10));
        assertEquals(1, r.getAttempts(), "重复投递不得导致重复处理");
        log.info("通过：三次投递仅处理一次，attempts={}", r.getAttempts());
    }

    @Test
    @DisplayName("[失败隔离] 混合批次中失败事件进入死信可查询，正常事件全部成功")
    void mixedBatchFailureIsolation() {
        log.info("=== 场景：混合批次失败隔离 ===");
        List<EventRequest> batch = List.of(
                new EventRequest("mix-good-1", "p", "none"),
                new EventRequest("mix-fatal-1", "p", "fatal"),
                new EventRequest("mix-good-2", "p", "none"),
                new EventRequest("mix-flaky-1", "p", "flaky"),
                new EventRequest("mix-retry-1", "p", "retryable"));

        BatchIngestResponse resp = postBatch(batch);
        assertEquals(5, resp.accepted());

        awaitEvent("mix-good-1", EventStatus.SUCCESS, Duration.ofSeconds(10));
        awaitEvent("mix-good-2", EventStatus.SUCCESS, Duration.ofSeconds(10));
        awaitEvent("mix-flaky-1", EventStatus.SUCCESS, Duration.ofSeconds(10));
        EventRecord fatal = awaitEvent("mix-fatal-1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));
        EventRecord retry = awaitEvent("mix-retry-1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));

        assertEquals("NON_RETRYABLE", fatal.getFailureReason());
        assertEquals("MAX_RETRIES_EXCEEDED", retry.getFailureReason());

        // 死信可按失败原因查询
        List<EventRecord> nonRetryable = rest.exchange(
                "/api/failures?reason=NON_RETRYABLE", HttpMethod.GET, null,
                new ParameterizedTypeReference<List<EventRecord>>() { }).getBody();
        assertNotNull(nonRetryable);
        assertTrue(nonRetryable.stream().anyMatch(e -> e.getDedupKey().equals("mix-fatal-1")));
        assertFalse(nonRetryable.stream().anyMatch(e -> e.getDedupKey().equals("mix-retry-1")));
        log.info("通过：fatal.reason={}, retry.reason={}, 死信查询命中={}",
                fatal.getFailureReason(), retry.getFailureReason(), nonRetryable.size());
    }

    @Test
    @DisplayName("[可观测] 指标端点返回队列深度、拒绝次数、处理耗时等")
    void metricsEndpoint() {
        log.info("=== 场景：指标可观测 ===");
        postBatch(List.of(new EventRequest("metric-1", "p", "none")));
        awaitEvent("metric-1", EventStatus.SUCCESS, Duration.ofSeconds(10));

        ResponseEntity<GatewayMetricsSnapshot> resp = rest.getForEntity(
                "/api/metrics", GatewayMetricsSnapshot.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        GatewayMetricsSnapshot m = resp.getBody();
        assertNotNull(m);
        assertTrue(m.totalReceived() > 0);
        assertTrue(m.totalSuccess() > 0);
        assertTrue(m.queueCapacity() > 0);
        assertTrue(m.queueDepth() >= 0);
        assertTrue(m.avgProcessingMillis() >= 0);
        log.info("通过：received={}, success={}, deadLetter={}, retries={}, queueDepth={}/{}, avgMs={}, maxMs={}",
                m.totalReceived(), m.totalSuccess(), m.totalDeadLetter(), m.totalRetries(),
                m.queueDepth(), m.queueCapacity(), m.avgProcessingMillis(), m.maxProcessingMillis());
    }

    @Test
    @DisplayName("[状态查询] 按状态过滤事件列表，未知键返回 404")
    void queryByStatusAndNotFound() {
        log.info("=== 场景：按状态查询与 404 ===");
        postBatch(List.of(new EventRequest("q-1", "p", "none")));
        awaitEvent("q-1", EventStatus.SUCCESS, Duration.ofSeconds(10));

        List<EventRecord> succeeded = rest.exchange(
                "/api/events?status=SUCCESS", HttpMethod.GET, null,
                new ParameterizedTypeReference<List<EventRecord>>() { }).getBody();
        assertNotNull(succeeded);
        assertTrue(succeeded.stream().anyMatch(e -> e.getDedupKey().equals("q-1")));

        ResponseEntity<EventRecord> missing = rest.getForEntity("/api/events/no-such-key", EventRecord.class);
        assertEquals(HttpStatus.NOT_FOUND, missing.getStatusCode());
        log.info("通过：SUCCESS 列表包含 q-1，未知键返回 404");
    }
}
