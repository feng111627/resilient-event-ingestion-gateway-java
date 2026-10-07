package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRequest;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayBatchResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayItemResult;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayStatusView;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重放端到端集成测试：真实 HTTP 重放入口 + 异步处理 + 重放状态查询。
 * 覆盖：重放成功、重放重试次数独立于首次接入、重放耗尽可能再次重放、
 * 已成功事件无需重放、按失败原因批量重放且条目互不影响。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "gateway.queue-capacity=512",
        "gateway.worker-threads=4",
        "gateway.max-retries=3",
        "gateway.retry-backoff-millis=10",
        "gateway.rate-limit-per-second=10000",
        "gateway.max-batch-size=500",
        "gateway.replay-max-retries=2",
        "gateway.replay-rate-limit-per-second=10000",
        "gateway.replay-max-batch-size=1000"
})
class ReplayIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayIntegrationTest.class);

    @Autowired
    private TestRestTemplate rest;

    private void ingest(EventRequest... events) {
        ResponseEntity<BatchIngestResponse> resp = rest.postForEntity(
                "/api/events/batch", List.of(events), BatchIngestResponse.class);
        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals(events.length, resp.getBody().accepted());
    }

    private EventRecord awaitEvent(String key, EventStatus expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ResponseEntity<EventRecord> resp = rest.getForEntity("/api/events/" + key, EventRecord.class);
            if (resp.getStatusCode() == HttpStatus.OK && resp.getBody() != null
                    && resp.getBody().getStatus() == expected) {
                return resp.getBody();
            }
            sleep(10);
        }
        fail("timeout awaiting " + expected + " for " + key);
        return null;
    }

    private ReplayStatusView replayStatus(String key) {
        ResponseEntity<ReplayStatusView> resp = rest.getForEntity(
                "/api/replay/events/" + key, ReplayStatusView.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        return resp.getBody();
    }

    /** 等待第 expectedReplayCount 轮重放彻底结束（回到死信且不在重放中），避免读到重放前的旧状态 */
    private EventRecord awaitReplaySettled(String key, int expectedReplayCount, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ResponseEntity<EventRecord> resp = rest.getForEntity("/api/events/" + key, EventRecord.class);
            if (resp.getStatusCode() == HttpStatus.OK && resp.getBody() != null) {
                EventRecord r = resp.getBody();
                if (r.getReplayCount() >= expectedReplayCount
                        && r.getStatus() == EventStatus.DEAD_LETTER
                        && !r.isReplayInProgress()) {
                    return r;
                }
            }
            sleep(10);
        }
        fail("timeout awaiting replay round " + expectedReplayCount + " settled for " + key);
        return null;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("interrupted");
        }
    }

    @Test
    @DisplayName("[重放成功] 下游恢复后单条重放死信事件，最终成功且重放状态可查")
    void replaySingleDeadLetterToSuccess() {
        log.info("=== 场景：单条重放成功 ===");
        // recoverable：首次接入期间恒失败（模拟下游抖动），重放时成功（模拟下游恢复）
        ingest(new EventRequest("rp-ok-1", "p", "recoverable"));
        EventRecord dead = awaitEvent("rp-ok-1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));
        assertEquals(4, dead.getAttempts(), "首次接入：1 + maxRetries(3)");
        assertEquals(0, dead.getReplayCount());

        ResponseEntity<ReplayItemResult> replayResp = rest.postForEntity(
                "/api/replay/events/rp-ok-1", null, ReplayItemResult.class);
        assertEquals(HttpStatus.ACCEPTED, replayResp.getStatusCode());
        assertNotNull(replayResp.getBody());
        assertEquals("ACCEPTED", replayResp.getBody().result());

        EventRecord succeeded = awaitEvent("rp-ok-1", EventStatus.SUCCESS, Duration.ofSeconds(10));
        assertEquals(5, succeeded.getAttempts(), "重放只新增 1 次尝试");
        assertEquals(1, succeeded.getReplayCount());
        assertEquals("SUCCESS", succeeded.getLastReplayResult());

        ReplayStatusView view = replayStatus("rp-ok-1");
        assertTrue(view.replayed());
        assertEquals(1, view.replayCount());
        assertEquals("SUCCESS", view.lastReplayResult());
        assertFalse(view.replayInProgress());
        assertEquals("SUCCESS", view.status());
        log.info("通过：attempts={}, replayCount={}, lastReplayResult={}",
                succeeded.getAttempts(), view.replayCount(), view.lastReplayResult());
    }

    @Test
    @DisplayName("[重放重试] 重放使用独立重试次数，耗尽后回到死信且可再次重放，不停留中间态")
    void replayUsesOwnRetryBudgetAndCanReplayAgain() {
        log.info("=== 场景：重放重试次数独立，耗尽可再重放 ===");
        ingest(new EventRequest("rp-again-1", "p", "retryable"));
        awaitEvent("rp-again-1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));

        // 第一次重放：retryable 恒失败，耗尽 replayMaxRetries(2) 后回到死信
        rest.postForEntity("/api/replay/events/rp-again-1", null, ReplayItemResult.class);
        // 等第一次重放轮次彻底结束（replayCount=1 且回到死信），避免读到重放前的旧死信状态
        EventRecord dead1 = awaitReplaySettled("rp-again-1", 1, Duration.ofSeconds(10));
        assertEquals(4 + 3, dead1.getAttempts(), "重放轮次：1 + replayMaxRetries(2)");
        assertEquals(1, dead1.getReplayCount());
        assertEquals("FAILED", dead1.getLastReplayResult());
        assertFalse(dead1.isReplayInProgress(), "不得卡在重放中的中间状态");

        // 再次重放仍然可行
        ResponseEntity<ReplayItemResult> second = rest.postForEntity(
                "/api/replay/events/rp-again-1", null, ReplayItemResult.class);
        assertEquals(HttpStatus.ACCEPTED, second.getStatusCode());
        // 等第二次重放轮次彻底结束
        EventRecord dead2 = awaitReplaySettled("rp-again-1", 2, Duration.ofSeconds(10));
        assertEquals(10, dead2.getAttempts());
        assertEquals(2, dead2.getReplayCount());
        assertEquals("FAILED", dead2.getLastReplayResult());
        assertEquals(EventStatus.DEAD_LETTER, dead2.getStatus());
        log.info("通过：attempts={}, replayCount={}, lastReplayResult={}",
                dead2.getAttempts(), dead2.getReplayCount(), dead2.getLastReplayResult());
    }

    @Test
    @DisplayName("[无需重放] 重放已成功事件返回 ALREADY_SUCCEEDED，不重复处理不重复计数")
    void replaySucceededEventIsNoOp() {
        log.info("=== 场景：已成功事件重放为幂等空操作 ===");
        ingest(new EventRequest("rp-noop-1", "p", "none"));
        EventRecord ok = awaitEvent("rp-noop-1", EventStatus.SUCCESS, Duration.ofSeconds(10));
        assertEquals(1, ok.getAttempts());

        ResponseEntity<ReplayItemResult> resp = rest.postForEntity(
                "/api/replay/events/rp-noop-1", null, ReplayItemResult.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals("ALREADY_SUCCEEDED", resp.getBody().result());

        EventRecord after = rest.getForEntity("/api/events/rp-noop-1", EventRecord.class).getBody();
        assertNotNull(after);
        assertEquals(1, after.getAttempts(), "不得重复处理");
        assertEquals(0, after.getReplayCount(), "不得重复计数");
        log.info("通过：result={}, attempts={}, replayCount={}",
                resp.getBody().result(), after.getAttempts(), after.getReplayCount());
    }

    @Test
    @DisplayName("[批量重放] 按失败原因批量重放，命中的恢复成功，未命中的死信不受影响")
    void batchReplayByReason() {
        log.info("=== 场景：按失败原因批量重放 ===");
        ingest(new EventRequest("rb-rec-1", "p", "recoverable"),
                new EventRequest("rb-rec-2", "p", "recoverable"),
                new EventRequest("rb-fatal-1", "p", "fatal"));
        awaitEvent("rb-rec-1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));
        awaitEvent("rb-rec-2", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));
        awaitEvent("rb-fatal-1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));

        // 只重放 MAX_RETRIES_EXCEEDED 的死信（两条 recoverable）
        ResponseEntity<ReplayBatchResponse> resp = rest.postForEntity(
                "/api/replay/failures?reason=MAX_RETRIES_EXCEEDED", null, ReplayBatchResponse.class);
        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        ReplayBatchResponse body = resp.getBody();
        assertNotNull(body);
        assertTrue(body.total() >= 2);
        assertTrue(body.accepted() >= 2);
        assertTrue(body.items().stream().anyMatch(i -> i.dedupKey().equals("rb-rec-1")
                && i.result().equals("ACCEPTED")));
        assertTrue(body.items().stream().anyMatch(i -> i.dedupKey().equals("rb-rec-2")
                && i.result().equals("ACCEPTED")));

        awaitEvent("rb-rec-1", EventStatus.SUCCESS, Duration.ofSeconds(10));
        awaitEvent("rb-rec-2", EventStatus.SUCCESS, Duration.ofSeconds(10));

        // 未命中原因的 fatal 死信不受影响，仍为死信且未重放过
        EventRecord fatal = rest.getForEntity("/api/events/rb-fatal-1", EventRecord.class).getBody();
        assertNotNull(fatal);
        assertEquals(EventStatus.DEAD_LETTER, fatal.getStatus());
        assertEquals("NON_RETRYABLE", fatal.getFailureReason());
        assertEquals(0, fatal.getReplayCount());

        // fatal 死信可单独按原因重放：不可重试异常再次失败回到死信，不停留中间态
        ResponseEntity<ReplayBatchResponse> resp2 = rest.postForEntity(
                "/api/replay/failures?reason=NON_RETRYABLE", null, ReplayBatchResponse.class);
        assertEquals(HttpStatus.ACCEPTED, resp2.getStatusCode());
        assertNotNull(resp2.getBody());
        assertTrue(resp2.getBody().accepted() >= 1);
        EventRecord fatalAfter = awaitReplaySettled("rb-fatal-1", 1, Duration.ofSeconds(10));
        assertEquals(1, fatalAfter.getReplayCount());
        assertEquals("FAILED", fatalAfter.getLastReplayResult());
        assertEquals(EventStatus.DEAD_LETTER, fatalAfter.getStatus());
        log.info("通过：批量受理={}, rb-rec-* 成功, rb-fatal-1 replayCount={}, lastReplayResult={}",
                body.accepted(), fatalAfter.getReplayCount(), fatalAfter.getLastReplayResult());
    }

    @Test
    @DisplayName("[状态查询] 重放状态端点未知键返回 404")
    void replayStatusNotFound() {
        log.info("=== 场景：重放状态查询 404 ===");
        ResponseEntity<ReplayStatusView> resp = rest.getForEntity(
                "/api/replay/events/no-such-key", ReplayStatusView.class);
        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
        log.info("通过：未知键重放状态返回 404");
    }
}
