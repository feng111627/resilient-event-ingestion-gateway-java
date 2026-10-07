package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchReplayResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRequest;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayStatusResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重放端到端集成测试：HTTP 接入制造死信 -> 单条/批量重放 -> 重放轨迹查询。
 * max-retries=0 让首次接入快速进死信；replay-max-retries=2 验证重放专属重试。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "gateway.queue-capacity=256",
        "gateway.worker-threads=2",
        "gateway.max-retries=0",
        "gateway.replay-max-retries=2",
        "gateway.retry-backoff-millis=10",
        "gateway.rate-limit-per-second=10000",
        "gateway.replay-rate-limit-per-second=10000",
        "gateway.max-batch-size=500"
})
class ReplayIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayIntegrationTest.class);

    @Autowired
    private TestRestTemplate rest;

    private void ingest(List<EventRequest> events) {
        ResponseEntity<String> resp = rest.postForEntity("/api/events/batch", events, String.class);
        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
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
    @DisplayName("[重放成功-HTTP] 死信事件单条重放成功，重放轨迹端点可查")
    void singleReplayEndToEnd() {
        log.info("=== 场景：HTTP 单条重放成功 ===");
        // flaky 首次接入(maxRetries=0)失败进死信；重放第 2 次尝试成功
        ingest(List.of(new EventRequest("http-replay-1", "p", "flaky")));
        awaitEvent("http-replay-1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));

        ResponseEntity<ReplayResponse> replay = rest.postForEntity(
                "/api/replays/http-replay-1", null, ReplayResponse.class);
        assertEquals(HttpStatus.ACCEPTED, replay.getStatusCode());
        assertNotNull(replay.getBody());
        assertEquals("REPLAY_ACCEPTED", replay.getBody().result());

        EventRecord done = awaitEvent("http-replay-1", EventStatus.SUCCESS, Duration.ofSeconds(10));
        assertEquals(1, done.getReplayCount());
        assertEquals("SUCCESS", done.getLastReplayResult());

        // 重放轨迹查询端点
        ResponseEntity<ReplayStatusResponse> status = rest.getForEntity(
                "/api/replays/http-replay-1", ReplayStatusResponse.class);
        assertEquals(HttpStatus.OK, status.getStatusCode());
        assertNotNull(status.getBody());
        assertEquals(1, status.getBody().replayCount());
        assertEquals("SUCCESS", status.getBody().lastReplayResult());
        assertFalse(status.getBody().replayInFlight());

        // 已成功事件再次重放：幂等跳过
        ResponseEntity<ReplayResponse> again = rest.postForEntity(
                "/api/replays/http-replay-1", null, ReplayResponse.class);
        assertEquals(HttpStatus.ACCEPTED, again.getStatusCode());
        assertNotNull(again.getBody());
        assertEquals("ALREADY_SUCCEEDED", again.getBody().result());
        log.info("通过：replayCount={}, lastReplayResult={}, 重复重放={}",
                status.getBody().replayCount(), status.getBody().lastReplayResult(),
                again.getBody().result());
    }

    @Test
    @DisplayName("[批量重放-HTTP] 按失败原因批量重放，未知键 404")
    void batchReplayEndToEnd() {
        log.info("=== 场景：HTTP 按原因批量重放 ===");
        ingest(List.of(
                new EventRequest("http-batch-r1", "p", "retryable"),
                new EventRequest("http-batch-r2", "p", "retryable"),
                new EventRequest("http-batch-f1", "p", "fatal")));
        awaitEvent("http-batch-r1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));
        awaitEvent("http-batch-r2", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));
        awaitEvent("http-batch-f1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<BatchReplayResponse> resp = rest.exchange(
                "/api/replays/batch", HttpMethod.POST,
                new HttpEntity<>(Map.of("reason", "MAX_RETRIES_EXCEEDED"), headers),
                BatchReplayResponse.class);
        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertTrue(resp.getBody().candidates() >= 2);
        assertTrue(resp.getBody().items().stream()
                .allMatch(i -> !i.dedupKey().equals("http-batch-f1")),
                "NON_RETRYABLE 记录不应被本批拉起");

        // 两条 retryable 重放后仍失败回到死信（不是中间状态），可查询轨迹
        EventRecord r1 = awaitEvent("http-batch-r1", EventStatus.DEAD_LETTER, Duration.ofSeconds(10));
        assertEquals(1, r1.getReplayCount());
        assertEquals("FAILED", r1.getLastReplayResult());

        // 未知键重放 404，轨迹查询 404
        ResponseEntity<ReplayResponse> missing = rest.postForEntity(
                "/api/replays/no-such-key", null, ReplayResponse.class);
        assertEquals(HttpStatus.NOT_FOUND, missing.getStatusCode());
        ResponseEntity<String> missingStatus = rest.getForEntity(
                "/api/replays/no-such-key", String.class);
        assertEquals(HttpStatus.NOT_FOUND, missingStatus.getStatusCode());
        log.info("通过：批量重放 candidates={}, r1.replayCount={}, lastReplayResult={}",
                resp.getBody().candidates(), r1.getReplayCount(), r1.getLastReplayResult());
    }
}
