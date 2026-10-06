package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.service.IngestionService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 覆盖业务：接入协议与状态查询 —— HTTP 批量接入、按去重键/状态/失败原因查询、
 * 死信查询与指标暴露。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ApiIntegrationTest.class);

    @Autowired
    TestRestTemplate rest;
    @Autowired
    IngestionService ingestionService;

    @Test
    void ingestThenQueryByKeyStatusAndReason() {
        log.info("=== 用例：HTTP 接入与多维查询 ===");
        List<Map<String, String>> batch = List.of(
                Map.of("dedupKey", "api-ok-1", "eventType", "ORDER", "payload", "normal"),
                Map.of("dedupKey", "api-fatal-1", "eventType", "ORDER", "payload", "FATAL"),
                Map.of("dedupKey", "api-ok-1", "eventType", "ORDER", "payload", "normal"));

        ResponseEntity<Map> resp = rest.postForEntity("/api/events/batch", batch, Map.class);
        log.info("接入响应: status={} body={}", resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat((Integer) resp.getBody().get("accepted")).isEqualTo(2);
        assertThat((Integer) resp.getBody().get("duplicated")).isEqualTo(1);

        ingestionService.awaitQuiescence(10_000);

        Map byKey = rest.getForObject("/api/events/api-ok-1", Map.class);
        log.info("按键查询: {}", byKey);
        assertThat(byKey.get("status")).isEqualTo("SUCCESS");

        List<?> deadLetters = rest.getForObject("/api/dead-letters", List.class);
        log.info("死信查询: {}", deadLetters);
        assertThat(deadLetters).anyMatch(r -> r.toString().contains("api-fatal-1"));

        String byReasonUri = org.springframework.web.util.UriComponentsBuilder
                .fromPath("/api/events")
                .queryParam("failureReason", "FATAL: 数据非法，不可重试")
                .build().toUriString();
        List<?> byReason = rest.getForObject(byReasonUri, List.class);
        log.info("按失败原因查询条数: {}", byReason.size());
        assertThat(byReason).isNotEmpty();

        Map metrics = rest.getForObject("/api/metrics", Map.class);
        log.info("指标: {}", metrics);
        assertThat((Integer) metrics.get("succeeded")).isGreaterThanOrEqualTo(1);
        assertThat((Integer) metrics.get("deadLettered")).isGreaterThanOrEqualTo(1);
        assertThat(metrics).containsKeys("queueDepth", "avgLatencyMillis", "rejected");
        log.info("=== 用例通过：接入、查询、死信、指标全链路正常 ===");
    }

    @Test
    void unknownKeyReturns404() {
        log.info("=== 用例：查询不存在的去重键 ===");
        ResponseEntity<Map> resp = rest.getForEntity("/api/events/no-such-key", Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        log.info("=== 用例通过：未知键返回 404 ===");
    }
}
