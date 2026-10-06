package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventMessage;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.metrics.GatewayMetrics;
import com.github.feng111627.resilienteventingestiongatewayjava.service.IngestionService;
import com.github.feng111627.resilienteventingestiongatewayjava.store.EventStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 覆盖业务：失败重试与隔离 —— 可重试异常按策略重试，重试耗尽与不可重试异常进死信，
 * 失败事件不拖住同批后续正常事件，死信可按失败原因查询。
 */
@SpringBootTest
class FailureIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(FailureIsolationTest.class);

    @Autowired
    IngestionService ingestionService;
    @Autowired
    EventStore store;
    @Autowired
    GatewayMetrics metrics;

    @Test
    void retryableFailureEventuallySucceeds() {
        log.info("=== 用例：可重试失败重试后成功 ===");
        ingestionService.ingestBatch(List.of(
                new EventMessage("fail-retry-ok", "ORDER", "RETRYABLE:2")));
        ingestionService.awaitQuiescence(10_000);

        EventRecord record = store.findByKey("fail-retry-ok").orElseThrow();
        log.info("记录: {}", record);
        assertThat(record.getStatus()).isEqualTo(EventStatus.SUCCESS);
        assertThat(record.getAttempts()).isEqualTo(3);
        log.info("=== 用例通过：第{}次尝试成功 ===", record.getAttempts());
    }

    @Test
    void nonRetryableFailureGoesStraightToDeadLetter() {
        log.info("=== 用例：不可重试失败直接进死信 ===");
        ingestionService.ingestBatch(List.of(
                new EventMessage("fail-fatal", "ORDER", "FATAL bad data")));
        ingestionService.awaitQuiescence(10_000);

        EventRecord record = store.findByKey("fail-fatal").orElseThrow();
        log.info("记录: {}", record);
        assertThat(record.getStatus()).isEqualTo(EventStatus.DEAD_LETTER);
        assertThat(record.getAttempts()).isEqualTo(1);
        assertThat(record.getFailureReason()).contains("FATAL");
        assertThat(store.findByFailureReason(record.getFailureReason())).isNotEmpty();
        log.info("=== 用例通过：不可重试异常未重试直接隔离 ===");
    }

    @Test
    void retryExhaustionGoesToDeadLetter() {
        log.info("=== 用例：重试耗尽进死信 ===");
        ingestionService.ingestBatch(List.of(
                new EventMessage("fail-always", "ORDER", "RETRYABLE")));
        ingestionService.awaitQuiescence(10_000);

        EventRecord record = store.findByKey("fail-always").orElseThrow();
        log.info("记录: {}", record);
        assertThat(record.getStatus()).isEqualTo(EventStatus.DEAD_LETTER);
        assertThat(record.getAttempts()).isEqualTo(3);
        log.info("=== 用例通过：{}次尝试用尽后隔离 ===", record.getAttempts());
    }

    @Test
    void failingEventsDoNotBlockSubsequentOnes() {
        log.info("=== 用例：失败事件不拖住后续正常事件 ===");
        List<EventMessage> batch = new ArrayList<>();
        batch.add(new EventMessage("iso-fatal-1", "ORDER", "FATAL"));
        batch.add(new EventMessage("iso-retry-1", "ORDER", "RETRYABLE"));
        for (int i = 0; i < 20; i++) {
            batch.add(new EventMessage("iso-ok-" + i, "ORDER", "normal"));
        }
        long start = System.currentTimeMillis();
        ingestionService.ingestBatch(batch);
        ingestionService.awaitQuiescence(30_000);
        long elapsed = System.currentTimeMillis() - start;

        long okCount = store.findByStatus(EventStatus.SUCCESS).stream()
                .filter(r -> r.getMessage().dedupKey().startsWith("iso-ok-")).count();
        log.info("正常事件成功数={} 总耗时={}ms 指标={}", okCount, elapsed, metrics.snapshot());
        assertThat(okCount).isEqualTo(20);
        assertThat(store.findByKey("iso-fatal-1").orElseThrow().getStatus())
                .isEqualTo(EventStatus.DEAD_LETTER);
        assertThat(store.findByKey("iso-retry-1").orElseThrow().getStatus())
                .isEqualTo(EventStatus.DEAD_LETTER);
        log.info("=== 用例通过：20 个正常事件全部成功，失败事件已隔离 ===");
    }
}
