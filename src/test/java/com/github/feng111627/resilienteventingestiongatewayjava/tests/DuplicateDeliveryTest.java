package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.domain.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventMessage;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.processing.SimulatedEventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.service.IngestionService;
import com.github.feng111627.resilienteventingestiongatewayjava.store.EventStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 覆盖业务：重复投递幂等 —— 同批重复、跨批重复、乱序重复均只受理一次，
 * 业务效果只应用一次，不重复计数。
 */
@SpringBootTest
class DuplicateDeliveryTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicateDeliveryTest.class);

    @Autowired
    IngestionService ingestionService;
    @Autowired
    EventStore store;
    @Autowired
    SimulatedEventProcessor processor;

    @Test
    void duplicatesInSameBatchAreAcceptedOnce() {
        log.info("=== 用例：同批内重复去重 ===");
        List<EventMessage> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(new EventMessage("dup-batch-key", "ORDER", "payload-" + i));
        }
        BatchIngestResponse resp = ingestionService.ingestBatch(batch);
        ingestionService.awaitQuiescence(5000);

        log.info("受理结果: accepted={} duplicated={}", resp.accepted(), resp.duplicated());
        assertThat(resp.accepted()).isEqualTo(1);
        assertThat(resp.duplicated()).isEqualTo(4);
        assertThat(store.findByKey("dup-batch-key")).isPresent();
        assertThat(store.findByKey("dup-batch-key").get().getStatus()).isEqualTo(EventStatus.SUCCESS);
        assertThat(processor.appliedCount("dup-batch-key")).isEqualTo(1);
        log.info("=== 用例通过：同批重复只受理一次，业务效果应用 {} 次 ===",
                processor.appliedCount("dup-batch-key"));
    }

    @Test
    void duplicatesAcrossBatchesAreIdempotent() {
        log.info("=== 用例：跨批重复投递幂等 ===");
        List<EventMessage> keys = List.of(
                new EventMessage("dup-cross-1", "ORDER", "a"),
                new EventMessage("dup-cross-2", "ORDER", "b"),
                new EventMessage("dup-cross-3", "ORDER", "c"));
        BatchIngestResponse first = ingestionService.ingestBatch(keys);

        // 模拟上游重试：乱序 + 重复再次投递
        List<EventMessage> redelivered = new ArrayList<>(keys);
        Collections.reverse(redelivered);
        redelivered.add(keys.get(0));
        BatchIngestResponse second = ingestionService.ingestBatch(redelivered);
        ingestionService.awaitQuiescence(5000);

        log.info("首批 accepted={} 重投批 accepted={} duplicated={}",
                first.accepted(), second.accepted(), second.duplicated());
        assertThat(first.accepted()).isEqualTo(3);
        assertThat(second.accepted()).isZero();
        assertThat(second.duplicated()).isEqualTo(4);
        for (EventMessage k : keys) {
            assertThat(processor.appliedCount(k.dedupKey())).isEqualTo(1);
        }
        assertThat(store.findByStatus(EventStatus.SUCCESS)).hasSizeGreaterThanOrEqualTo(3);
        log.info("=== 用例通过：跨批乱序重投未造成重复计数 ===");
    }
}
