package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.domain.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventMessage;
import com.github.feng111627.resilienteventingestiongatewayjava.metrics.GatewayMetrics;
import com.github.feng111627.resilienteventingestiongatewayjava.service.IngestionService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 覆盖业务：容量保护 —— 队列容量不足时明确拒绝而非无限堆积，
 * 拒绝数计入指标，被拒绝的事件可查询不到占位记录（允许上游重试）。
 */
@SpringBootTest(properties = {
        "gateway.worker-threads=1",
        "gateway.queue-capacity=2"
})
class CapacityRejectionTest {

    private static final Logger log = LoggerFactory.getLogger(CapacityRejectionTest.class);

    @Autowired
    IngestionService ingestionService;
    @Autowired
    GatewayMetrics metrics;

    @Test
    void fullQueueRejectsInsteadOfPilingUp() {
        log.info("=== 用例：队列容量不足时明确拒绝 ===");
        // 每个事件处理 200ms，1 个线程 + 容量 2 的队列，瞬间涌入 20 个必然溢出
        List<EventMessage> flood = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            flood.add(new EventMessage("cap-key-" + i, "ORDER", "SLOW:200"));
        }
        BatchIngestResponse resp = ingestionService.ingestBatch(flood);
        log.info("受理结果: accepted={} rejected={} 指标={}",
                resp.accepted(), resp.rejected(), metrics.snapshot());

        assertThat(resp.accepted() + resp.rejected()).isEqualTo(20);
        assertThat(resp.rejected()).isGreaterThan(0);
        assertThat(resp.accepted()).isLessThanOrEqualTo(1 + 2); // 1 在执行 + 2 在队列
        assertThat((Long) metrics.snapshot().get("rejected")).isGreaterThan(0L);

        // 被拒绝的键不留下占位记录，上游重试可重新受理
        long acceptedKeys = resp.results().stream()
                .filter(r -> r.outcome().equals(BatchIngestResponse.ItemResult.ACCEPTED)).count();
        assertThat(acceptedKeys).isEqualTo(resp.accepted());

        ingestionService.awaitQuiescence(30_000);
        log.info("排空后指标: {}", metrics.snapshot());
        log.info("=== 用例通过：容量不足明确拒绝 {} 条，未无限堆积 ===", resp.rejected());
    }
}
