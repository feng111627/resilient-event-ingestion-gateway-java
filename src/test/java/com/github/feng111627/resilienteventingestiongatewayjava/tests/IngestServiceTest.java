package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRequest;
import com.github.feng111627.resilienteventingestiongatewayjava.service.DefaultIngestService;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventStore;
import com.github.feng111627.resilienteventingestiongatewayjava.service.GatewayMetrics;
import com.github.feng111627.resilienteventingestiongatewayjava.web.RateLimitExceededException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 接入服务单元测试：幂等去重、参数校验、容量拒绝、限流。
 */
class IngestServiceTest {

    private static final Logger log = LoggerFactory.getLogger(IngestServiceTest.class);

    private GatewayProperties props;
    private EventStore store;
    private GatewayMetrics metrics;
    private StubProcessor processor;
    private DefaultIngestService service;

    /** 可控的处理器桩：capacity<=0 时模拟队列满 */
    static class StubProcessor implements EventProcessor {
        final AtomicInteger submitted = new AtomicInteger();
        int capacity = 10;

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
        props.setRateLimitPerSecond(1000);
        store = new EventStore();
        metrics = new GatewayMetrics();
        processor = new StubProcessor();
        service = new DefaultIngestService(props, store, processor, metrics);
    }

    @Test
    @DisplayName("[重复投递] 同批内重复 dedupKey 只受理一次，其余标记 DUPLICATE")
    void duplicateWithinSameBatch() {
        log.info("=== 场景：同批内重复投递幂等 ===");
        BatchIngestResponse resp = service.ingest(List.of(
                new EventRequest("k1", "p1", "none"),
                new EventRequest("k1", "p1", "none"),
                new EventRequest("k2", "p2", "none")));

        assertEquals(2, resp.accepted());
        assertEquals(1, resp.duplicates());
        assertEquals(2, processor.submitted.get(), "重复事件不得重复入队");
        assertEquals(2, store.size(), "重复事件不得重复计数");
        log.info("通过：accepted={}, duplicates={}, storeSize={}", resp.accepted(), resp.duplicates(), store.size());
    }

    @Test
    @DisplayName("[重复投递] 跨批次重复提交同一 dedupKey 幂等跳过")
    void duplicateAcrossBatches() {
        log.info("=== 场景：跨批次重复投递幂等 ===");
        BatchIngestResponse first = service.ingest(List.of(new EventRequest("k1", "p1", "none")));
        BatchIngestResponse second = service.ingest(List.of(new EventRequest("k1", "p1", "none")));

        assertEquals(1, first.accepted());
        assertEquals(0, second.accepted());
        assertEquals(1, second.duplicates());
        assertEquals(1, processor.submitted.get());
        log.info("通过：两批提交后仅受理 1 条");
    }

    @Test
    @DisplayName("[参数校验] 缺失 dedupKey 标记 INVALID，不影响同批其他事件")
    void invalidDedupKey() {
        log.info("=== 场景：非法事件部分失败不影响整批 ===");
        BatchIngestResponse resp = service.ingest(List.of(
                new EventRequest(null, "p", "none"),
                new EventRequest("ok", "p", "none")));

        assertEquals(1, resp.invalid());
        assertEquals(1, resp.accepted());
        log.info("通过：invalid={}, accepted={}", resp.invalid(), resp.accepted());
    }

    @Test
    @DisplayName("[容量拒绝] 队列满时明确 REJECTED 而不是堆积")
    void rejectWhenQueueFull() {
        log.info("=== 场景：队列容量不足时明确拒绝 ===");
        processor.capacity = 1;
        BatchIngestResponse resp = service.ingest(List.of(
                new EventRequest("k1", "p", "none"),
                new EventRequest("k2", "p", "none"),
                new EventRequest("k3", "p", "none")));

        assertEquals(1, resp.accepted());
        assertEquals(2, resp.rejected());
        assertEquals("REJECTED", resp.items().get(1).result());
        log.info("通过：accepted={}, rejected={}", resp.accepted(), resp.rejected());
    }

    @Test
    @DisplayName("[限流] 超过速率限制的请求快速拒绝")
    void rateLimitRejects() {
        log.info("=== 场景：限流触发快速失败 ===");
        props.setRateLimitPerSecond(1);
        service = new DefaultIngestService(props, store, processor, metrics);

        service.ingest(List.of(new EventRequest("k1", "p", "none")));
        RateLimitExceededException ex = assertThrows(RateLimitExceededException.class,
                () -> service.ingest(List.of(new EventRequest("k2", "p", "none"))));
        log.info("通过：限流异常信息={}", ex.getMessage());
    }

    @Test
    @DisplayName("[参数校验] 空批次与超大批次被拒绝")
    void batchSizeValidation() {
        log.info("=== 场景：批量大小校验 ===");
        assertThrows(IllegalArgumentException.class, () -> service.ingest(List.of()));
        props.setMaxBatchSize(2);
        assertThrows(IllegalArgumentException.class, () -> service.ingest(List.of(
                new EventRequest("a", "p", "none"),
                new EventRequest("b", "p", "none"),
                new EventRequest("c", "p", "none"))));
        log.info("通过：空批与超大批均被拒绝");
    }
}
