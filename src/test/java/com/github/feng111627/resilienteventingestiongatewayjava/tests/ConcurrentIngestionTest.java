package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.domain.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventMessage;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.metrics.GatewayMetrics;
import com.github.feng111627.resilienteventingestiongatewayjava.processing.SimulatedEventProcessor;
import com.github.feng111627.resilienteventingestiongatewayjava.service.IngestionService;
import com.github.feng111627.resilienteventingestiongatewayjava.store.EventStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 覆盖业务：并发接收 —— 多线程同时提交大量重叠批次，
 * 每个去重键只受理一次，受理+重复数守恒，不丢数据不重复计数。
 */
@SpringBootTest
class ConcurrentIngestionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestionTest.class);

    @Autowired
    IngestionService ingestionService;
    @Autowired
    EventStore store;
    @Autowired
    SimulatedEventProcessor processor;
    @Autowired
    GatewayMetrics metrics;

    @Test
    void concurrentOverlappingBatchesAreConsistent() throws InterruptedException {
        log.info("=== 用例：高并发重叠批次接收 ===");
        int threads = 8;
        int keysPerThread = 50;
        // 每个线程提交同一批键空间，模拟上游并发重投
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger acceptedTotal = new AtomicInteger();
        AtomicInteger duplicatedTotal = new AtomicInteger();
        AtomicInteger rejectedTotal = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                List<EventMessage> batch = new ArrayList<>();
                for (int i = 0; i < keysPerThread; i++) {
                    batch.add(new EventMessage("conc-key-" + i, "ORDER", "p" + i));
                }
                BatchIngestResponse resp = ingestionService.ingestBatch(batch);
                acceptedTotal.addAndGet(resp.accepted());
                duplicatedTotal.addAndGet(resp.duplicated());
                rejectedTotal.addAndGet(resp.rejected());
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        ingestionService.awaitQuiescence(30_000);

        log.info("并发受理汇总: accepted={} duplicated={} rejected={}",
                acceptedTotal.get(), duplicatedTotal.get(), rejectedTotal.get());
        // 守恒：每个键要么被受理一次，要么其余提交全部判重
        assertThat(acceptedTotal.get()).isEqualTo(keysPerThread);
        assertThat(acceptedTotal.get() + duplicatedTotal.get() + rejectedTotal.get())
                .isEqualTo(threads * keysPerThread);

        long successCount = store.findByStatus(EventStatus.SUCCESS).stream()
                .filter(r -> r.getMessage().dedupKey().startsWith("conc-key-")).count();
        log.info("成功处理记录数={} 业务应用总次数={}", successCount, processor.totalApplied());
        assertThat(successCount).isEqualTo(keysPerThread);
        for (int i = 0; i < keysPerThread; i++) {
            assertThat(processor.appliedCount("conc-key-" + i)).isEqualTo(1);
        }
        log.info("指标快照: {}", metrics.snapshot());
        log.info("=== 用例通过：并发下无重复计数、无数据丢失 ===");
    }
}
