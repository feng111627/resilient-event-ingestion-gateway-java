package com.github.feng111627.resilienteventingestiongatewayjava.metrics;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 网关运行指标：计数、耗时、队列深度、拒绝次数。
 */
@Component
public class GatewayMetrics {

    private final LongAdder received = new LongAdder();
    private final LongAdder duplicated = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder succeeded = new LongAdder();
    private final LongAdder retried = new LongAdder();
    private final LongAdder deadLettered = new LongAdder();
    private final LongAdder totalLatencyMillis = new LongAdder();
    private final AtomicLong maxLatencyMillis = new AtomicLong();
    private final AtomicInteger queueDepth = new AtomicInteger();

    public void recordReceived(int count) { received.add(count); }
    public void recordDuplicate() { duplicated.increment(); }
    public void recordRejected() { rejected.increment(); }
    public void recordRetry() { retried.increment(); }
    public void recordDeadLetter() { deadLettered.increment(); }
    public void setQueueDepth(int depth) { queueDepth.set(depth); }

    public void recordSuccess(long latencyMillis) {
        succeeded.increment();
        totalLatencyMillis.add(latencyMillis);
        maxLatencyMillis.accumulateAndGet(latencyMillis, Math::max);
    }

    public Map<String, Object> snapshot() {
        long ok = succeeded.sum();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("received", received.sum());
        m.put("duplicated", duplicated.sum());
        m.put("rejected", rejected.sum());
        m.put("succeeded", ok);
        m.put("retried", retried.sum());
        m.put("deadLettered", deadLettered.sum());
        m.put("queueDepth", queueDepth.get());
        m.put("avgLatencyMillis", ok == 0 ? 0 : totalLatencyMillis.sum() / ok);
        m.put("maxLatencyMillis", maxLatencyMillis.get());
        return m;
    }
}
