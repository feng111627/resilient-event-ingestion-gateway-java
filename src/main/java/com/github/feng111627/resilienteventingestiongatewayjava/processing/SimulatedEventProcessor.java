package com.github.feng111627.resilienteventingestiongatewayjava.processing;

import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地模拟下游处理器：按 payload 中的指令注入失败与耗时，用于本地验证。
 * <ul>
 *   <li>payload 含 "FATAL"：抛不可重试异常，直接进死信</li>
 *   <li>payload 含 "RETRYABLE:n"：前 n 次抛可重试异常，之后成功；不带 n 则一直失败</li>
 *   <li>payload 含 "SLOW:毫秒"：模拟处理耗时</li>
 * </ul>
 * 每个去重键的业务效果只会计数一次，便于验证幂等。
 */
@Component
public class SimulatedEventProcessor implements EventProcessor {

    private static final Logger log = LoggerFactory.getLogger(SimulatedEventProcessor.class);

    private final Map<String, AtomicInteger> attemptsPerKey = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> appliedPerKey = new ConcurrentHashMap<>();

    @Override
    public void process(EventMessage message) {
        String payload = message.payload() == null ? "" : message.payload();
        int attempt = attemptsPerKey
                .computeIfAbsent(message.dedupKey(), k -> new AtomicInteger())
                .incrementAndGet();

        int slowMillis = directiveValue(payload, "SLOW");
        if (slowMillis > 0) {
            try {
                Thread.sleep(slowMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (payload.contains("FATAL")) {
            log.warn("[模拟下游] key={} 第{}次处理触发不可重试失败", message.dedupKey(), attempt);
            throw new NonRetryableEventException("FATAL: 数据非法，不可重试");
        }
        int retryableTimes = directiveValue(payload, "RETRYABLE");
        boolean alwaysFail = payload.contains("RETRYABLE") && retryableTimes < 0;
        if (alwaysFail || attempt <= retryableTimes) {
            log.warn("[模拟下游] key={} 第{}次处理触发可重试失败", message.dedupKey(), attempt);
            throw new RetryableEventException("RETRYABLE: 模拟临时故障(第" + attempt + "次)");
        }

        appliedPerKey.computeIfAbsent(message.dedupKey(), k -> new AtomicInteger()).incrementAndGet();
        log.debug("[模拟下游] key={} 处理成功", message.dedupKey());
    }

    /** 解析 "NAME:数字" 指令；只有名字没有数字时返回 -1，不存在返回 0。 */
    private int directiveValue(String payload, String name) {
        int idx = payload.indexOf(name);
        if (idx < 0) {
            return 0;
        }
        int colon = payload.indexOf(':', idx + name.length());
        if (colon != idx + name.length()) {
            return -1;
        }
        StringBuilder num = new StringBuilder();
        for (int i = colon + 1; i < payload.length() && Character.isDigit(payload.charAt(i)); i++) {
            num.append(payload.charAt(i));
        }
        return num.isEmpty() ? -1 : Integer.parseInt(num.toString());
    }

    /** 某去重键业务效果被应用的次数（验证幂等：应为 1）。 */
    public int appliedCount(String dedupKey) {
        AtomicInteger c = appliedPerKey.get(dedupKey);
        return c == null ? 0 : c.get();
    }

    public int totalApplied() {
        return appliedPerKey.values().stream().mapToInt(AtomicInteger::get).sum();
    }
}
