package com.github.feng111627.resilienteventingestiongatewayjava.ratelimit;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 简单令牌桶限流器，限制接入请求速率；取不到令牌立即拒绝，不排队等待，
 * 保证压力升高时响应延迟有界。
 */
@Component
public class RateLimiter {

    private final double permitsPerSecond;
    private final double maxPermits;
    private double storedPermits;
    private long lastRefillNanos;

    @Autowired
    public RateLimiter(GatewayProperties properties) {
        this(properties.getRateLimitPerSecond());
    }

    public RateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        this.maxPermits = Math.max(1.0, permitsPerSecond);
        this.storedPermits = this.maxPermits;
        this.lastRefillNanos = System.nanoTime();
    }

    /** 尝试获取一个令牌，不等待。 */
    public synchronized boolean tryAcquire() {
        refill();
        if (storedPermits >= 1.0) {
            storedPermits -= 1.0;
            return true;
        }
        return false;
    }

    private void refill() {
        long now = System.nanoTime();
        double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000.0;
        storedPermits = Math.min(maxPermits, storedPermits + elapsedSeconds * permitsPerSecond);
        lastRefillNanos = now;
    }
}
