package com.github.feng111627.resilienteventingestiongatewayjava.tests;

import com.github.feng111627.resilienteventingestiongatewayjava.ratelimit.RateLimiter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 覆盖业务：接入限流 —— 突发流量下超出速率的请求立即被拒绝，
 * 不排队等待，保证压力升高时响应延迟有界。
 */
class RateLimiterTest {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterTest.class);

    @Test
    void burstBeyondRateIsRejectedImmediately() {
        log.info("=== 用例：突发流量限流 ===");
        RateLimiter limiter = new RateLimiter(10); // 10 请求/秒
        int acquired = 0;
        for (int i = 0; i < 100; i++) {
            if (limiter.tryAcquire()) {
                acquired++;
            }
        }
        log.info("突发 100 个请求，瞬时通过 {} 个", acquired);
        assertThat(acquired).isLessThanOrEqualTo(10);
        assertThat(acquired).isGreaterThan(0);
        log.info("=== 用例通过：超出速率的请求被立即拒绝 ===");
    }

    @Test
    void tokensRefillOverTime() throws InterruptedException {
        log.info("=== 用例：令牌随时间补充 ===");
        RateLimiter limiter = new RateLimiter(50); // 50/秒 => 20ms 约 1 个
        while (limiter.tryAcquire()) {
            // 抽干
        }
        Thread.sleep(100);
        int acquired = 0;
        for (int i = 0; i < 10; i++) {
            if (limiter.tryAcquire()) {
                acquired++;
            }
        }
        log.info("等待 100ms 后可获取 {} 个令牌", acquired);
        assertThat(acquired).isGreaterThan(0);
        log.info("=== 用例通过：限流器按速率恢复 ===");
    }
}
