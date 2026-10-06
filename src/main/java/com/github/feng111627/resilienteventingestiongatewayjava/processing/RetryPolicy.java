package com.github.feng111627.resilienteventingestiongatewayjava.processing;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 重试策略参数：最大尝试次数与退避间隔。
 */
@ConfigurationProperties(prefix = "gateway.retry")
public class RetryPolicy {

    /** 最大尝试次数（含首次）。 */
    private int maxAttempts = 3;

    /** 重试退避基数（毫秒），第 n 次重试等待 backoffMillis * n。 */
    private long backoffMillis = 50;

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getBackoffMillis() { return backoffMillis; }
    public void setBackoffMillis(long backoffMillis) { this.backoffMillis = backoffMillis; }
}
