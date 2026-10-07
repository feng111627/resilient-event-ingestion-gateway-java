package com.github.feng111627.resilienteventingestiongatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 网关容量与限流参数 */
@Component
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {
    /** 待处理队列容量，满则拒绝 */
    private int queueCapacity = 256;
    /** 处理工作线程数 */
    private int workerThreads = 4;
    /** 最大重试次数（不含首次） */
    private int maxRetries = 3;
    /** 重试基础退避毫秒 */
    private long retryBackoffMillis = 50;
    /** 限流：每秒允许的接入请求数 */
    private double rateLimitPerSecond = 200;
    /** 单批最大事件数 */
    private int maxBatchSize = 500;
    /** 重放专属最大重试次数（不含重放首次），与首次接入的 maxRetries 相互独立 */
    private int replayMaxRetries = 2;
    /** 重放入口限流：每秒允许的重放请求数，保证压力下重放延迟有界 */
    private double replayRateLimitPerSecond = 50;

    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public long getRetryBackoffMillis() { return retryBackoffMillis; }
    public void setRetryBackoffMillis(long retryBackoffMillis) { this.retryBackoffMillis = retryBackoffMillis; }
    public double getRateLimitPerSecond() { return rateLimitPerSecond; }
    public void setRateLimitPerSecond(double rateLimitPerSecond) { this.rateLimitPerSecond = rateLimitPerSecond; }
    public int getMaxBatchSize() { return maxBatchSize; }
    public void setMaxBatchSize(int maxBatchSize) { this.maxBatchSize = maxBatchSize; }
    public int getReplayMaxRetries() { return replayMaxRetries; }
    public void setReplayMaxRetries(int replayMaxRetries) { this.replayMaxRetries = replayMaxRetries; }
    public double getReplayRateLimitPerSecond() { return replayRateLimitPerSecond; }
    public void setReplayRateLimitPerSecond(double replayRateLimitPerSecond) { this.replayRateLimitPerSecond = replayRateLimitPerSecond; }
}
