package com.github.feng111627.resilienteventingestiongatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 容量与限流参数。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /** 处理线程数。 */
    private int workerThreads = 4;

    /** 队列容量，满则拒绝。 */
    private int queueCapacity = 100;

    /** 限流速率（请求/秒）。 */
    private double rateLimitPerSecond = 200.0;

    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public double getRateLimitPerSecond() { return rateLimitPerSecond; }
    public void setRateLimitPerSecond(double rateLimitPerSecond) { this.rateLimitPerSecond = rateLimitPerSecond; }
}
