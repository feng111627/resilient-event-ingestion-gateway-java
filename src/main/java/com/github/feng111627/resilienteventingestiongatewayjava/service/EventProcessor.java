package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;

/** 异步事件处理器：有界队列 + 工作线程 + 重试/死信 */
public interface EventProcessor {

    /**
     * 尝试提交事件到处理队列。
     * @return true=已入队，false=队列已满（调用方应拒绝）
     */
    boolean trySubmit(EventRecord record);

    int getQueueDepth();
    int getQueueCapacity();
    int getActiveWorkers();

    void start();
    void shutdown();
}
