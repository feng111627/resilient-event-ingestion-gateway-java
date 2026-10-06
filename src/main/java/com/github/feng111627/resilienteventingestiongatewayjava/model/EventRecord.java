package com.github.feng111627.resilienteventingestiongatewayjava.model;

import java.time.Instant;

/** 事件处理记录（接入侧可查询的状态） */
public class EventRecord {
    private final String dedupKey;
    private final String payload;
    private final String failMode;
    private volatile EventStatus status;
    private volatile int attempts;
    private volatile String lastError;
    private volatile String failureReason;
    private final Instant receivedAt;
    private volatile Instant updatedAt;
    private volatile long processingMillis;

    public EventRecord(String dedupKey, String payload, String failMode) {
        this.dedupKey = dedupKey;
        this.payload = payload;
        this.failMode = failMode == null ? "none" : failMode;
        this.status = EventStatus.RECEIVED;
        this.receivedAt = Instant.now();
        this.updatedAt = this.receivedAt;
    }

    public String getDedupKey() { return dedupKey; }
    public String getPayload() { return payload; }
    public String getFailMode() { return failMode; }
    public EventStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public String getLastError() { return lastError; }
    public String getFailureReason() { return failureReason; }
    public Instant getReceivedAt() { return receivedAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public long getProcessingMillis() { return processingMillis; }

    public void markAttempt() {
        this.attempts++;
        this.updatedAt = Instant.now();
    }

    public void markRetrying(String error) {
        this.status = EventStatus.RETRYING;
        this.lastError = error;
        this.updatedAt = Instant.now();
    }

    public void markSuccess(long processingMillis) {
        this.status = EventStatus.SUCCESS;
        this.processingMillis = processingMillis;
        this.updatedAt = Instant.now();
    }

    public void markDeadLetter(String reason, String error) {
        this.status = EventStatus.DEAD_LETTER;
        this.failureReason = reason;
        this.lastError = error;
        this.updatedAt = Instant.now();
    }
}
