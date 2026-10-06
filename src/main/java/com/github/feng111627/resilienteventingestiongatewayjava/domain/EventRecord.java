package com.github.feng111627.resilienteventingestiongatewayjava.domain;

import java.time.Instant;

/**
 * 事件在网关内的处理记录，状态随处理推进而迁移。
 */
public class EventRecord {

    private final EventMessage message;
    private volatile EventStatus status;
    private volatile int attempts;
    private volatile String failureReason;
    private volatile long lastLatencyMillis;
    private final Instant receivedAt;
    private volatile Instant updatedAt;

    public EventRecord(EventMessage message) {
        this.message = message;
        this.status = EventStatus.RECEIVED;
        this.attempts = 0;
        this.receivedAt = Instant.now();
        this.updatedAt = this.receivedAt;
    }

    public EventMessage getMessage() { return message; }
    public EventStatus getStatus() { return status; }
    @Override
    public String toString() {
        return "EventRecord{key=" + message.dedupKey() + ", status=" + status
                + ", attempts=" + attempts + ", reason=" + failureReason + "}";
    }
    public int getAttempts() { return attempts; }
    public String getFailureReason() { return failureReason; }
    public long getLastLatencyMillis() { return lastLatencyMillis; }
    public Instant getReceivedAt() { return receivedAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public synchronized void markProcessing() {
        this.status = EventStatus.PROCESSING;
        this.attempts++;
        touch();
    }

    public synchronized void markSuccess(long latencyMillis) {
        this.status = EventStatus.SUCCESS;
        this.lastLatencyMillis = latencyMillis;
        touch();
    }

    public synchronized void markRetrying(String reason, long latencyMillis) {
        this.status = EventStatus.FAILED_RETRYING;
        this.failureReason = reason;
        this.lastLatencyMillis = latencyMillis;
        touch();
    }

    public synchronized void markDeadLetter(String reason, long latencyMillis) {
        this.status = EventStatus.DEAD_LETTER;
        this.failureReason = reason;
        this.lastLatencyMillis = latencyMillis;
        touch();
    }

    private void touch() {
        this.updatedAt = Instant.now();
    }
}
