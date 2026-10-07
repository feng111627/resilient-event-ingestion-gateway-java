package com.github.feng111627.resilienteventingestiongatewayjava.model;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/** 事件处理记录（接入侧可查询的状态），含重放轨迹 */
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

    /** 重放并发守卫：同一时刻只允许一个重放在途 */
    private final AtomicBoolean replayInFlight = new AtomicBoolean(false);
    /** 已被受理的重放次数（容量拒绝未执行的不计数） */
    private volatile int replayCount;
    /** 本轮重放内的处理尝试次数（用于重放专属重试上限判定） */
    private volatile int replayAttempts;
    /** 最近一次重放最终结果：NONE / SUCCESS / FAILED */
    private volatile String lastReplayResult = "NONE";
    private volatile String lastReplayError;
    private volatile Instant lastReplayAt;

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

    public boolean isReplayInFlight() { return replayInFlight.get(); }
    public int getReplayCount() { return replayCount; }
    public int getReplayAttempts() { return replayAttempts; }
    public String getLastReplayResult() { return lastReplayResult; }
    public String getLastReplayError() { return lastReplayError; }
    public Instant getLastReplayAt() { return lastReplayAt; }

    public synchronized void markAttempt() {
        this.attempts++;
        if (replayInFlight.get()) {
            this.replayAttempts++;
        }
        this.updatedAt = Instant.now();
    }

    public synchronized void markRetrying(String error) {
        this.status = EventStatus.RETRYING;
        this.lastError = error;
        this.updatedAt = Instant.now();
    }

    public synchronized void markSuccess(long processingMillis) {
        this.status = EventStatus.SUCCESS;
        this.processingMillis = processingMillis;
        this.updatedAt = Instant.now();
    }

    public synchronized void markDeadLetter(String reason, String error) {
        this.status = EventStatus.DEAD_LETTER;
        this.failureReason = reason;
        this.lastError = error;
        this.updatedAt = Instant.now();
    }

    /**
     * 尝试开始一轮重放。仅当当前处于 DEAD_LETTER 且没有其他重放在途时成功，
     * synchronized 保证与 markReplaySuccess/markReplayFailed 的状态迁移互斥，
     * 已重放成功（SUCCESS）的事件不会再次被拉起。
     */
    public synchronized boolean tryBeginReplay() {
        if (status != EventStatus.DEAD_LETTER || replayInFlight.get()) {
            return false;
        }
        replayInFlight.set(true);
        this.replayAttempts = 0;
        this.replayCount++;
        this.status = EventStatus.REPLAYING;
        this.updatedAt = Instant.now();
        return true;
    }

    /** 重放入队失败（容量拒绝）时回滚：不计数、不改失败记录，之后仍可再次重放 */
    public synchronized void cancelReplay() {
        if (!replayInFlight.get()) {
            return;
        }
        replayInFlight.set(false);
        this.replayAttempts = 0;
        this.replayCount--;
        this.status = EventStatus.DEAD_LETTER;
        this.updatedAt = Instant.now();
    }

    public synchronized void markReplaySuccess(long processingMillis) {
        this.status = EventStatus.SUCCESS;
        this.processingMillis = processingMillis;
        this.lastReplayResult = "SUCCESS";
        this.lastReplayError = null;
        this.lastReplayAt = Instant.now();
        this.replayInFlight.set(false);
        this.updatedAt = Instant.now();
    }

    /** 重放最终失败：回到 DEAD_LETTER，保留首次失败原因，之后仍可再次重放 */
    public synchronized void markReplayFailed(String error) {
        this.status = EventStatus.DEAD_LETTER;
        this.lastReplayResult = "FAILED";
        this.lastReplayError = error;
        this.lastReplayAt = Instant.now();
        this.replayInFlight.set(false);
        this.updatedAt = Instant.now();
    }
}
