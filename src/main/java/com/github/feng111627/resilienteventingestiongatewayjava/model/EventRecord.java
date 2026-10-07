package com.github.feng111627.resilienteventingestiongatewayjava.model;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

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

    /** 重放并发守卫：同一事件同一时间只允许一个重放轮次 */
    private final AtomicBoolean replaying = new AtomicBoolean(false);
    /** 已启动的重放轮次数 */
    private volatile int replayCount;
    /** 最近一次重放结果：SUCCESS / FAILED / null(从未重放) */
    private volatile String lastReplayResult;
    /** 当前重放轮次起始时的 attempts 基线，用于计算轮次内尝试次数 */
    private volatile int replayBaseAttempts;

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
    public int getReplayCount() { return replayCount; }
    public String getLastReplayResult() { return lastReplayResult; }
    public boolean isReplayInProgress() { return replaying.get(); }

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
        if (replaying.compareAndSet(true, false)) {
            this.lastReplayResult = "SUCCESS";
        }
    }

    public void markDeadLetter(String reason, String error) {
        this.status = EventStatus.DEAD_LETTER;
        this.failureReason = reason;
        this.lastError = error;
        this.updatedAt = Instant.now();
        if (replaying.compareAndSet(true, false)) {
            this.lastReplayResult = "FAILED";
        }
    }

    /**
     * 尝试认领一次重放（并发守卫）。认领成功即记录一轮重放与尝试基线，
     * 但此时事件尚未入队；若入队失败须调用 {@link #cancelReplayClaim()} 回滚。
     *
     * @return true=认领成功，false=已有重放进行中
     */
    public boolean tryClaimReplay() {
        if (!replaying.compareAndSet(false, true)) {
            return false;
        }
        this.replayCount++;
        this.replayBaseAttempts = this.attempts;
        return true;
    }

    /** 入队失败时回滚重放认领：不计数、不改状态，失败记录保持原样 */
    public void cancelReplayClaim() {
        this.replayCount--;
        this.replaying.set(false);
    }

    /** 重放入队成功：事件重新进入处理流程 */
    public void markReplaySubmitted() {
        this.status = EventStatus.RETRYING;
        this.updatedAt = Instant.now();
    }

    /** 当前是否处于重放轮次中（处理器据此选用重放专用重试次数） */
    public boolean isReplayRoundActive() {
        return replaying.get();
    }

    /** 当前轮次内已尝试次数（首次接入时等于 attempts） */
    public int attemptsInCurrentRound() {
        return attempts - replayBaseAttempts;
    }
}
