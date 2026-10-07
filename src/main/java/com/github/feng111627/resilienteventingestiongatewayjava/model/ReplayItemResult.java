package com.github.feng111627.resilienteventingestiongatewayjava.model;

/** 单条事件重放的受理结果 */
public record ReplayItemResult(
        String dedupKey,
        /**
         * ACCEPTED=已受理重放, ALREADY_SUCCEEDED=已成功无需重放, NOT_REPLAYABLE=当前状态不可重放,
         * ALREADY_REPLAYING=正在重放中, REJECTED=容量不足拒绝, NOT_FOUND=去重键不存在
         */
        String result,
        String message
) {
    public static ReplayItemResult accepted(String key) {
        return new ReplayItemResult(key, "ACCEPTED", "replay accepted for async processing");
    }
    public static ReplayItemResult alreadySucceeded(String key) {
        return new ReplayItemResult(key, "ALREADY_SUCCEEDED", "event already succeeded, no replay needed");
    }
    public static ReplayItemResult notReplayable(String key, String msg) {
        return new ReplayItemResult(key, "NOT_REPLAYABLE", msg);
    }
    public static ReplayItemResult alreadyReplaying(String key) {
        return new ReplayItemResult(key, "ALREADY_REPLAYING", "replay already in progress for this event");
    }
    public static ReplayItemResult rejected(String key, String msg) {
        return new ReplayItemResult(key, "REJECTED", msg);
    }
    public static ReplayItemResult notFound(String key) {
        return new ReplayItemResult(key, "NOT_FOUND", "no event found for dedupKey");
    }
}
