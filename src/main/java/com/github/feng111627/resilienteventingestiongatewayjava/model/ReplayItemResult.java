package com.github.feng111627.resilienteventingestiongatewayjava.model;

/** 重放请求中单条事件的受理结果 */
public record ReplayItemResult(
        String dedupKey,
        /**
         * REPLAY_ACCEPTED=已受理进入重放, ALREADY_SUCCEEDED=已成功无需重放(幂等跳过),
         * REPLAY_IN_PROGRESS=已有重放在途, NOT_REPLAYABLE=当前状态不可重放,
         * REJECTED=容量不足拒绝(原失败记录不变), NOT_FOUND=去重键不存在
         */
        String result,
        String message
) {
    public static ReplayItemResult accepted(String key) {
        return new ReplayItemResult(key, "REPLAY_ACCEPTED", "accepted for async replay");
    }
    public static ReplayItemResult alreadySucceeded(String key) {
        return new ReplayItemResult(key, "ALREADY_SUCCEEDED", "already succeeded, no replay needed");
    }
    public static ReplayItemResult inProgress(String key) {
        return new ReplayItemResult(key, "REPLAY_IN_PROGRESS", "another replay is already in flight");
    }
    public static ReplayItemResult notReplayable(String key, String status) {
        return new ReplayItemResult(key, "NOT_REPLAYABLE",
                "only DEAD_LETTER events can be replayed, current status=" + status);
    }
    public static ReplayItemResult rejected(String key, String msg) {
        return new ReplayItemResult(key, "REJECTED", msg);
    }
    public static ReplayItemResult notFound(String key) {
        return new ReplayItemResult(key, "NOT_FOUND", "no event with this dedupKey");
    }
}
