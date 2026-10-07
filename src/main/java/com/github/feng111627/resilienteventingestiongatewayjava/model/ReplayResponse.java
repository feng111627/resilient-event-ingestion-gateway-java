package com.github.feng111627.resilienteventingestiongatewayjava.model;

/** 单条重放响应：受理结果 + 当前重放轨迹 */
public record ReplayResponse(
        String dedupKey,
        String result,
        String message,
        String status,
        int replayCount,
        String lastReplayResult
) {
}
