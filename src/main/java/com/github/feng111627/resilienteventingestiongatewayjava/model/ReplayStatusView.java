package com.github.feng111627.resilienteventingestiongatewayjava.model;

/** 按去重键查询的重放状态视图 */
public record ReplayStatusView(
        String dedupKey,
        String status,
        boolean replayed,
        int replayCount,
        /** 最近一次重放结果：SUCCESS / FAILED / null(从未重放) */
        String lastReplayResult,
        boolean replayInProgress
) {
}
