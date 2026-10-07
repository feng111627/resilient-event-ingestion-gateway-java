package com.github.feng111627.resilienteventingestiongatewayjava.model;

import java.time.Instant;

/** 按去重键查询的重放轨迹视图 */
public record ReplayStatusResponse(
        String dedupKey,
        String status,
        boolean replayInFlight,
        int replayCount,
        String lastReplayResult,
        String lastReplayError,
        Instant lastReplayAt
) {
    public static ReplayStatusResponse of(EventRecord r) {
        return new ReplayStatusResponse(
                r.getDedupKey(),
                r.getStatus().name(),
                r.isReplayInFlight(),
                r.getReplayCount(),
                r.getLastReplayResult(),
                r.getLastReplayError(),
                r.getLastReplayAt());
    }
}
