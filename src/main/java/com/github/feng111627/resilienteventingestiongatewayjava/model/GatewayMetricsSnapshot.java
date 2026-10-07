package com.github.feng111627.resilienteventingestiongatewayjava.model;

import java.util.Map;

/** 网关运行指标快照 */
public record GatewayMetricsSnapshot(
        long totalReceived,
        long totalAccepted,
        long totalDuplicates,
        long totalRejected,
        long totalSuccess,
        long totalDeadLetter,
        long totalRetries,
        int queueDepth,
        int queueCapacity,
        int activeWorkers,
        double avgProcessingMillis,
        long maxProcessingMillis,
        Map<String, Long> deadLetterByReason,
        long totalReplayAccepted,
        long totalReplayRejected,
        long totalReplaySuccess,
        long totalReplayFailed
) {
}
