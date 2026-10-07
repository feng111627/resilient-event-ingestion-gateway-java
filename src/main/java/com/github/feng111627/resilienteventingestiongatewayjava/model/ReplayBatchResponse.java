package com.github.feng111627.resilienteventingestiongatewayjava.model;

import java.util.List;

/** 批量重放响应 */
public record ReplayBatchResponse(
        String replayBatchId,
        String reason,
        int total,
        int accepted,
        int alreadySucceeded,
        int notReplayable,
        int alreadyReplaying,
        int rejected,
        List<ReplayItemResult> items
) {
}
