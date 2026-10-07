package com.github.feng111627.resilienteventingestiongatewayjava.model;

import java.util.List;

/** 按失败原因批量重放的响应 */
public record BatchReplayResponse(
        String reason,
        /** 命中的失败记录总数（可能被 maxBatchSize 截断） */
        int candidates,
        /** 本次实际处理的条数 */
        int processed,
        int replayAccepted,
        int alreadySucceeded,
        int inProgress,
        int rejected,
        int notReplayable,
        /** 命中数超过单批上限被截断时为 true，可再次发起重放继续 */
        boolean truncated,
        List<ReplayItemResult> items
) {
}
