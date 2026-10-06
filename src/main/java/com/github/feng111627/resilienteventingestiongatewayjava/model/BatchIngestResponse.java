package com.github.feng111627.resilienteventingestiongatewayjava.model;

import java.util.List;

/** 批量接入响应 */
public record BatchIngestResponse(
        String batchId,
        int total,
        int accepted,
        int duplicates,
        int rejected,
        int invalid,
        List<BatchItemResult> items
) {
}
