package com.github.feng111627.resilienteventingestiongatewayjava.domain;

import java.util.List;

/**
 * 批量接入响应：按去重键给出每个事件的受理结果。
 */
public record BatchIngestResponse(int accepted, int duplicated, int rejected,
                                  List<ItemResult> results) {

    public record ItemResult(String dedupKey, String outcome) {
        public static final String ACCEPTED = "ACCEPTED";
        public static final String DUPLICATE = "DUPLICATE";
        public static final String REJECTED = "REJECTED";
    }
}
