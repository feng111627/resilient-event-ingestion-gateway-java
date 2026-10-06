package com.github.feng111627.resilienteventingestiongatewayjava.model;

/** 批量接入中单条事件的受理结果 */
public record BatchItemResult(
        String dedupKey,
        /** ACCEPTED=新受理, DUPLICATE=幂等去重, REJECTED=容量不足拒绝, INVALID=参数非法 */
        String result,
        String message
) {
    public static BatchItemResult accepted(String key) {
        return new BatchItemResult(key, "ACCEPTED", "accepted for async processing");
    }
    public static BatchItemResult duplicate(String key) {
        return new BatchItemResult(key, "DUPLICATE", "same dedupKey already ingested, idempotent skip");
    }
    public static BatchItemResult rejected(String key, String msg) {
        return new BatchItemResult(key, "REJECTED", msg);
    }
    public static BatchItemResult invalid(String key, String msg) {
        return new BatchItemResult(key, "INVALID", msg);
    }
}
