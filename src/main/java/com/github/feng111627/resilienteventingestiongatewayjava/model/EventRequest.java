package com.github.feng111627.resilienteventingestiongatewayjava.model;

/** 单条事件请求 */
public record EventRequest(
        /** 去重键，必填，同一键重复提交幂等 */
        String dedupKey,
        /** 业务负载 */
        String payload,
        /** 本地模拟用：控制处理行为，可选值 none/retryable/fatal/flaky */
        String failMode
) {
}
