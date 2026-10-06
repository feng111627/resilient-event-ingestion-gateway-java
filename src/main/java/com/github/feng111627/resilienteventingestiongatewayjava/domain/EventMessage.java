package com.github.feng111627.resilienteventingestiongatewayjava.domain;

/**
 * 接入事件。dedupKey 为幂等去重键，由上游生成。
 */
public record EventMessage(String dedupKey, String eventType, String payload) {
}
