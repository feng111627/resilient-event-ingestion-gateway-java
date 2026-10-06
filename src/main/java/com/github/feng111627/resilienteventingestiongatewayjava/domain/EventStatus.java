package com.github.feng111627.resilienteventingestiongatewayjava.domain;

/**
 * 事件处理状态机：RECEIVED -> PROCESSING -> SUCCESS
 *                                  \-> FAILED_RETRYING -> PROCESSING ...
 *                                  \-> DEAD_LETTER (重试耗尽或不可重试)
 */
public enum EventStatus {
    RECEIVED,
    PROCESSING,
    SUCCESS,
    FAILED_RETRYING,
    DEAD_LETTER
}
