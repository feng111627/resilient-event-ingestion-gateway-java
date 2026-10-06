package com.github.feng111627.resilienteventingestiongatewayjava.model;

/** 事件处理状态 */
public enum EventStatus {
    /** 已接收，等待处理 */
    RECEIVED,
    /** 处理成功 */
    SUCCESS,
    /** 可重试失败处理中（等待重试） */
    RETRYING,
    /** 重试耗尽或不可重试，进入失败记录 */
    DEAD_LETTER
}
