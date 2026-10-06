package com.github.feng111627.resilienteventingestiongatewayjava.service;

/** 不可重试的处理异常（如模拟数据非法），直接进入失败记录 */
public class NonRetryableProcessingException extends RuntimeException {
    public NonRetryableProcessingException(String message) {
        super(message);
    }
}
