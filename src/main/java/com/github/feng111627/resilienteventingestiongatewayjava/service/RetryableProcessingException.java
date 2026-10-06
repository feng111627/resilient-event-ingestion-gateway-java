package com.github.feng111627.resilienteventingestiongatewayjava.service;

/** 可重试的处理异常（如模拟下游超时） */
public class RetryableProcessingException extends RuntimeException {
    public RetryableProcessingException(String message) {
        super(message);
    }
}
