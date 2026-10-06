package com.github.feng111627.resilienteventingestiongatewayjava.processing;

/** 可重试异常：临时性故障，重试后可能成功。 */
public class RetryableEventException extends RuntimeException {
    public RetryableEventException(String message) { super(message); }
}
