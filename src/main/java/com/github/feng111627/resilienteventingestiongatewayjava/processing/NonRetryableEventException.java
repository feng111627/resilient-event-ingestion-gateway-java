package com.github.feng111627.resilienteventingestiongatewayjava.processing;

/** 不可重试异常：数据本身有问题，重试无意义，直接进死信。 */
public class NonRetryableEventException extends RuntimeException {
    public NonRetryableEventException(String message) { super(message); }
}
