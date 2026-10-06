package com.github.feng111627.resilienteventingestiongatewayjava.web;

/** 限流触发异常 */
public class RateLimitExceededException extends RuntimeException {
    public RateLimitExceededException(String message) {
        super(message);
    }
}
