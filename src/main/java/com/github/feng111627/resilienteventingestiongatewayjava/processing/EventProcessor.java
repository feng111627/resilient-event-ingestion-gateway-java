package com.github.feng111627.resilienteventingestiongatewayjava.processing;

import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventMessage;

/**
 * 事件业务处理器，模拟下游真实处理逻辑。
 */
public interface EventProcessor {
    void process(EventMessage message);
}
