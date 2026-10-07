package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchReplayResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayResponse;

/** 失败事件重放服务：按去重键单条重放、按失败原因批量重放 */
public interface ReplayService {

    /**
     * 按去重键重放单条失败事件。
     * 仅 DEAD_LETTER 状态可重放；已成功事件幂等跳过；并发重放只受理一个。
     */
    ReplayResponse replayByKey(String dedupKey);

    /**
     * 按失败原因批量重放（reason 为 null 时重放全部失败记录）。
     * 逐条独立受理，互不影响；超过单批上限时截断，可再次发起继续。
     */
    BatchReplayResponse replayByReason(String reason);
}
