package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayBatchResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayItemResult;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayStatusView;

import java.util.Optional;

/** 失败事件重放服务：单条重放、按失败原因批量重放、重放状态查询 */
public interface ReplayService {

    /** 按去重键重放单条失败事件 */
    ReplayItemResult replayByKey(String dedupKey);

    /** 按失败原因批量重放死信事件；reason 为 null 时重放全部死信 */
    ReplayBatchResponse replayByReason(String reason);

    /** 按去重键查询重放状态 */
    Optional<ReplayStatusView> replayStatusOf(String dedupKey);
}
