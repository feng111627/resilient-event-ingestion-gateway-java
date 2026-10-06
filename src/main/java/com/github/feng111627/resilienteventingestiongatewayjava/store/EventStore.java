package com.github.feng111627.resilienteventingestiongatewayjava.store;

import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventStatus;

import java.util.List;
import java.util.Optional;

/**
 * 事件记录存储。以去重键为唯一约束，保证重复提交幂等。
 */
public interface EventStore {

    /** 已存在同键记录时返回 false，不覆盖原记录。 */
    boolean saveIfAbsent(EventRecord record);

    /** 投递被拒绝时移除记录，允许上游重试重新受理。 */
    void remove(String dedupKey);

    Optional<EventRecord> findByKey(String dedupKey);

    List<EventRecord> findByStatus(EventStatus status);

    List<EventRecord> findByFailureReason(String failureReason);

    List<EventRecord> findAll();
}
