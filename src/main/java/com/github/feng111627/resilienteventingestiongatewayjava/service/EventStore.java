package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件记录存储（本地内存实现）。
 * 以 dedupKey 为主键，putIfAbsent 保证并发下同一键只受理一次。
 */
@Component
public class EventStore {

    private final ConcurrentHashMap<String, EventRecord> records = new ConcurrentHashMap<>();

    /**
     * 幂等写入：仅当 dedupKey 不存在时插入。
     *
     * @return true=新插入，false=已存在（重复提交，调用方按幂等去重处理）
     */
    public boolean insertIfAbsent(EventRecord record) {
        return records.putIfAbsent(record.getDedupKey(), record) == null;
    }

    public Optional<EventRecord> findByKey(String dedupKey) {
        return Optional.ofNullable(records.get(dedupKey));
    }

    public List<EventRecord> findByStatus(EventStatus status) {
        return records.values().stream()
                .filter(r -> r.getStatus() == status)
                .sorted(Comparator.comparing(EventRecord::getReceivedAt))
                .toList();
    }

    public List<EventRecord> findDeadLettersByReason(String reason) {
        return records.values().stream()
                .filter(r -> r.getStatus() == EventStatus.DEAD_LETTER)
                .filter(r -> reason == null || reason.equals(r.getFailureReason()))
                .sorted(Comparator.comparing(EventRecord::getReceivedAt))
                .toList();
    }

    public List<EventRecord> findAll() {
        return records.values().stream()
                .sorted(Comparator.comparing(EventRecord::getReceivedAt))
                .toList();
    }

    public int size() {
        return records.size();
    }
}
