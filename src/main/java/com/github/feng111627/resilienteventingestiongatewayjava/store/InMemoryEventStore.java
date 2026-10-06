package com.github.feng111627.resilienteventingestiongatewayjava.store;

import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.domain.EventStatus;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于 ConcurrentHashMap 的本地存储，putIfAbsent 保证去重键唯一。
 */
@Component
public class InMemoryEventStore implements EventStore {

    private final ConcurrentMap<String, EventRecord> records = new ConcurrentHashMap<>();

    @Override
    public boolean saveIfAbsent(EventRecord record) {
        return records.putIfAbsent(record.getMessage().dedupKey(), record) == null;
    }

    @Override
    public void remove(String dedupKey) {
        records.remove(dedupKey);
    }

    @Override
    public Optional<EventRecord> findByKey(String dedupKey) {
        return Optional.ofNullable(records.get(dedupKey));
    }

    @Override
    public List<EventRecord> findByStatus(EventStatus status) {
        return records.values().stream()
                .filter(r -> r.getStatus() == status)
                .toList();
    }

    @Override
    public List<EventRecord> findByFailureReason(String failureReason) {
        return records.values().stream()
                .filter(r -> failureReason.equals(r.getFailureReason()))
                .toList();
    }

    @Override
    public List<EventRecord> findAll() {
        return List.copyOf(records.values());
    }
}
