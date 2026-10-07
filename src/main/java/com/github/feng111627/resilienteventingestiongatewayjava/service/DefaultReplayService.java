package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchReplayResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayItemResult;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.web.RateLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 默认重放服务。
 * 处理顺序：限流 -> 查记录 -> 逐条状态判定 -> 并发守卫(tryBeginReplay) -> 非阻塞入队。
 * - 仅 DEAD_LETTER 可重放；已成功事件幂等跳过（不重复处理、不重复计数）；
 * - 同一事件并发重放由 EventRecord 的守卫保证只受理一个；
 * - 队列满时明确 REJECTED 并回滚守卫，原失败记录不变，之后仍可再次重放；
 * - 入口令牌桶限流 + 非阻塞入队，压力下响应延迟有界。
 */
@Service
public class DefaultReplayService implements ReplayService {

    private static final Logger log = LoggerFactory.getLogger(DefaultReplayService.class);

    private final GatewayProperties props;
    private final EventStore store;
    private final EventProcessor processor;
    private final GatewayMetrics metrics;
    private final DefaultIngestService.TokenBucket rateLimiter;

    public DefaultReplayService(GatewayProperties props, EventStore store,
                                EventProcessor processor, GatewayMetrics metrics) {
        this.props = props;
        this.store = store;
        this.processor = processor;
        this.metrics = metrics;
        this.rateLimiter = new DefaultIngestService.TokenBucket(props.getReplayRateLimitPerSecond());
    }

    @Override
    public ReplayResponse replayByKey(String dedupKey) {
        if (dedupKey == null || dedupKey.isBlank()) {
            throw new IllegalArgumentException("dedupKey is required");
        }
        acquirePermit();
        Optional<EventRecord> found = store.findByKey(dedupKey);
        if (found.isEmpty()) {
            return new ReplayResponse(dedupKey, "NOT_FOUND",
                    "no event with this dedupKey", null, 0, null);
        }
        EventRecord record = found.get();
        ReplayItemResult result = tryReplayOne(record);
        log.info("Replay by key {}: {}", dedupKey, result.result());
        return new ReplayResponse(record.getDedupKey(), result.result(), result.message(),
                record.getStatus().name(), record.getReplayCount(), record.getLastReplayResult());
    }

    @Override
    public BatchReplayResponse replayByReason(String reason) {
        acquirePermit();
        List<EventRecord> candidates = store.findDeadLettersByReason(reason);
        boolean truncated = candidates.size() > props.getMaxBatchSize();
        List<EventRecord> batch = truncated
                ? candidates.subList(0, props.getMaxBatchSize())
                : candidates;

        List<ReplayItemResult> items = new ArrayList<>(batch.size());
        int accepted = 0, alreadySucceeded = 0, inProgress = 0, rejected = 0, notReplayable = 0;
        for (EventRecord record : batch) {
            // 逐条独立受理：单条被拒/在途/不可重放不影响其他条
            ReplayItemResult r = tryReplayOne(record);
            items.add(r);
            switch (r.result()) {
                case "REPLAY_ACCEPTED" -> accepted++;
                case "ALREADY_SUCCEEDED" -> alreadySucceeded++;
                case "REPLAY_IN_PROGRESS" -> inProgress++;
                case "REJECTED" -> rejected++;
                default -> notReplayable++;
            }
        }
        log.info("Batch replay reason={}: candidates={}, processed={}, accepted={}, "
                        + "alreadySucceeded={}, inProgress={}, rejected={}, notReplayable={}, truncated={}",
                reason, candidates.size(), batch.size(), accepted, alreadySucceeded,
                inProgress, rejected, notReplayable, truncated);
        return new BatchReplayResponse(reason, candidates.size(), batch.size(), accepted,
                alreadySucceeded, inProgress, rejected, notReplayable, truncated, items);
    }

    /** 重放入口限流：超限快速失败，保证压力下延迟有界 */
    private void acquirePermit() {
        if (!rateLimiter.tryAcquire()) {
            metrics.onReplayRejected();
            throw new RateLimitExceededException("replay rate limit exceeded: "
                    + props.getReplayRateLimitPerSecond() + " req/s");
        }
    }

    /** 单条重放受理：状态判定 -> 并发守卫 -> 非阻塞入队，容量拒绝时回滚守卫 */
    private ReplayItemResult tryReplayOne(EventRecord record) {
        String key = record.getDedupKey();
        // 已成功（含此前重放成功）：幂等跳过，不重复处理、不重复计数
        if (record.getStatus() == EventStatus.SUCCESS) {
            return ReplayItemResult.alreadySucceeded(key);
        }
        if (record.getStatus() != EventStatus.DEAD_LETTER) {
            return record.isReplayInFlight()
                    ? ReplayItemResult.inProgress(key)
                    : ReplayItemResult.notReplayable(key, record.getStatus().name());
        }
        // 并发守卫：同一事件同一时刻只允许一个重放在途
        if (!record.tryBeginReplay()) {
            return record.getStatus() == EventStatus.SUCCESS
                    ? ReplayItemResult.alreadySucceeded(key)
                    : ReplayItemResult.inProgress(key);
        }
        if (!processor.trySubmit(record)) {
            // 容量不足：回滚守卫与计数，原失败记录保持不变，之后可再次重放
            record.cancelReplay();
            metrics.onReplayRejected();
            return ReplayItemResult.rejected(key, "processing queue is full, replay later");
        }
        metrics.onReplayAccepted();
        return ReplayItemResult.accepted(key);
    }
}
