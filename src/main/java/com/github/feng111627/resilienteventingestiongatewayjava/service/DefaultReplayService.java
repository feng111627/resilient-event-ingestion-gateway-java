package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.config.GatewayProperties;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRecord;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventStatus;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayBatchResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayItemResult;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayStatusView;
import com.github.feng111627.resilienteventingestiongatewayjava.web.RateLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 默认失败事件重放服务。
 * - 仅 DEAD_LETTER 状态的事件可重放；已成功的识别为无需重放，不重复处理、不重复计数；
 * - 同一事件的重放认领是原子操作，并发重放只有一个获胜，其余返回 ALREADY_REPLAYING；
 * - 入队非阻塞：容量不足时明确拒绝并回滚认领，原失败记录不被改动，之后仍可再次重放；
 * - 重放入口有独立限流，压力升高时快速拒绝，响应延迟有界。
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
    public ReplayItemResult replayByKey(String dedupKey) {
        if (dedupKey == null || dedupKey.isBlank()) {
            throw new IllegalArgumentException("dedupKey is required");
        }
        acquirePermit();
        Optional<EventRecord> found = store.findByKey(dedupKey);
        if (found.isEmpty()) {
            return ReplayItemResult.notFound(dedupKey);
        }
        ReplayItemResult result = tryReplay(found.get());
        log.info("Replay single {}: {}", dedupKey, result.result());
        return result;
    }

    @Override
    public ReplayBatchResponse replayByReason(String reason) {
        acquirePermit();
        List<EventRecord> deadLetters = store.findDeadLettersByReason(reason);
        if (deadLetters.size() > props.getReplayMaxBatchSize()) {
            throw new IllegalArgumentException("replay batch size " + deadLetters.size()
                    + " exceeds replayMaxBatchSize " + props.getReplayMaxBatchSize()
                    + ", narrow the reason filter");
        }

        String replayBatchId = UUID.randomUUID().toString();
        List<ReplayItemResult> items = new ArrayList<>(deadLetters.size());
        int accepted = 0, alreadySucceeded = 0, notReplayable = 0, alreadyReplaying = 0, rejected = 0;

        for (EventRecord record : deadLetters) {
            // 逐条独立受理：单条不可重放/容量拒绝不影响其他条目
            ReplayItemResult item = tryReplay(record);
            items.add(item);
            switch (item.result()) {
                case "ACCEPTED" -> accepted++;
                case "ALREADY_SUCCEEDED" -> alreadySucceeded++;
                case "NOT_REPLAYABLE" -> notReplayable++;
                case "ALREADY_REPLAYING" -> alreadyReplaying++;
                case "REJECTED" -> rejected++;
                default -> { }
            }
        }

        log.info("Replay batch {} (reason={}): total={}, accepted={}, alreadySucceeded={}, "
                        + "notReplayable={}, alreadyReplaying={}, rejected={}",
                replayBatchId, reason, deadLetters.size(), accepted, alreadySucceeded,
                notReplayable, alreadyReplaying, rejected);
        return new ReplayBatchResponse(replayBatchId, reason, deadLetters.size(), accepted,
                alreadySucceeded, notReplayable, alreadyReplaying, rejected, items);
    }

    @Override
    public Optional<ReplayStatusView> replayStatusOf(String dedupKey) {
        return store.findByKey(dedupKey).map(r -> new ReplayStatusView(
                r.getDedupKey(),
                r.getStatus().name(),
                r.getReplayCount() > 0,
                r.getReplayCount(),
                r.getLastReplayResult(),
                r.isReplayInProgress()));
    }

    /** 单条重放受理：状态校验 -> 原子认领 -> 非阻塞入队，失败则回滚认领 */
    private ReplayItemResult tryReplay(EventRecord record) {
        String key = record.getDedupKey();
        if (record.getStatus() == EventStatus.SUCCESS) {
            // 已成功的事件无需重放：不重复处理、不重复计数
            return ReplayItemResult.alreadySucceeded(key);
        }
        if (record.isReplayInProgress()) {
            // 重放进行中（含已认领尚未入队的窗口）：并发重放直接识别
            return ReplayItemResult.alreadyReplaying(key);
        }
        if (record.getStatus() != EventStatus.DEAD_LETTER) {
            return ReplayItemResult.notReplayable(key,
                    "only DEAD_LETTER events can be replayed, current status=" + record.getStatus());
        }
        if (!record.tryClaimReplay()) {
            // 并发重放同一条失败事件：只有一个认领成功
            return ReplayItemResult.alreadyReplaying(key);
        }
        if (!processor.trySubmit(record)) {
            // 容量不足：回滚认领，失败记录保持原样，之后仍可再次重放
            record.cancelReplayClaim();
            metrics.onReplayRejected();
            return ReplayItemResult.rejected(key, "processing queue is full, replay later");
        }
        record.markReplaySubmitted();
        metrics.onReplayAccepted();
        return ReplayItemResult.accepted(key);
    }

    private void acquirePermit() {
        if (!rateLimiter.tryAcquire()) {
            metrics.onReplayRejected();
            throw new RateLimitExceededException("replay rate limit exceeded: "
                    + props.getReplayRateLimitPerSecond() + " req/s");
        }
    }
}
