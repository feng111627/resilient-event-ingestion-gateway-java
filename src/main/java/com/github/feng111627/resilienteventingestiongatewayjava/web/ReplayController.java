package com.github.feng111627.resilienteventingestiongatewayjava.web;

import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchReplayResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayStatusResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.service.EventStore;
import com.github.feng111627.resilienteventingestiongatewayjava.service.ReplayService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 失败事件重放端点。
 * 单条重放：202=已受理/幂等跳过，404=键不存在，409=已有重放在途，422=状态不可重放，429=容量不足拒绝。
 * 批量重放：逐条结果见 items；全部因容量被拒时整体 429。
 */
@RestController
@RequestMapping("/api/replays")
public class ReplayController {

    private final ReplayService replayService;
    private final EventStore store;

    public ReplayController(ReplayService replayService, EventStore store) {
        this.replayService = replayService;
        this.store = store;
    }

    /** 按去重键重放单条失败事件 */
    @PostMapping("/{dedupKey}")
    public ResponseEntity<ReplayResponse> replayOne(@PathVariable String dedupKey) {
        ReplayResponse response = replayService.replayByKey(dedupKey);
        HttpStatus status = switch (response.result()) {
            case "NOT_FOUND" -> HttpStatus.NOT_FOUND;
            case "REJECTED" -> HttpStatus.TOO_MANY_REQUESTS;
            case "REPLAY_IN_PROGRESS" -> HttpStatus.CONFLICT;
            case "NOT_REPLAYABLE" -> HttpStatus.UNPROCESSABLE_ENTITY;
            default -> HttpStatus.ACCEPTED;
        };
        return ResponseEntity.status(status).body(response);
    }

    /** 按失败原因批量重放，body 可选：{"reason": "MAX_RETRIES_EXCEEDED"}，缺省重放全部失败记录 */
    @PostMapping("/batch")
    public ResponseEntity<BatchReplayResponse> replayBatch(
            @RequestBody(required = false) Map<String, String> body) {
        String reason = body == null ? null : body.get("reason");
        BatchReplayResponse response = replayService.replayByReason(reason);
        HttpStatus status = (response.replayAccepted() == 0 && response.rejected() > 0)
                ? HttpStatus.TOO_MANY_REQUESTS
                : HttpStatus.ACCEPTED;
        return ResponseEntity.status(status).body(response);
    }

    /** 按去重键查询重放轨迹：是否重放过、重放次数、最终结果 */
    @GetMapping("/{dedupKey}")
    public ResponseEntity<ReplayStatusResponse> replayStatus(@PathVariable String dedupKey) {
        return store.findByKey(dedupKey)
                .map(r -> ResponseEntity.ok(ReplayStatusResponse.of(r)))
                .orElse(ResponseEntity.notFound().build());
    }
}
