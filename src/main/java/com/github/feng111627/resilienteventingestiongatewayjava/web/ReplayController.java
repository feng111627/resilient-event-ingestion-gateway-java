package com.github.feng111627.resilienteventingestiongatewayjava.web;

import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayBatchResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayItemResult;
import com.github.feng111627.resilienteventingestiongatewayjava.model.ReplayStatusView;
import com.github.feng111627.resilienteventingestiongatewayjava.service.ReplayService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** 失败事件重放端点 */
@RestController
@RequestMapping("/api/replay")
public class ReplayController {

    private final ReplayService replayService;

    public ReplayController(ReplayService replayService) {
        this.replayService = replayService;
    }

    /**
     * 按去重键重放单条失败事件。
     * 202=已受理重放；200=已成功无需重放；404=键不存在；409=状态不可重放或重放进行中；429=容量不足。
     */
    @PostMapping("/events/{dedupKey}")
    public ResponseEntity<ReplayItemResult> replayOne(@PathVariable String dedupKey) {
        ReplayItemResult result = replayService.replayByKey(dedupKey);
        HttpStatus status = switch (result.result()) {
            case "ACCEPTED" -> HttpStatus.ACCEPTED;
            case "ALREADY_SUCCEEDED" -> HttpStatus.OK;
            case "NOT_FOUND" -> HttpStatus.NOT_FOUND;
            case "NOT_REPLAYABLE", "ALREADY_REPLAYING" -> HttpStatus.CONFLICT;
            default -> HttpStatus.TOO_MANY_REQUESTS;
        };
        return ResponseEntity.status(status).body(result);
    }

    /**
     * 按失败原因批量重放死信事件，如 ?reason=MAX_RETRIES_EXCEEDED；不传则重放全部死信。
     * 逐条独立受理，互不影响；全部因容量被拒时返回 429。
     */
    @PostMapping("/failures")
    public ResponseEntity<ReplayBatchResponse> replayByReason(@RequestParam(required = false) String reason) {
        ReplayBatchResponse response = replayService.replayByReason(reason);
        HttpStatus status;
        if (response.accepted() > 0 || response.total() == 0) {
            status = HttpStatus.ACCEPTED;
        } else if (response.rejected() > 0
                && response.alreadySucceeded() == 0 && response.alreadyReplaying() == 0) {
            status = HttpStatus.TOO_MANY_REQUESTS;
        } else {
            status = HttpStatus.ACCEPTED;
        }
        return ResponseEntity.status(status).body(response);
    }

    /** 按去重键查询重放状态：是否重放过、重放次数、最近一次重放结果 */
    @GetMapping("/events/{dedupKey}")
    public ResponseEntity<ReplayStatusView> replayStatus(@PathVariable String dedupKey) {
        return replayService.replayStatusOf(dedupKey)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
