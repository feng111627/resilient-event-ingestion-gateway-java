package com.github.feng111627.resilienteventingestiongatewayjava.web;

import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRequest;
import com.github.feng111627.resilienteventingestiongatewayjava.service.IngestService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/** 批量事件接入端点 */
@RestController
@RequestMapping("/api/events")
public class IngestController {

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    /**
     * 批量接收事件。受理后立即返回（异步处理）。
     * 全部拒绝时返回 429，否则 202 并在 items 中给出逐条结果。
     */
    @PostMapping("/batch")
    public ResponseEntity<BatchIngestResponse> ingestBatch(@RequestBody List<EventRequest> events) {
        BatchIngestResponse response = ingestService.ingest(events);
        HttpStatus status = (response.accepted() == 0 && response.duplicates() == 0)
                ? HttpStatus.TOO_MANY_REQUESTS
                : HttpStatus.ACCEPTED;
        return ResponseEntity.status(status).body(response);
    }
}
