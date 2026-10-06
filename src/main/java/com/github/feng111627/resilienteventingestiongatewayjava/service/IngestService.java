package com.github.feng111627.resilienteventingestiongatewayjava.service;

import com.github.feng111627.resilienteventingestiongatewayjava.model.BatchIngestResponse;
import com.github.feng111627.resilienteventingestiongatewayjava.model.EventRequest;

import java.util.List;

/** 接入服务：限流、校验、幂等受理、入队 */
public interface IngestService {
    BatchIngestResponse ingest(List<EventRequest> events);
}
