package com.nexus.orchestrator.core.engine.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexus.orchestrator.core.engine.TaskExecutionContext;
import com.nexus.orchestrator.core.engine.TaskHandler;
import com.nexus.orchestrator.model.ExecutionRecord;
import com.nexus.orchestrator.repository.ExecutionRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class RecordExecutionTaskHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(RecordExecutionTaskHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ExecutionRecordRepository recordRepository;

    public RecordExecutionTaskHandler(ExecutionRecordRepository recordRepository) {
        this.recordRepository = recordRepository;
    }

    @Override
    public String taskType() {
        return "RECORD_EXECUTION";
    }

    @Override
    public JsonNode execute(JsonNode payload, TaskExecutionContext context) {
        String key = context.getIdempotencyKey();
        if (key == null || key.isBlank()) {
            if (payload != null && payload.has("key")) {
                key = payload.get("key").asText();
            } else {
                key = "exec-" + context.getTaskId();
            }
        }

        // Check if already processed idempotently
        Optional<ExecutionRecord> existing = recordRepository.findByIdempotencyKey(key);
        if (existing.isPresent()) {
            log.info("[RecordExecutionHandler] Idempotent hit: Key [{}] already executed by worker [{}]. Returning existing outcome.",
                key, existing.get().getWorkerId());
            ObjectNode res = MAPPER.createObjectNode();
            res.put("status", "SUCCESS");
            res.put("idempotentReplay", true);
            res.put("key", key);
            res.put("firstRecordedBy", existing.get().getWorkerId());
            res.set("executionData", existing.get().getExecutionData());
            return res;
        }

        // Attempt write protected by PostgreSQL unique constraint
        ObjectNode execData = MAPPER.createObjectNode();
        execData.put("action", "LEDGER_CREDIT");
        execData.put("workerId", context.getWorkerId());
        execData.put("fencingToken", context.getFencingToken());
        execData.put("attempt", context.getAttemptNumber());

        ExecutionRecord record = new ExecutionRecord(
            key,
            context.getTaskId(),
            context.getWorkerId(),
            String.valueOf(payload != null ? payload.hashCode() : 0),
            execData
        );

        try {
            recordRepository.save(record);
            log.info("[RecordExecutionHandler] Successfully recorded initial execution for key [{}] by worker [{}]",
                key, context.getWorkerId());

            ObjectNode res = MAPPER.createObjectNode();
            res.put("status", "SUCCESS");
            res.put("idempotentReplay", false);
            res.put("key", key);
            res.put("workerId", context.getWorkerId());
            res.set("executionData", execData);
            return res;
        } catch (DataIntegrityViolationException ex) {
            // Concurrent race won by another worker: safe idempotent recovery
            log.warn("[RecordExecutionHandler] Unique constraint hit for key [{}] during concurrent attempt. Reading saved record.", key);
            ExecutionRecord saved = recordRepository.findByIdempotencyKey(key)
                .orElseThrow(() -> new IllegalStateException("Record must exist after duplicate key violation"));
            ObjectNode res = MAPPER.createObjectNode();
            res.put("status", "SUCCESS");
            res.put("idempotentReplay", true);
            res.put("key", key);
            res.put("firstRecordedBy", saved.getWorkerId());
            res.set("executionData", saved.getExecutionData());
            return res;
        }
    }
}
