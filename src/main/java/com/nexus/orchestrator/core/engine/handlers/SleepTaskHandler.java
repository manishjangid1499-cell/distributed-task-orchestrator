package com.nexus.orchestrator.core.engine.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexus.orchestrator.core.engine.RetryableTaskException;
import com.nexus.orchestrator.core.engine.TaskExecutionContext;
import com.nexus.orchestrator.core.engine.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SleepTaskHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(SleepTaskHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String taskType() {
        return "SLEEP";
    }

    @Override
    public JsonNode execute(JsonNode payload, TaskExecutionContext context) throws Exception {
        long durationMs = 200;
        if (payload != null && payload.has("durationMs")) {
            durationMs = payload.get("durationMs").asLong(200);
        }

        log.info("[SleepHandler] Task [{}] starting sleep of {}ms on worker [{}]", 
            context.getTaskId(), durationMs, context.getWorkerId());

        long elapsed = 0;
        long step = 100;
        while (elapsed < durationMs) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Task thread interrupted during sleep execution");
            }
            if (!context.isOwnershipValid()) {
                throw new RetryableTaskException("Lease ownership lost during long-running execution");
            }
            long sleepChunk = Math.min(step, durationMs - elapsed);
            Thread.sleep(sleepChunk);
            elapsed += sleepChunk;
        }

        ObjectNode res = MAPPER.createObjectNode();
        res.put("status", "SUCCESS");
        res.put("sleptMs", durationMs);
        res.put("workerId", context.getWorkerId());
        res.put("attempt", context.getAttemptNumber());
        return res;
    }
}
