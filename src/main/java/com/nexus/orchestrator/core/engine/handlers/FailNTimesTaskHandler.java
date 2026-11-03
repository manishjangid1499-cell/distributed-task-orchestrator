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
public class FailNTimesTaskHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(FailNTimesTaskHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String taskType() {
        return "FAIL_N_TIMES";
    }

    @Override
    public JsonNode execute(JsonNode payload, TaskExecutionContext context) {
        int failAttempts = 2;
        String customError = "Simulated transient hardware/network fault";
        if (payload != null) {
            if (payload.has("failAttempts")) {
                failAttempts = payload.get("failAttempts").asInt(2);
            }
            if (payload.has("error")) {
                customError = payload.get("error").asText(customError);
            }
        }

        if (context.getAttemptNumber() <= failAttempts) {
            log.warn("[FailNTimesHandler] Task [{}] injecting failure on attempt {}/{} (target fail count: {})",
                context.getTaskId(), context.getAttemptNumber(), failAttempts, failAttempts);
            throw new RetryableTaskException(customError + " (Attempt " + context.getAttemptNumber() + "/" + failAttempts + ")");
        }

        log.info("[FailNTimesHandler] Task [{}] succeeded after {} retries on attempt {}",
            context.getTaskId(), failAttempts, context.getAttemptNumber());

        ObjectNode res = MAPPER.createObjectNode();
        res.put("status", "SUCCESS");
        res.put("message", "Task recovered after transient failures");
        res.put("totalAttemptsRequired", context.getAttemptNumber());
        res.put("workerId", context.getWorkerId());
        return res;
    }
}
