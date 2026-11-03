package com.nexus.orchestrator.core.engine.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexus.orchestrator.core.engine.TaskExecutionContext;
import com.nexus.orchestrator.core.engine.TaskHandler;
import org.springframework.stereotype.Component;

@Component
public class NoOpTaskHandler implements TaskHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String taskType() {
        return "NO_OP";
    }

    @Override
    public JsonNode execute(JsonNode payload, TaskExecutionContext context) {
        ObjectNode res = MAPPER.createObjectNode();
        res.put("status", "SUCCESS");
        res.put("message", "No-op task executed successfully");
        res.put("workerId", context.getWorkerId());
        res.put("attempt", context.getAttemptNumber());
        return res;
    }
}
