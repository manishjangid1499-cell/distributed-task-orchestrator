package com.nexus.orchestrator.core.engine;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Pluggable execution handler for typed distributed tasks.
 */
public interface TaskHandler {

    /**
     * Unique identifier for the supported task type.
     */
    String taskType();

    /**
     * Executes the task payload within the given context.
     *
     * @param payload Task input payload (JSON)
     * @param context Execution metadata including workerId, attempt, fencing token
     * @return Execution result (JSON)
     * @throws Exception on task failure
     */
    JsonNode execute(JsonNode payload, TaskExecutionContext context) throws Exception;
}
