package com.nexus.orchestrator.core.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TaskHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(TaskHandlerRegistry.class);
    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    public TaskHandlerRegistry(List<TaskHandler> registeredHandlers) {
        for (TaskHandler handler : registeredHandlers) {
            register(handler);
        }
    }

    public void register(TaskHandler handler) {
        handlers.put(handler.taskType().toUpperCase(), handler);
        log.info("[HandlerRegistry] Registered task handler for type [{}]", handler.taskType().toUpperCase());
    }

    public Optional<TaskHandler> getHandler(String taskType) {
        if (taskType == null) return Optional.empty();
        return Optional.ofNullable(handlers.get(taskType.toUpperCase()));
    }
}
