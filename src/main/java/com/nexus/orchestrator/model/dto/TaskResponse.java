package com.nexus.orchestrator.model.dto;

import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import java.time.Instant;

public record TaskResponse(
    String taskId,
    String taskName,
    TaskPriority priority,
    TaskStatus status,
    String assignedWorkerId,
    int retryCount,
    int maxRetries,
    Instant createdAt,
    Instant startedAt,
    Instant completedAt,
    Long executionDurationMs,
    String errorMessage,
    String submittedBy
) {
    public static TaskResponse fromEntity(Task task) {
        if (task == null) return null;
        return new TaskResponse(
            task.getTaskId(),
            task.getTaskName(),
            task.getPriority(),
            task.getStatus(),
            task.getAssignedWorkerId(),
            task.getRetryCount(),
            task.getMaxRetries(),
            task.getCreatedAt(),
            task.getStartedAt(),
            task.getCompletedAt(),
            task.getExecutionDurationMs(),
            task.getErrorMessage(),
            task.getSubmittedBy()
        );
    }
}
