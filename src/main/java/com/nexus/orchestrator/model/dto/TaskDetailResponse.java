package com.nexus.orchestrator.model.dto;

import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskAttempt;

import java.util.List;

public record TaskDetailResponse(
    TaskResponse task,
    List<TaskAttempt> attempts
) {
    public static TaskDetailResponse of(Task task, List<TaskAttempt> attempts) {
        return new TaskDetailResponse(TaskResponse.fromEntity(task), attempts != null ? attempts : List.of());
    }
}
