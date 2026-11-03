package com.nexus.orchestrator.model.dto;

import com.nexus.orchestrator.model.TaskPriority;
import jakarta.validation.constraints.Size;

public class TaskSubmissionRequest {

    @Size(max = 128, message = "Task name must not exceed 128 characters")
    private String taskName;

    private String taskType;

    private String payload;

    private TaskPriority priority = TaskPriority.NORMAL;

    private String idempotencyKey;

    private String submittedBy = "default-client";

    private int maxAttempts = 3;

    private long simulatedExecutionDurationMs = 200;

    private boolean shouldSimulateFailure = false;

    public TaskSubmissionRequest() {}

    public TaskSubmissionRequest(String taskName, String payload, TaskPriority priority) {
        this.taskName = taskName;
        this.taskType = taskName;
        this.payload = payload;
        this.priority = priority;
    }

    public String getTaskName() {
        if (taskName != null && !taskName.isBlank()) return taskName;
        if (taskType != null && !taskType.isBlank()) return taskType;
        return "NO_OP";
    }

    public void setTaskName(String taskName) { this.taskName = taskName; }

    public String getTaskType() {
        if (taskType != null && !taskType.isBlank()) return taskType;
        if (taskName != null && !taskName.isBlank()) return taskName;
        return "NO_OP";
    }

    public void setTaskType(String taskType) { this.taskType = taskType; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public TaskPriority getPriority() { return priority != null ? priority : TaskPriority.NORMAL; }
    public void setPriority(TaskPriority priority) { this.priority = priority; }

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }

    public String getSubmittedBy() { return submittedBy != null ? submittedBy : "default-client"; }
    public void setSubmittedBy(String submittedBy) { this.submittedBy = submittedBy; }

    public int getMaxAttempts() { return maxAttempts > 0 ? maxAttempts : 3; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public long getSimulatedExecutionDurationMs() { return simulatedExecutionDurationMs; }
    public void setSimulatedExecutionDurationMs(long simulatedExecutionDurationMs) { this.simulatedExecutionDurationMs = simulatedExecutionDurationMs; }

    public boolean isShouldSimulateFailure() { return shouldSimulateFailure; }
    public void setShouldSimulateFailure(boolean shouldSimulateFailure) { this.shouldSimulateFailure = shouldSimulateFailure; }
}
