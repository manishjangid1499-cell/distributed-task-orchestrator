package com.nexus.orchestrator.model.dto;

import com.nexus.orchestrator.model.TaskPriority;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public class TaskSubmissionRequest {

    @NotBlank(message = "Task name is required")
    @Size(max = 128, message = "Task name must not exceed 128 characters")
    private String taskName;

    private String payload;

    private TaskPriority priority = TaskPriority.NORMAL;

    private String submittedBy = "default-client";

    private long simulatedExecutionDurationMs = 200;

    private boolean shouldSimulateFailure = false;

    public TaskSubmissionRequest() {}

    public String getTaskName() { return taskName; }
    public void setTaskName(String taskName) { this.taskName = taskName; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public TaskPriority getPriority() { return priority; }
    public void setPriority(TaskPriority priority) { this.priority = priority; }

    public String getSubmittedBy() { return submittedBy; }
    public void setSubmittedBy(String submittedBy) { this.submittedBy = submittedBy; }

    public long getSimulatedExecutionDurationMs() { return simulatedExecutionDurationMs; }
    public void setSimulatedExecutionDurationMs(long simulatedExecutionDurationMs) { this.simulatedExecutionDurationMs = simulatedExecutionDurationMs; }

    public boolean isShouldSimulateFailure() { return shouldSimulateFailure; }
    public void setShouldSimulateFailure(boolean shouldSimulateFailure) { this.shouldSimulateFailure = shouldSimulateFailure; }
}
