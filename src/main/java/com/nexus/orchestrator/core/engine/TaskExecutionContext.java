package com.nexus.orchestrator.core.engine;

import com.nexus.orchestrator.model.TaskPriority;

import java.util.UUID;
import java.util.function.BooleanSupplier;

public class TaskExecutionContext {

    private final UUID taskId;
    private final String workerId;
    private final int attemptNumber;
    private final long fencingToken;
    private final TaskPriority priority;
    private final String idempotencyKey;
    private final BooleanSupplier isOwnershipValid;

    public TaskExecutionContext(
        UUID taskId,
        String workerId,
        int attemptNumber,
        long fencingToken,
        TaskPriority priority,
        String idempotencyKey,
        BooleanSupplier isOwnershipValid
    ) {
        this.taskId = taskId;
        this.workerId = workerId;
        this.attemptNumber = attemptNumber;
        this.fencingToken = fencingToken;
        this.priority = priority;
        this.idempotencyKey = idempotencyKey;
        this.isOwnershipValid = isOwnershipValid != null ? isOwnershipValid : () -> true;
    }

    public UUID getTaskId() {
        return taskId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public long getFencingToken() {
        return fencingToken;
    }

    public TaskPriority getPriority() {
        return priority;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public boolean isOwnershipValid() {
        return isOwnershipValid.getAsBoolean();
    }
}
