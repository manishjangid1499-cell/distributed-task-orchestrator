package com.nexus.orchestrator.model;

/**
 * Persistent lifecycle states of an orchestrated task.
 */
public enum TaskStatus {
    PENDING,
    RUNNING,
    RETRY_WAIT,
    COMPLETED,
    DEAD_LETTERED,
    CANCELLED,

    // Aliases for backward compatibility
    SUBMITTED,
    QUEUED,
    FAILED,
    RETRYING;

    public boolean isTerminal() {
        return this == COMPLETED || this == DEAD_LETTERED || this == CANCELLED;
    }
}
