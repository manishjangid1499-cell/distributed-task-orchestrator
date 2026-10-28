package com.nexus.orchestrator.model;

/**
 * Lifecycle states of an orchestrated task.
 */
public enum TaskStatus {
    SUBMITTED,
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
    RETRYING,
    DEAD_LETTERED,
    CANCELLED
}
