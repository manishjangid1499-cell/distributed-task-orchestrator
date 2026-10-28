package com.nexus.orchestrator.model;

/**
 * Task priority levels for preemptive priority queue scheduling.
 * Lower level integer denotes higher priority (CRITICAL = 0 executes first).
 */
public enum TaskPriority {
    CRITICAL(0),
    HIGH(1),
    NORMAL(2),
    LOW(3);

    private final int level;

    TaskPriority(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }
}
