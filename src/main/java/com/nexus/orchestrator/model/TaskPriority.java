package com.nexus.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Task priority levels for priority-based task scheduling.
 * Lower level integer denotes higher priority (CRITICAL = 0 claimed first).
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

    @JsonValue
    public String getCode() {
        return name();
    }

    @JsonCreator
    public static TaskPriority fromString(String value) {
        if (value == null) {
            return NORMAL;
        }
        for (TaskPriority p : values()) {
            if (p.name().equalsIgnoreCase(value)) {
                return p;
            }
        }
        try {
            int level = Integer.parseInt(value);
            return fromLevel(level);
        } catch (NumberFormatException e) {
            return NORMAL;
        }
    }

    public static TaskPriority fromLevel(int level) {
        for (TaskPriority p : values()) {
            if (p.level == level) {
                return p;
            }
        }
        return NORMAL;
    }
}
