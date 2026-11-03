package com.nexus.orchestrator.model;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dead_letter_tasks", indexes = {
    @Index(name = "idx_dlq_created_at", columnList = "created_at"),
    @Index(name = "idx_dlq_original_task_id", columnList = "original_task_id", unique = true)
})
public class DeadLetterTask {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "original_task_id", nullable = false, unique = true)
    private UUID originalTaskId;

    @Column(name = "task_type", nullable = false, length = 128)
    private String taskType;

    @Column(name = "task_name", length = 128)
    private String taskName;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", nullable = false)
    private JsonNode payload;

    @Column(nullable = false)
    private short priority;

    @Column(name = "total_attempts", nullable = false)
    private int totalAttempts;

    @Column(name = "final_error", columnDefinition = "TEXT")
    private String finalError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "replayed_at")
    private Instant replayedAt;

    @Column(name = "replayed_by", length = 64)
    private String replayedBy;

    public DeadLetterTask() {
        this.id = UUID.randomUUID();
        this.createdAt = Instant.now();
    }

    public DeadLetterTask(UUID originalTaskId, String taskType, String taskName, JsonNode payload, short priority, int totalAttempts, String finalError) {
        this();
        this.originalTaskId = originalTaskId;
        this.taskType = taskType;
        this.taskName = taskName != null ? taskName : taskType;
        this.payload = payload;
        this.priority = priority;
        this.totalAttempts = totalAttempts;
        this.finalError = finalError;
    }

    // Getters and Setters
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public UUID getOriginalTaskId() { return originalTaskId; }
    public void setOriginalTaskId(UUID originalTaskId) { this.originalTaskId = originalTaskId; }

    public String getTaskId() { return originalTaskId != null ? originalTaskId.toString() : null; }

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public String getTaskName() { return taskName; }
    public void setTaskName(String taskName) { this.taskName = taskName; }

    public JsonNode getPayload() { return payload; }
    public void setPayload(JsonNode payload) { this.payload = payload; }

    public short getPriority() { return priority; }
    public void setPriority(short priority) { this.priority = priority; }

    public int getTotalAttempts() { return totalAttempts; }
    public void setTotalAttempts(int totalAttempts) { this.totalAttempts = totalAttempts; }

    public int getTotalRetryAttempts() { return totalAttempts; }

    public String getFinalError() { return finalError; }
    public void setFinalError(String finalError) { this.finalError = finalError; }

    public String getFailureReason() { return finalError; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getQuarantinedAt() { return createdAt; }

    public Instant getReplayedAt() { return replayedAt; }
    public void setReplayedAt(Instant replayedAt) { this.replayedAt = replayedAt; }

    public String getReplayedBy() { return replayedBy; }
    public void setReplayedBy(String replayedBy) { this.replayedBy = replayedBy; }

    public boolean isReplayed() { return replayedAt != null; }
}
