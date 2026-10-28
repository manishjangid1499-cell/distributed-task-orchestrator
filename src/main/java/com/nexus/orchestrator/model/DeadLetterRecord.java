package com.nexus.orchestrator.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "dead_letter_records", indexes = {
    @Index(name = "idx_dlq_task_id", columnList = "taskId")
})
public class DeadLetterRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 64)
    private String taskId;

    @Column(nullable = false, length = 128)
    private String taskName;

    @Lob
    @Column(columnDefinition = "TEXT")
    private String originalPayload;

    @Lob
    @Column(columnDefinition = "TEXT", nullable = false)
    private String failureReason;

    @Column(nullable = false)
    private int totalRetryAttempts;

    @Column(nullable = false, updatable = false)
    private Instant quarantinedAt;

    private boolean replayed;

    public DeadLetterRecord() {
        this.quarantinedAt = Instant.now();
        this.replayed = false;
    }

    public DeadLetterRecord(String taskId, String taskName, String payload, String reason, int attempts) {
        this();
        this.taskId = taskId;
        this.taskName = taskName;
        this.originalPayload = payload;
        this.failureReason = reason;
        this.totalRetryAttempts = attempts;
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }

    public String getTaskName() { return taskName; }
    public void setTaskName(String taskName) { this.taskName = taskName; }

    public String getOriginalPayload() { return originalPayload; }
    public void setOriginalPayload(String originalPayload) { this.originalPayload = originalPayload; }

    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }

    public int getTotalRetryAttempts() { return totalRetryAttempts; }
    public void setTotalRetryAttempts(int totalRetryAttempts) { this.totalRetryAttempts = totalRetryAttempts; }

    public Instant getQuarantinedAt() { return quarantinedAt; }
    public void setQuarantinedAt(Instant quarantinedAt) { this.quarantinedAt = quarantinedAt; }

    public boolean isReplayed() { return replayed; }
    public void setReplayed(boolean replayed) { this.replayed = replayed; }
}
