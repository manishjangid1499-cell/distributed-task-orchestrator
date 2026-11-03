package com.nexus.orchestrator.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "tasks", indexes = {
    @Index(name = "idx_tasks_ready_claim", columnList = "status, available_at, priority, created_at"),
    @Index(name = "idx_tasks_expired_lease", columnList = "status, lease_until, attempt_count"),
    @Index(name = "idx_tasks_status", columnList = "status"),
    @Index(name = "idx_tasks_locked_by", columnList = "locked_by"),
    @Index(name = "idx_tasks_idempotency_key", columnList = "idempotency_key")
})
public class Task implements Comparable<Task> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "task_type", nullable = false, length = 128)
    private String taskType;

    @Column(name = "task_name", length = 128)
    private String taskName;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", nullable = false)
    private JsonNode payload;

    @Column(nullable = false)
    private TaskPriority priority;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private TaskStatus status;

    @Column(name = "idempotency_key", unique = true, length = 128)
    private String idempotencyKey;

    @Column(name = "payload_hash", nullable = false, length = 64)
    private String payloadHash;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts;

    @Column(name = "available_at", nullable = false)
    private Instant availableAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "locked_by", length = 64)
    private String lockedBy;

    @Column(name = "lease_until")
    private Instant leaseUntil;

    @Column(name = "fencing_token", nullable = false)
    private long fencingToken;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private JsonNode result;

    @Column(name = "submitted_by", length = 64)
    private String submittedBy;

    @Column(name = "execution_duration_ms")
    private Long executionDurationMs;

    public Task() {
        this.id = UUID.randomUUID();
        this.taskType = "NO_OP";
        this.taskName = "NO_OP";
        this.priority = TaskPriority.NORMAL;
        this.status = TaskStatus.PENDING;
        this.attemptCount = 0;
        this.maxAttempts = 3;
        this.fencingToken = 0L;
        Instant now = Instant.now();
        this.createdAt = now;
        this.availableAt = now;
        this.payload = OBJECT_MAPPER.createObjectNode();
        this.payloadHash = "";
    }

    public Task(String taskType, String taskName, JsonNode payload, TaskPriority priority, String idempotencyKey, String payloadHash, int maxAttempts, String submittedBy) {
        this();
        this.taskType = taskType != null ? taskType : "NO_OP";
        this.taskName = taskName != null ? taskName : this.taskType;
        this.payload = payload != null ? payload : OBJECT_MAPPER.createObjectNode();
        this.priority = priority != null ? priority : TaskPriority.NORMAL;
        this.idempotencyKey = idempotencyKey;
        this.payloadHash = payloadHash != null ? payloadHash : "";
        this.maxAttempts = maxAttempts > 0 ? maxAttempts : 3;
        this.submittedBy = submittedBy != null ? submittedBy : "default-client";
    }

    public Task(String taskName, String payload, TaskPriority priority, String submittedBy, long durationMs) {
        this();
        this.taskName = taskName;
        this.taskType = taskName != null ? taskName : "NO_OP";
        setPayloadFromString(payload);
        this.priority = priority != null ? priority : TaskPriority.NORMAL;
        this.submittedBy = submittedBy != null ? submittedBy : "default-client";
        this.executionDurationMs = durationMs > 0 ? durationMs : 200L;
    }

    public long getSimulatedExecutionDurationMs() {
        return executionDurationMs != null ? executionDurationMs : 200L;
    }

    public void setSimulatedExecutionDurationMs(long durationMs) {
        this.executionDurationMs = durationMs;
    }

    // Priority-based order: lower level integer first, tie-break FIFO by createdAt
    @Override
    public int compareTo(Task other) {
        if (other == null) return -1;
        int priorityCompare = Integer.compare(this.priority.getLevel(), other.priority.getLevel());
        if (priorityCompare != 0) {
            return priorityCompare;
        }
        return this.createdAt.compareTo(other.createdAt);
    }

    // Getters and Setters
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public String getTaskId() { return id != null ? id.toString() : null; }
    public void setTaskId(String taskId) {
        if (taskId != null) {
            this.id = UUID.fromString(taskId);
        }
    }

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public String getTaskName() { return taskName; }
    public void setTaskName(String taskName) { this.taskName = taskName; }

    public JsonNode getPayload() { return payload; }
    public void setPayload(JsonNode payload) { this.payload = payload; }

    public String getPayloadAsString() {
        return payload != null ? payload.toString() : null;
    }

    public void setPayloadFromString(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            this.payload = OBJECT_MAPPER.createObjectNode();
            return;
        }
        try {
            this.payload = OBJECT_MAPPER.readTree(rawPayload);
        } catch (Exception e) {
            this.payload = new TextNode(rawPayload);
        }
    }

    public TaskPriority getPriority() { return priority; }
    public void setPriority(TaskPriority priority) { this.priority = priority; }

    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }

    public String getPayloadHash() { return payloadHash; }
    public void setPayloadHash(String payloadHash) { this.payloadHash = payloadHash; }

    public int getAttemptCount() { return attemptCount; }
    public void setAttemptCount(int attemptCount) { this.attemptCount = attemptCount; }

    public int getRetryCount() { return attemptCount; }
    public void setRetryCount(int retryCount) { this.attemptCount = retryCount; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public int getMaxRetries() { return maxAttempts; }
    public void setMaxRetries(int maxRetries) { this.maxAttempts = maxRetries; }

    public Instant getAvailableAt() { return availableAt; }
    public void setAvailableAt(Instant availableAt) { this.availableAt = availableAt; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }

    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }

    public String getLockedBy() { return lockedBy; }
    public void setLockedBy(String lockedBy) { this.lockedBy = lockedBy; }

    public String getAssignedWorkerId() { return lockedBy; }
    public void setAssignedWorkerId(String assignedWorkerId) { this.lockedBy = assignedWorkerId; }

    public Instant getLeaseUntil() { return leaseUntil; }
    public void setLeaseUntil(Instant leaseUntil) { this.leaseUntil = leaseUntil; }

    public long getFencingToken() { return fencingToken; }
    public void setFencingToken(long fencingToken) { this.fencingToken = fencingToken; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public String getErrorMessage() { return lastError; }
    public void setErrorMessage(String errorMessage) { this.lastError = errorMessage; }

    public JsonNode getResult() { return result; }
    public void setResult(JsonNode result) { this.result = result; }

    public String getSubmittedBy() { return submittedBy; }
    public void setSubmittedBy(String submittedBy) { this.submittedBy = submittedBy; }

    public Long getExecutionDurationMs() { return executionDurationMs; }
    public void setExecutionDurationMs(Long executionDurationMs) { this.executionDurationMs = executionDurationMs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task task = (Task) o;
        return Objects.equals(id, task.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
