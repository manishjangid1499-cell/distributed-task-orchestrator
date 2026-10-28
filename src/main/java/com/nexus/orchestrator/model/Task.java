package com.nexus.orchestrator.model;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "tasks", indexes = {
    @Index(name = "idx_task_id", columnList = "taskId", unique = true),
    @Index(name = "idx_task_status", columnList = "status"),
    @Index(name = "idx_task_priority", columnList = "priority"),
    @Index(name = "idx_assigned_worker", columnList = "assignedWorkerId")
})
public class Task implements Comparable<Task> {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, updatable = false, length = 64)
    private String taskId;

    @Column(nullable = false, length = 128)
    private String taskName;

    @Lob
    @Column(columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private TaskPriority priority;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private TaskStatus status;

    @Column(length = 64)
    private String assignedWorkerId;

    @Column(nullable = false)
    private int retryCount;

    @Column(nullable = false)
    private int maxRetries;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    private Instant startedAt;

    private Instant completedAt;

    private Long executionDurationMs;

    @Column(length = 1024)
    private String errorMessage;

    @Column(length = 64)
    private String submittedBy;

    @Column(nullable = false)
    private long simulatedExecutionDurationMs;

    public Task() {
        this.taskId = UUID.randomUUID().toString();
        this.priority = TaskPriority.NORMAL;
        this.status = TaskStatus.SUBMITTED;
        this.retryCount = 0;
        this.maxRetries = 3;
        this.createdAt = Instant.now();
        this.simulatedExecutionDurationMs = 200;
    }

    public Task(String taskName, String payload, TaskPriority priority, String submittedBy, long durationMs) {
        this();
        this.taskName = taskName;
        this.payload = payload;
        this.priority = priority != null ? priority : TaskPriority.NORMAL;
        this.submittedBy = submittedBy != null ? submittedBy : "default-client";
        this.simulatedExecutionDurationMs = durationMs > 0 ? durationMs : 200;
    }

    // Preemptive priority ordering: lower level first, then FIFO by creation time
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
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }

    public String getTaskName() { return taskName; }
    public void setTaskName(String taskName) { this.taskName = taskName; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public TaskPriority getPriority() { return priority; }
    public void setPriority(TaskPriority priority) { this.priority = priority; }

    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }

    public String getAssignedWorkerId() { return assignedWorkerId; }
    public void setAssignedWorkerId(String assignedWorkerId) { this.assignedWorkerId = assignedWorkerId; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }

    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }

    public Long getExecutionDurationMs() { return executionDurationMs; }
    public void setExecutionDurationMs(Long executionDurationMs) { this.executionDurationMs = executionDurationMs; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public String getSubmittedBy() { return submittedBy; }
    public void setSubmittedBy(String submittedBy) { this.submittedBy = submittedBy; }

    public long getSimulatedExecutionDurationMs() { return simulatedExecutionDurationMs; }
    public void setSimulatedExecutionDurationMs(long simulatedExecutionDurationMs) { this.simulatedExecutionDurationMs = simulatedExecutionDurationMs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task task = (Task) o;
        return Objects.equals(taskId, task.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId);
    }
}
