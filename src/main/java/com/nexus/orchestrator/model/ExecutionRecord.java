package com.nexus.orchestrator.model;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "execution_records", indexes = {
    @Index(name = "idx_exec_idempotency_key", columnList = "idempotency_key", unique = true)
})
public class ExecutionRecord {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, unique = true, length = 128)
    private String idempotencyKey;

    @Column(name = "task_id", nullable = false)
    private UUID taskId;

    @Column(name = "worker_id", nullable = false, length = 64)
    private String workerId;

    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt;

    @Column(name = "payload_hash", nullable = false, length = 64)
    private String payloadHash;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "execution_data", columnDefinition = "jsonb")
    private JsonNode executionData;

    public ExecutionRecord() {
        this.id = UUID.randomUUID();
        this.recordedAt = Instant.now();
    }

    public ExecutionRecord(String idempotencyKey, UUID taskId, String workerId, String payloadHash, JsonNode executionData) {
        this();
        this.idempotencyKey = idempotencyKey;
        this.taskId = taskId;
        this.workerId = workerId;
        this.payloadHash = payloadHash;
        this.executionData = executionData;
    }

    // Getters and Setters
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }

    public UUID getTaskId() { return taskId; }
    public void setTaskId(UUID taskId) { this.taskId = taskId; }

    public String getWorkerId() { return workerId; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }

    public Instant getRecordedAt() { return recordedAt; }
    public void setRecordedAt(Instant recordedAt) { this.recordedAt = recordedAt; }

    public String getPayloadHash() { return payloadHash; }
    public void setPayloadHash(String payloadHash) { this.payloadHash = payloadHash; }

    public JsonNode getExecutionData() { return executionData; }
    public void setExecutionData(JsonNode executionData) { this.executionData = executionData; }
}
