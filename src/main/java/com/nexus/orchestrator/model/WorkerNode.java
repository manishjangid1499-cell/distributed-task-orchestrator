package com.nexus.orchestrator.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "workers", indexes = {
    @Index(name = "idx_workers_heartbeat", columnList = "last_heartbeat_at, status")
})
public class WorkerNode {

    @Id
    @Column(name = "worker_id", nullable = false, unique = true, length = 64)
    private String workerId;

    @Column(nullable = false, length = 128)
    private String hostname;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private WorkerStatus status;

    @Column(name = "registered_at", nullable = false)
    private Instant registeredAt;

    @Column(name = "last_heartbeat_at", nullable = false)
    private Instant lastHeartbeatAt;

    @Column(nullable = false)
    private int capacity;

    @Column(name = "active_jobs", nullable = false)
    private int activeJobs;

    @Column(name = "total_processed_count", nullable = false)
    private long totalProcessedCount;

    public WorkerNode() {
        this.status = WorkerStatus.STARTING;
        Instant now = Instant.now();
        this.registeredAt = now;
        this.lastHeartbeatAt = now;
        this.capacity = 10;
        this.activeJobs = 0;
        this.totalProcessedCount = 0;
    }

    public WorkerNode(String workerId, String hostname, int capacity) {
        this();
        this.workerId = workerId;
        this.hostname = hostname != null ? hostname : "unknown-host";
        this.capacity = capacity > 0 ? capacity : 10;
        this.status = WorkerStatus.HEALTHY;
    }

    public void recordHeartbeat() {
        this.lastHeartbeatAt = Instant.now();
        if (this.status == WorkerStatus.UNRESPONSIVE || this.status == WorkerStatus.CRASHED || this.status == WorkerStatus.STARTING) {
            this.status = WorkerStatus.HEALTHY;
        }
    }

    public boolean isOverCapacity() {
        return this.activeJobs >= this.capacity;
    }

    public int getAvailableCapacity() {
        return Math.max(0, this.capacity - this.activeJobs);
    }

    // Getters and Setters
    public String getWorkerId() { return workerId; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public WorkerStatus getStatus() { return status; }
    public void setStatus(WorkerStatus status) { this.status = status; }

    public Instant getRegisteredAt() { return registeredAt; }
    public void setRegisteredAt(Instant registeredAt) { this.registeredAt = registeredAt; }

    public Instant getLastHeartbeatAt() { return lastHeartbeatAt; }
    public void setLastHeartbeatAt(Instant lastHeartbeatAt) { this.lastHeartbeatAt = lastHeartbeatAt; }

    public Instant getLastHeartbeat() { return lastHeartbeatAt; }
    public void setLastHeartbeat(Instant lastHeartbeat) { this.lastHeartbeatAt = lastHeartbeat; }

    public int getCapacity() { return capacity; }
    public void setCapacity(int capacity) { this.capacity = capacity; }

    public int getMaxCapacity() { return capacity; }
    public void setMaxCapacity(int maxCapacity) { this.capacity = maxCapacity; }

    public int getActiveJobs() { return activeJobs; }
    public void setActiveJobs(int activeJobs) { this.activeJobs = activeJobs; }

    public int getActiveTaskCount() { return activeJobs; }
    public void setActiveTaskCount(int activeTaskCount) { this.activeJobs = activeTaskCount; }

    public long getTotalProcessedCount() { return totalProcessedCount; }
    public void setTotalProcessedCount(long totalProcessedCount) { this.totalProcessedCount = totalProcessedCount; }
}
