package com.nexus.orchestrator.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "worker_nodes")
public class WorkerNode {

    @Id
    @Column(nullable = false, unique = true, length = 64)
    private String workerId;

    @Column(nullable = false, length = 128)
    private String hostname;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private WorkerStatus status;

    @Column(nullable = false)
    private Instant lastHeartbeat;

    @Column(nullable = false)
    private int activeTaskCount;

    @Column(nullable = false)
    private int maxCapacity;

    @Column(nullable = false)
    private long totalProcessedCount;

    public WorkerNode() {
        this.status = WorkerStatus.HEALTHY;
        this.lastHeartbeat = Instant.now();
        this.activeTaskCount = 0;
        this.maxCapacity = 10;
        this.totalProcessedCount = 0;
    }

    public WorkerNode(String workerId, String hostname, int maxCapacity) {
        this();
        this.workerId = workerId;
        this.hostname = hostname;
        this.maxCapacity = maxCapacity > 0 ? maxCapacity : 10;
    }

    public void recordHeartbeat() {
        this.lastHeartbeat = Instant.now();
        if (this.status == WorkerStatus.UNRESPONSIVE || this.status == WorkerStatus.CRASHED) {
            this.status = WorkerStatus.HEALTHY;
        }
    }

    public boolean isOverCapacity() {
        return this.activeTaskCount >= this.maxCapacity;
    }

    // Getters and Setters
    public String getWorkerId() { return workerId; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public WorkerStatus getStatus() { return status; }
    public void setStatus(WorkerStatus status) { this.status = status; }

    public Instant getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(Instant lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }

    public int getActiveTaskCount() { return activeTaskCount; }
    public void setActiveTaskCount(int activeTaskCount) { this.activeTaskCount = activeTaskCount; }

    public int getMaxCapacity() { return maxCapacity; }
    public void setMaxCapacity(int maxCapacity) { this.maxCapacity = maxCapacity; }

    public long getTotalProcessedCount() { return totalProcessedCount; }
    public void setTotalProcessedCount(long totalProcessedCount) { this.totalProcessedCount = totalProcessedCount; }
}
