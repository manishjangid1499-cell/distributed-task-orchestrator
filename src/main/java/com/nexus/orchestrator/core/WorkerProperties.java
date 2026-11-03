package com.nexus.orchestrator.core;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.UUID;

@Component
@ConfigurationProperties(prefix = "nexus.worker")
public class WorkerProperties {

    /**
     * Runtime execution role: API, WORKER, or ALL.
     */
    private String mode = "ALL";

    /**
     * Unique identifier for this worker node.
     */
    private String id = "worker-" + UUID.randomUUID().toString().substring(0, 8);

    /**
     * Hostname or container identifier.
     */
    private String hostname;

    /**
     * Thread pool size / max concurrent tasks for this worker process.
     */
    private int threads = 8;

    /**
     * Database poll interval in milliseconds when idle.
     */
    private long pollIntervalMs = 1000L;

    /**
     * Duration in seconds for which a task lease is held.
     */
    private int leaseDurationSeconds = 15;

    /**
     * Interval in seconds between heartbeat renewals.
     */
    private int heartbeatIntervalSeconds = 5;

    /**
     * Heartbeat timeout after which a worker is deemed unresponsive/crashed.
     */
    private int heartbeatTimeoutSeconds = 15;

    /**
     * Whether legacy in-memory execution engine is enabled (disabled by default in Nexus 2.0).
     */
    private boolean legacyEngineEnabled = false;

    public WorkerProperties() {
        try {
            this.hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            this.hostname = "nexus-host";
        }
    }

    public boolean isWorkerMode() {
        return "WORKER".equalsIgnoreCase(mode) || "ALL".equalsIgnoreCase(mode);
    }

    public boolean isApiMode() {
        return "API".equalsIgnoreCase(mode) || "ALL".equalsIgnoreCase(mode);
    }

    // Getters and Setters
    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

    public String getId() {
        if (id == null || id.isBlank()) {
            id = "worker-" + UUID.randomUUID().toString().substring(0, 8);
        }
        return id;
    }
    public void setId(String id) { this.id = id; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public int getThreads() { return threads; }
    public void setThreads(int threads) { this.threads = threads; }

    public long getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }

    public int getLeaseDurationSeconds() { return leaseDurationSeconds; }
    public void setLeaseDurationSeconds(int leaseDurationSeconds) { this.leaseDurationSeconds = leaseDurationSeconds; }

    public int getHeartbeatIntervalSeconds() { return heartbeatIntervalSeconds; }
    public void setHeartbeatIntervalSeconds(int heartbeatIntervalSeconds) { this.heartbeatIntervalSeconds = heartbeatIntervalSeconds; }

    public int getHeartbeatTimeoutSeconds() { return heartbeatTimeoutSeconds; }
    public void setHeartbeatTimeoutSeconds(int heartbeatTimeoutSeconds) { this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds; }

    public boolean isLegacyEngineEnabled() { return legacyEngineEnabled; }
    public void setLegacyEngineEnabled(boolean legacyEngineEnabled) { this.legacyEngineEnabled = legacyEngineEnabled; }
}
