package com.nexus.orchestrator.model;

/**
 * Health and availability states of worker nodes in the cluster.
 */
public enum WorkerStatus {
    HEALTHY,
    BUSY,
    UNRESPONSIVE,
    CRASHED,
    OFFLINE
}
