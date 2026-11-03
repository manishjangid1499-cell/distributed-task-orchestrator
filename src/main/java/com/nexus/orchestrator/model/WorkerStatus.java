package com.nexus.orchestrator.model;

/**
 * Health and availability states of worker nodes in the distributed cluster.
 */
public enum WorkerStatus {
    STARTING,
    HEALTHY,
    BUSY,
    DRAINING,
    OFFLINE,

    // Fault states
    UNRESPONSIVE,
    CRASHED
}
