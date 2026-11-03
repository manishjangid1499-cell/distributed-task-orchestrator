package com.nexus.orchestrator.model.dto;

public record MetricsSummaryResponse(
    long totalTasksSubmitted,
    long totalTasksCompleted,
    long totalTasksFailed,
    long totalTasksRetried,
    long totalDeadLettered,
    int queueDepth,
    int activeWorkerCount,
    int healthyWorkerCount,
    double averageExecutionLatencyMs,
    long rateLimitViolationsCount,
    long pendingTasksCount,
    long runningTasksCount
) {
    public MetricsSummaryResponse(
        long totalTasksSubmitted,
        long totalTasksCompleted,
        long totalTasksFailed,
        long totalTasksRetried,
        long totalDeadLettered,
        int queueDepth,
        int activeWorkerCount,
        int healthyWorkerCount,
        double averageExecutionLatencyMs,
        long rateLimitViolationsCount
    ) {
        this(
            totalTasksSubmitted,
            totalTasksCompleted,
            totalTasksFailed,
            totalTasksRetried,
            totalDeadLettered,
            queueDepth,
            activeWorkerCount,
            healthyWorkerCount,
            averageExecutionLatencyMs,
            rateLimitViolationsCount,
            queueDepth,
            0
        );
    }
}
