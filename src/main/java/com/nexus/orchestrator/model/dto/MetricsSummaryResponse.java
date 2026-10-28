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
    long rateLimitViolationsCount
) {}
