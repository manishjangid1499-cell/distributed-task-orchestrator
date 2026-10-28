package com.nexus.orchestrator.model.dto;

public record RateLimitResponse(
    String clientId,
    boolean allowed,
    long remainingQuota,
    long limit,
    long windowSeconds,
    long retryAfterMs
) {}
