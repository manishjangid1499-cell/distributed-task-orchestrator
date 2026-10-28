package com.nexus.orchestrator.core.ratelimit;

public interface RateLimiter {

    record Result(boolean allowed, long remainingQuota, long limit, long windowSeconds, long retryAfterMs) {}

    Result tryAcquire(String clientId, int tokens);
}
