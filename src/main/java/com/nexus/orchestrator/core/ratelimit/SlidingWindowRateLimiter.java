package com.nexus.orchestrator.core.ratelimit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe Sliding Window Rate Limiter.
 * Uses atomic locks per client to track request timestamps within a sliding time window.
 */
@Component
public class SlidingWindowRateLimiter implements RateLimiter {

    private final long defaultLimit;
    private final long windowDurationMs;

    // Per-client sliding window log and fine-grained mutex
    private final Map<String, Deque<Long>> clientRequestLogs = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> clientLocks = new ConcurrentHashMap<>();

    public SlidingWindowRateLimiter(
        @Value("${nexus.scheduler.rate-limit.default-requests-per-window:20}") long defaultLimit,
        @Value("${nexus.scheduler.rate-limit.window-seconds:10}") long windowSeconds
    ) {
        this.defaultLimit = defaultLimit;
        this.windowDurationMs = windowSeconds * 1000L;
    }

    @Override
    public Result tryAcquire(String clientId, int tokens) {
        if (tokens <= 0) {
            return new Result(true, defaultLimit, defaultLimit, windowDurationMs / 1000, 0);
        }

        ReentrantLock lock = clientLocks.computeIfAbsent(clientId, k -> new ReentrantLock());
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            long windowStart = now - windowDurationMs;

            Deque<Long> log = clientRequestLogs.computeIfAbsent(clientId, k -> new ArrayDeque<>());

            // Evict timestamps older than the sliding window boundary
            while (!log.isEmpty() && log.peekFirst() <= windowStart) {
                log.pollFirst();
            }

            int currentUsage = log.size();
            if (currentUsage + tokens <= defaultLimit) {
                // Quota permitted: record timestamps
                for (int i = 0; i < tokens; i++) {
                    log.addLast(now);
                }
                long remaining = defaultLimit - (currentUsage + tokens);
                return new Result(true, remaining, defaultLimit, windowDurationMs / 1000, 0);
            } else {
                // Quota exceeded: calculate retry-after duration
                long oldestTimestamp = log.isEmpty() ? now : log.peekFirst();
                long retryAfterMs = Math.max(0, (oldestTimestamp + windowDurationMs) - now);
                return new Result(false, 0, defaultLimit, windowDurationMs / 1000, retryAfterMs);
            }
        } finally {
            lock.unlock();
        }
    }

    public void resetClient(String clientId) {
        clientRequestLogs.remove(clientId);
        clientLocks.remove(clientId);
    }
}
