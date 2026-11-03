package com.nexus.orchestrator.core.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-Safe In-Memory Sliding Window Rate Limiter.
 *
 * <p>Scope: Per API process instance. Each API node maintains its own local sliding window counters.
 * This implementation is non-cluster-wide: cluster-wide rate limiting would require shared distributed
 * coordination (e.g., Redis or distributed token-bucket counters).</p>
 *
 * <p>Complexity:
 * - Time: Evicting k expired timestamps from the front of the deque is O(k) total (amortized O(1) per removed entry).
 * - Space: O(N * L) where N is the number of active clients and L is the window request limit.</p>
 */
@Component
public class SlidingWindowRateLimiter implements RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(SlidingWindowRateLimiter.class);

    private final long defaultLimit;
    private final long windowDurationMs;

    private static class ClientState {
        final Deque<Long> requestTimestamps = new ArrayDeque<>();
        final ReentrantLock lock = new ReentrantLock();
        volatile long lastAccessTime = System.currentTimeMillis();
    }

    private final Map<String, ClientState> clientStates = new ConcurrentHashMap<>();

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

        String effectiveClient = (clientId == null || clientId.isBlank()) ? "anonymous" : clientId;
        ClientState state = clientStates.computeIfAbsent(effectiveClient, k -> new ClientState());

        state.lock.lock();
        try {
            long now = System.currentTimeMillis();
            state.lastAccessTime = now;
            long windowStart = now - windowDurationMs;

            Deque<Long> logQueue = state.requestTimestamps;

            // Evict expired timestamps older than window boundary: O(k) where k is the number of expired entries
            while (!logQueue.isEmpty() && logQueue.peekFirst() <= windowStart) {
                logQueue.pollFirst();
            }

            int currentUsage = logQueue.size();
            if (currentUsage + tokens <= defaultLimit) {
                // Quota permitted: record acquisition timestamps
                for (int i = 0; i < tokens; i++) {
                    logQueue.addLast(now);
                }
                long remaining = defaultLimit - (currentUsage + tokens);
                return new Result(true, remaining, defaultLimit, windowDurationMs / 1000, 0);
            } else {
                // Quota exceeded: calculate accurate Retry-After backoff duration
                long oldestTimestamp = logQueue.isEmpty() ? now : logQueue.peekFirst();
                long retryAfterMs = Math.max(0, (oldestTimestamp + windowDurationMs) - now);
                return new Result(false, 0, defaultLimit, windowDurationMs / 1000, retryAfterMs);
            }
        } finally {
            state.lock.unlock();
        }
    }

    public void resetClient(String clientId) {
        if (clientId != null) {
            clientStates.remove(clientId);
        }
    }

    /**
     * Scheduled cleanup to evict inactive client states and prevent memory leakage.
     */
    @Scheduled(fixedRate = 60000)
    public void cleanupStaleClients() {
        long cutoff = System.currentTimeMillis() - (windowDurationMs * 2);
        clientStates.entrySet().removeIf(entry -> {
            ClientState state = entry.getValue();
            if (state.lastAccessTime < cutoff && state.lock.tryLock()) {
                try {
                    return state.requestTimestamps.isEmpty();
                } finally {
                    state.lock.unlock();
                }
            }
            return false;
        });
    }

    public int getTrackedClientCount() {
        return clientStates.size();
    }
}
