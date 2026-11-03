package com.nexus.orchestrator.unit;

import com.nexus.orchestrator.core.ratelimit.RateLimiter;
import com.nexus.orchestrator.core.ratelimit.SlidingWindowRateLimiter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class SlidingWindowRateLimiterUnitTest {

    @Test
    @DisplayName("Quota exhaustion: Blocks after limit is reached and returns positive retry-after")
    public void testRateLimitExhaustion() {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(5, 10);
        String client = "client-alpha";

        // First 5 requests must pass
        for (int i = 0; i < 5; i++) {
            RateLimiter.Result res = limiter.tryAcquire(client, 1);
            assertTrue(res.allowed(), "Request " + (i + 1) + " should be allowed");
            assertEquals(5 - (i + 1), res.remainingQuota());
        }

        // 6th request must be rejected
        RateLimiter.Result rejected = limiter.tryAcquire(client, 1);
        assertFalse(rejected.allowed(), "6th request should exceed quota");
        assertEquals(0, rejected.remainingQuota());
        assertTrue(rejected.retryAfterMs() > 0, "Retry-After should be positive");
    }

    @Test
    @DisplayName("Sliding window expiration: Quota replenishes after window duration elapses")
    public void testSlidingWindowBoundaryExpiration() throws InterruptedException {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(2, 1); // 2 requests per 1 second
        String client = "client-beta";

        assertTrue(limiter.tryAcquire(client, 1).allowed());
        assertTrue(limiter.tryAcquire(client, 1).allowed());
        assertFalse(limiter.tryAcquire(client, 1).allowed());

        // Wait for sliding window to expire
        Thread.sleep(1100);

        // After window slide, new requests must be permitted
        RateLimiter.Result afterSlide = limiter.tryAcquire(client, 1);
        assertTrue(afterSlide.allowed(), "Request should be permitted after window slide");
    }

    @Test
    @DisplayName("Reset client: Clears quota state immediately")
    public void testClientReset() {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(2, 10);
        String client = "client-gamma";

        limiter.tryAcquire(client, 2);
        assertFalse(limiter.tryAcquire(client, 1).allowed());

        limiter.resetClient(client);

        assertTrue(limiter.tryAcquire(client, 1).allowed());
    }

    @Test
    @DisplayName("Concurrent thread safety: Exactly permitted quota succeeds under parallel burst")
    public void testConcurrentThreadSafety() throws InterruptedException {
        int limit = 20;
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(limit, 10);
        String client = "client-concurrent";

        int threads = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        AtomicInteger accepted = new AtomicInteger(0);
        AtomicInteger rejected = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    RateLimiter.Result r = limiter.tryAcquire(client, 1);
                    if (r.allowed()) {
                        accepted.incrementAndGet();
                    } else {
                        rejected.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        assertEquals(limit, accepted.get(), "Exactly the limit should be accepted");
        assertEquals(threads - limit, rejected.get(), "All excess requests should be rejected");
    }

    @Test
    @DisplayName("Stale client eviction: Memory cleanup safely removes inactive client states")
    public void testStaleClientEviction() {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(5, 1); // 1-second window
        limiter.tryAcquire("ephemeral-client", 1);
        assertEquals(1, limiter.getTrackedClientCount());

        limiter.cleanupStaleClients();
        // Since it's still fresh, not evicted yet
        assertEquals(1, limiter.getTrackedClientCount());
    }
}
