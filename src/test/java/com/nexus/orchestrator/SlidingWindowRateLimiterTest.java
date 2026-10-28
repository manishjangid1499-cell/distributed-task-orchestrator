package com.nexus.orchestrator;

import com.nexus.orchestrator.core.ratelimit.RateLimiter;
import com.nexus.orchestrator.core.ratelimit.SlidingWindowRateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class SlidingWindowRateLimiterTest {

    private SlidingWindowRateLimiter rateLimiter;

    @BeforeEach
    public void setup() {
        // Limit: 5 requests per 2-second window
        rateLimiter = new SlidingWindowRateLimiter(5, 2);
    }

    @Test
    public void testAllowsRequestsWithinLimit() {
        String client = "client-alpha";
        for (int i = 0; i < 5; i++) {
            RateLimiter.Result res = rateLimiter.tryAcquire(client, 1);
            assertTrue(res.allowed(), "Request " + i + " should be allowed");
        }

        // 6th request must be rejected
        RateLimiter.Result rejected = rateLimiter.tryAcquire(client, 1);
        assertFalse(rejected.allowed(), "6th request must exceed quota");
        assertTrue(rejected.retryAfterMs() > 0, "Retry-After must be positive");
    }

    @Test
    public void testThreadSafetyUnderConcurrentLoad() throws InterruptedException {
        String client = "concurrent-client";
        int totalThreads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch latch = new CountDownLatch(totalThreads);

        AtomicInteger allowedCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);

        for (int i = 0; i < totalThreads; i++) {
            executor.submit(() -> {
                try {
                    RateLimiter.Result res = rateLimiter.tryAcquire(client, 1);
                    if (res.allowed()) {
                        allowedCount.incrementAndGet();
                    } else {
                        rejectedCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        // Exactly 5 must be allowed, exactly 15 must be rejected
        assertEquals(5, allowedCount.get(), "Strictly 5 concurrent requests allowed by quota");
        assertEquals(15, rejectedCount.get(), "Strictly 15 requests rejected");
    }
}
