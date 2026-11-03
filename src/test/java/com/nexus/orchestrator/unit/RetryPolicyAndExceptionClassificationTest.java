package com.nexus.orchestrator.unit;

import com.nexus.orchestrator.core.engine.NonRetryableTaskException;
import com.nexus.orchestrator.core.engine.RetryPolicy;
import com.nexus.orchestrator.core.engine.RetryableTaskException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class RetryPolicyAndExceptionClassificationTest {

    private final RetryPolicy retryPolicy = new RetryPolicy(2, 30);

    @Test
    @DisplayName("Retry delay calculation: Generates full-jitter backoff bounded between 0 and max backoff")
    public void testFullJitterRetryDelayBounds() {
        int baseDelay = 2;
        int maxDelay = 30;

        for (int attempt = 1; attempt <= 10; attempt++) {
            long maxExpectedBackoff = Math.min(maxDelay, (long) (baseDelay * Math.pow(2, attempt - 1)));
            for (int sample = 0; sample < 50; sample++) {
                int delay = RetryPolicy.calculateDelaySeconds(attempt, baseDelay, maxDelay);
                assertTrue(delay >= 0, "Delay must be non-negative");
                assertTrue(delay <= maxExpectedBackoff, "Delay " + delay + " exceeded max ceiling " + maxExpectedBackoff);
            }
        }
    }

    @Test
    @DisplayName("Exception classification: Distinguishes retryable transient faults from fatal exceptions")
    public void testExceptionClassification() {
        assertTrue(retryPolicy.isRetryable(new RetryableTaskException("Temporary connection timeout")));
        assertTrue(retryPolicy.isRetryable(new IOException("Disk IO transient glitch")));
        assertTrue(retryPolicy.isRetryable(new RuntimeException("Generic unexpected exception")));

        assertFalse(retryPolicy.isRetryable(new NonRetryableTaskException("Invalid task payload format")));
        assertFalse(retryPolicy.isRetryable(new RuntimeException("Wrapped fatal error", new NonRetryableTaskException("Nested cause"))));
    }
}
