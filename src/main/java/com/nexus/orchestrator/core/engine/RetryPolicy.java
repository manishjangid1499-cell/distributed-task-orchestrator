package com.nexus.orchestrator.core.engine;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Retry Policy implementing Full-Jitter Exponential Backoff.
 * delay = random(0, min(maxDelay, baseDelay * 2^(attempt - 1)))
 */
@Component
public class RetryPolicy {

    private final int baseDelaySeconds;
    private final int maxDelaySeconds;

    public RetryPolicy(
        @Value("${nexus.scheduler.retry.base-delay-seconds:2}") int baseDelaySeconds,
        @Value("${nexus.scheduler.retry.max-delay-seconds:60}") int maxDelaySeconds
    ) {
        this.baseDelaySeconds = baseDelaySeconds;
        this.maxDelaySeconds = maxDelaySeconds;
    }

    /**
     * Calculates the retry backoff duration in seconds using full jitter.
     */
    public int calculateDelaySeconds(int attemptNumber) {
        return calculateDelaySeconds(attemptNumber, baseDelaySeconds, maxDelaySeconds);
    }

    public static int calculateDelaySeconds(int attemptNumber, int baseDelay, int maxDelay) {
        if (attemptNumber <= 0) {
            return 0;
        }
        long exponential = (long) (baseDelay * Math.pow(2, attemptNumber - 1));
        long ceiling = Math.min(maxDelay, Math.max(baseDelay, exponential));
        if (ceiling <= 0) return 0;
        return ThreadLocalRandom.current().nextInt(0, (int) ceiling + 1);
    }

    /**
     * Determines whether an exception represents a retryable or terminal failure.
     */
    public boolean isRetryable(Throwable throwable) {
        if (throwable == null) return false;
        if (throwable instanceof NonRetryableTaskException) {
            return false;
        }
        if (throwable.getCause() != null && throwable.getCause() instanceof NonRetryableTaskException) {
            return false;
        }
        return true;
    }
}
