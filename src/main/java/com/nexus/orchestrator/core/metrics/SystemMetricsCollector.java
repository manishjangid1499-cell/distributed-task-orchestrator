package com.nexus.orchestrator.core.metrics;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

@Component
public class SystemMetricsCollector {

    private final LongAdder totalSubmitted = new LongAdder();
    private final LongAdder totalCompleted = new LongAdder();
    private final LongAdder totalFailed = new LongAdder();
    private final LongAdder totalRetried = new LongAdder();
    private final LongAdder totalDeadLettered = new LongAdder();
    private final LongAdder rateLimitViolations = new LongAdder();

    private final LongAdder totalExecutionLatencyMs = new LongAdder();
    private final AtomicInteger currentQueueDepth = new AtomicInteger(0);

    public void incrementSubmitted() { totalSubmitted.increment(); }
    public void incrementCompleted(long durationMs) {
        totalCompleted.increment();
        totalExecutionLatencyMs.add(durationMs);
    }
    public void incrementFailed() { totalFailed.increment(); }
    public void incrementRetried() { totalRetried.increment(); }
    public void incrementDeadLettered() { totalDeadLettered.increment(); }
    public void incrementRateLimitViolations() { rateLimitViolations.increment(); }

    public void setQueueDepth(int depth) { currentQueueDepth.set(depth); }

    public long getTotalSubmitted() { return totalSubmitted.sum(); }
    public long getTotalCompleted() { return totalCompleted.sum(); }
    public long getTotalFailed() { return totalFailed.sum(); }
    public long getTotalRetried() { return totalRetried.sum(); }
    public long getTotalDeadLettered() { return totalDeadLettered.sum(); }
    public long getRateLimitViolations() { return rateLimitViolations.sum(); }
    public int getQueueDepth() { return currentQueueDepth.get(); }

    public double getAverageLatencyMs() {
        long completed = totalCompleted.sum();
        if (completed == 0) return 0.0;
        return (double) totalExecutionLatencyMs.sum() / completed;
    }
}
