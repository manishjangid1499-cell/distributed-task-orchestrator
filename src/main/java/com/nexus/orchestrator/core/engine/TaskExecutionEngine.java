package com.nexus.orchestrator.core.engine;

import com.nexus.orchestrator.core.metrics.SystemMetricsCollector;
import com.nexus.orchestrator.model.DeadLetterRecord;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.repository.DeadLetterRepository;
import com.nexus.orchestrator.repository.TaskRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class TaskExecutionEngine {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutionEngine.class);

    private final TaskRepository taskRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final SystemMetricsCollector metrics;

    private final int corePoolSize;
    private final int maxPoolSize;
    private final int queueCapacity;

    private ThreadPoolExecutor executor;
    private PriorityBlockingQueue<Runnable> priorityQueue;

    public TaskExecutionEngine(
        TaskRepository taskRepository,
        DeadLetterRepository deadLetterRepository,
        SystemMetricsCollector metrics,
        @Value("${nexus.scheduler.core-pool-size:8}") int corePoolSize,
        @Value("${nexus.scheduler.max-pool-size:32}") int maxPoolSize,
        @Value("${nexus.scheduler.queue-capacity:1000}") int queueCapacity
    ) {
        this.taskRepository = taskRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.metrics = metrics;
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.queueCapacity = queueCapacity;
    }

    @PostConstruct
    public void initialize() {
        this.priorityQueue = new PriorityBlockingQueue<>(queueCapacity);

        AtomicInteger threadId = new AtomicInteger(1);
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "nexus-worker-" + threadId.getAndIncrement());
            t.setDaemon(true);
            return t;
        };

        RejectedExecutionHandler rejectionHandler = (r, exec) -> {
            log.error("[Engine] Task rejected! Priority queue saturated (capacity={})", queueCapacity);
            metrics.incrementFailed();
            throw new RejectedExecutionException("Nexus Task Queue capacity exceeded. Throttling active.");
        };

        this.executor = new ThreadPoolExecutor(
            corePoolSize,
            maxPoolSize,
            60L,
            TimeUnit.SECONDS,
            priorityQueue,
            threadFactory,
            rejectionHandler
        );

        log.info("[Engine] Task Execution Engine initialized with corePoolSize={}, maxPoolSize={}, queueCapacity={}",
            corePoolSize, maxPoolSize, queueCapacity);
    }

    public void submit(Task task, String workerId) {
        task.setStatus(TaskStatus.QUEUED);
        task.setAssignedWorkerId(workerId);
        taskRepository.save(task);

        metrics.incrementSubmitted();
        metrics.setQueueDepth(priorityQueue.size());

        PrioritizedTaskRunnable runnable = new PrioritizedTaskRunnable(task, () -> executeTask(task));
        executor.execute(runnable);
        log.info("[Engine] Enqueued Task [{}] with Priority: {}", task.getTaskId(), task.getPriority());
    }

    private void executeTask(Task task) {
        long startTime = System.currentTimeMillis();
        task.setStatus(TaskStatus.RUNNING);
        task.setStartedAt(Instant.now());
        taskRepository.save(task);

        log.info("[Engine] Thread [{}] executing Task [{}] ({})",
            Thread.currentThread().getName(), task.getTaskId(), task.getTaskName());

        try {
            // Check if simulated failure requested
            if (task.getPayload() != null && task.getPayload().contains("SIMULATE_FAILURE")) {
                throw new RuntimeException("Simulated hardware/IO fault in execution pipeline");
            }

            // Simulate actual CPU/IO workload duration
            long duration = task.getSimulatedExecutionDurationMs() > 0 ? task.getSimulatedExecutionDurationMs() : 100;
            Thread.sleep(duration);

            long totalDuration = System.currentTimeMillis() - startTime;
            task.setStatus(TaskStatus.COMPLETED);
            task.setCompletedAt(Instant.now());
            task.setExecutionDurationMs(totalDuration);
            taskRepository.save(task);

            metrics.incrementCompleted(totalDuration);
            log.info("[Engine] Task [{}] COMPLETED in {}ms", task.getTaskId(), totalDuration);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handleFailure(task, "Task thread interrupted during execution");
        } catch (Exception e) {
            handleFailure(task, e.getMessage());
        } finally {
            metrics.setQueueDepth(priorityQueue.size());
        }
    }

    private void handleFailure(Task task, String errorMessage) {
        task.setErrorMessage(errorMessage);
        int retries = task.getRetryCount() + 1;
        task.setRetryCount(retries);

        if (retries <= task.getMaxRetries()) {
            task.setStatus(TaskStatus.RETRYING);
            taskRepository.save(task);
            metrics.incrementRetried();
            log.warn("[Engine] Task [{}] failed (attempt {}/{}). Error: {}. Scheduling exponential retry...",
                task.getTaskId(), retries, task.getMaxRetries(), errorMessage);

            // Exponential backoff delay
            long backoffMs = (long) (Math.pow(2, retries) * 500);
            CompletableFuture.delayedExecutor(backoffMs, TimeUnit.MILLISECONDS)
                .execute(() -> submit(task, task.getAssignedWorkerId()));

        } else {
            task.setStatus(TaskStatus.DEAD_LETTERED);
            taskRepository.save(task);
            metrics.incrementFailed();
            metrics.incrementDeadLettered();

            // Quarantine into Dead Letter Repository
            DeadLetterRecord dlq = new DeadLetterRecord(
                task.getTaskId(),
                task.getTaskName(),
                task.getPayload(),
                errorMessage,
                retries
            );
            deadLetterRepository.save(dlq);
            log.error("[Engine] Task [{}] exceeded max retries! Quarantined to DEAD-LETTER QUEUE.", task.getTaskId());
        }
    }

    public int getQueueSize() {
        return priorityQueue != null ? priorityQueue.size() : 0;
    }

    public int getActiveCount() {
        return executor != null ? executor.getActiveCount() : 0;
    }

    @PreDestroy
    public void shutdown() {
        log.info("[Engine] Initiating graceful shutdown of TaskExecutionEngine...");
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("[Engine] Engine shutdown complete.");
    }
}
