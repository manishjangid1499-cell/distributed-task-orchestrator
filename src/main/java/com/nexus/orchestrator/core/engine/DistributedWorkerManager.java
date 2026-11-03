package com.nexus.orchestrator.core.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.orchestrator.core.WorkerProperties;
import com.nexus.orchestrator.core.metrics.SystemMetricsCollector;
import com.nexus.orchestrator.model.*;
import com.nexus.orchestrator.repository.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed Worker Manager.
 * Orchestrates atomic job claiming, execution on bounded thread pools,
 * periodic lease renewals, heartbeats, and graceful shutdowns.
 */
@Component
public class DistributedWorkerManager {

    private static final Logger log = LoggerFactory.getLogger(DistributedWorkerManager.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final WorkerProperties properties;
    private final DistributedTaskDao taskDao;
    private final WorkerRepository workerRepository;
    private final TaskAttemptRepository attemptRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final TaskHandlerRegistry handlerRegistry;
    private final RetryPolicy retryPolicy;
    private final SystemMetricsCollector metrics;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeJobs = new AtomicInteger(0);

    // Active tasks tracked for periodic lease renewal: TaskId -> ActiveTaskInfo
    private final Map<UUID, ActiveTaskInfo> activeTasks = new ConcurrentHashMap<>();

    private ThreadPoolExecutor workerPool;
    private ScheduledExecutorService pollingExecutor;
    private ScheduledExecutorService maintenanceExecutor;

    public static class ActiveTaskInfo {
        final UUID taskId;
        final long fencingToken;
        final AtomicBoolean leaseValid = new AtomicBoolean(true);

        public ActiveTaskInfo(UUID taskId, long fencingToken) {
            this.taskId = taskId;
            this.fencingToken = fencingToken;
        }
    }

    public DistributedWorkerManager(
        WorkerProperties properties,
        DistributedTaskDao taskDao,
        WorkerRepository workerRepository,
        TaskAttemptRepository attemptRepository,
        DeadLetterRepository deadLetterRepository,
        TaskHandlerRegistry handlerRegistry,
        RetryPolicy retryPolicy,
        SystemMetricsCollector metrics
    ) {
        this.properties = properties;
        this.taskDao = taskDao;
        this.workerRepository = workerRepository;
        this.attemptRepository = attemptRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.handlerRegistry = handlerRegistry;
        this.retryPolicy = retryPolicy;
        this.metrics = metrics;
    }

    @PostConstruct
    public void start() {
        if (!properties.isWorkerMode()) {
            log.info("[WorkerManager] Skipping worker initialization (runtime mode: {})", properties.getMode());
            return;
        }

        running.set(true);
        String workerId = properties.getId();
        int threads = properties.getThreads();

        log.info("[WorkerManager] Starting worker process [{}] on host [{}] with {} threads (polling every {}ms, lease {}s)",
            workerId, properties.getHostname(), threads, properties.getPollIntervalMs(), properties.getLeaseDurationSeconds());

        // 1. Register worker node in PostgreSQL
        registerWorker(workerId, properties.getHostname(), threads);

        // 2. Initialize bounded worker execution thread pool
        AtomicInteger threadCount = new AtomicInteger(1);
        workerPool = new ThreadPoolExecutor(
            threads,
            threads,
            0L,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            r -> {
                Thread t = new Thread(r, workerId + "-thread-" + threadCount.getAndIncrement());
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 3. Polling loop
        pollingExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, workerId + "-poll-loop");
            t.setDaemon(true);
            return t;
        });

        pollingExecutor.scheduleWithFixedDelay(
            this::pollAndClaimTasks,
            500L,
            Math.max(100L, properties.getPollIntervalMs()),
            TimeUnit.MILLISECONDS
        );

        // 4. Maintenance loop (heartbeat + lease renewals)
        maintenanceExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, workerId + "-maintenance");
            t.setDaemon(true);
            return t;
        });

        int heartbeatInterval = Math.max(1, properties.getHeartbeatIntervalSeconds());
        maintenanceExecutor.scheduleAtFixedRate(
            this::sendHeartbeat,
            heartbeatInterval,
            heartbeatInterval,
            TimeUnit.SECONDS
        );

        int leaseRenewalInterval = Math.max(1, properties.getLeaseDurationSeconds() / 3);
        maintenanceExecutor.scheduleAtFixedRate(
            this::renewActiveLeases,
            leaseRenewalInterval,
            leaseRenewalInterval,
            TimeUnit.SECONDS
        );

        log.info("[WorkerManager] Worker [{}] successfully initialized and running.", workerId);
    }

    private void registerWorker(String workerId, String hostname, int capacity) {
        WorkerNode node = workerRepository.findById(workerId).orElseGet(() -> new WorkerNode(workerId, hostname, capacity));
        node.setHostname(hostname);
        node.setCapacity(capacity);
        node.setStatus(WorkerStatus.HEALTHY);
        node.setActiveJobs(0);
        node.setLastHeartbeatAt(Instant.now());
        workerRepository.save(node);
        log.info("[WorkerManager] Worker [{}] registered in database.", workerId);
    }

    private void sendHeartbeat() {
        if (!running.get()) return;
        try {
            workerRepository.updateHeartbeat(properties.getId(), activeJobs.get(), Instant.now());
        } catch (Exception e) {
            log.warn("[WorkerManager] Failed to publish heartbeat for worker [{}]: {}", properties.getId(), e.getMessage());
        }
    }

    private void renewActiveLeases() {
        if (!running.get() || activeTasks.isEmpty()) return;

        String workerId = properties.getId();
        int leaseSec = properties.getLeaseDurationSeconds();

        for (ActiveTaskInfo info : activeTasks.values()) {
            try {
                boolean renewed = taskDao.renewLease(info.taskId, workerId, info.fencingToken, leaseSec);
                if (!renewed) {
                    info.leaseValid.set(false);
                    log.warn("[WorkerManager] Failed to renew lease for task [{}] (token={}). Worker ownership revoked!",
                        info.taskId, info.fencingToken);
                } else {
                    log.debug("[WorkerManager] Renewed lease for task [{}] by worker [{}]", info.taskId, workerId);
                }
            } catch (Exception e) {
                log.error("[WorkerManager] Exception during lease renewal for task [{}]: {}", info.taskId, e.getMessage());
            }
        }
    }

    private void pollAndClaimTasks() {
        if (!running.get()) return;

        // Ensure worker has available capacity before claiming from database
        int availableCapacity = properties.getThreads() - activeJobs.get();
        if (availableCapacity <= 0) {
            return;
        }

        try {
            Optional<Task> claimedOpt = taskDao.claimNextTask(properties.getId(), properties.getLeaseDurationSeconds());
            if (claimedOpt.isEmpty()) {
                return;
            }

            Task task = claimedOpt.get();
            activeJobs.incrementAndGet();
            ActiveTaskInfo info = new ActiveTaskInfo(task.getId(), task.getFencingToken());
            activeTasks.put(task.getId(), info);

            log.info("[WorkerManager] Worker [{}] atomically claimed Task [{}] (Type: {}, Priority: {}, Attempt: {}, FencingToken: {})",
                properties.getId(), task.getId(), task.getTaskType(), task.getPriority(), task.getAttemptCount(), task.getFencingToken());

            metrics.incrementSubmitted();

            // Record execution attempt
            TaskAttempt attempt = new TaskAttempt(task.getId(), properties.getId(), task.getAttemptCount(), task.getFencingToken());
            attemptRepository.save(attempt);

            // Execute on worker thread pool
            workerPool.execute(() -> executeClaimedTask(task, info, attempt));

        } catch (RejectedExecutionException re) {
            log.error("[WorkerManager] Local worker pool saturated unexpectedly! Releasing claim.");
        } catch (Exception e) {
            log.error("[WorkerManager] Error polling for tasks: {}", e.getMessage(), e);
        }
    }

    private void executeClaimedTask(Task task, ActiveTaskInfo info, TaskAttempt attempt) {
        long startNanos = System.nanoTime();
        String workerId = properties.getId();
        UUID taskId = task.getId();
        long fencingToken = task.getFencingToken();

        TaskExecutionContext context = new TaskExecutionContext(
            taskId,
            workerId,
            task.getAttemptCount(),
            fencingToken,
            task.getPriority(),
            task.getIdempotencyKey(),
            () -> info.leaseValid.get() && running.get()
        );

        try {
            TaskHandler handler = handlerRegistry.getHandler(task.getTaskType())
                .orElseThrow(() -> new NonRetryableTaskException("No registered TaskHandler for task_type: " + task.getTaskType()));

            JsonNode result = handler.execute(task.getPayload(), context);
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            // Validate lease was not revoked during execution
            if (!info.leaseValid.get()) {
                throw new RetryableTaskException("Task lease expired or revoked during handler execution");
            }

            String resultStr = result != null ? result.toString() : "{\"status\":\"COMPLETED\"}";
            boolean updated = taskDao.completeTask(taskId, workerId, fencingToken, resultStr, durationMs);

            if (updated) {
                attempt.setFinishedAt(Instant.now());
                attempt.setOutcome("COMPLETED");
                attempt.setExecutionDurationMs(durationMs);
                attemptRepository.save(attempt);

                metrics.incrementCompleted(durationMs);
                log.info("[WorkerManager] Worker [{}] COMPLETED task [{}] in {}ms (fencing token: {})",
                    workerId, taskId, durationMs, fencingToken);
            } else {
                attempt.setFinishedAt(Instant.now());
                attempt.setOutcome("FENCED_OUT");
                attempt.setErrorMessage("Task completion rejected by fencing token validation or lost lease");
                attempt.setExecutionDurationMs(durationMs);
                attemptRepository.save(attempt);

                log.warn("[WorkerManager] Worker [{}] was FENCED OUT for task [{}] (token={})! Completion not committed.",
                    workerId, taskId, fencingToken);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handleTaskFailure(task, info, attempt, "Task thread interrupted during execution", startNanos, true);
        } catch (Exception e) {
            boolean isRetryable = retryPolicy.isRetryable(e);
            handleTaskFailure(task, info, attempt, e.getMessage(), startNanos, isRetryable);
        } finally {
            activeTasks.remove(taskId);
            activeJobs.decrementAndGet();
        }
    }

    private void handleTaskFailure(
        Task task,
        ActiveTaskInfo info,
        TaskAttempt attempt,
        String errorMsg,
        long startNanos,
        boolean isRetryable
    ) {
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        String workerId = properties.getId();
        UUID taskId = task.getId();
        long fencingToken = task.getFencingToken();

        attempt.setFinishedAt(Instant.now());
        attempt.setErrorMessage(errorMsg != null ? errorMsg : "Task execution error");
        attempt.setExecutionDurationMs(durationMs);

        // Check if retry is eligible
        if (isRetryable && task.getAttemptCount() < task.getMaxAttempts()) {
            int delaySeconds = retryPolicy.calculateDelaySeconds(task.getAttemptCount());
            log.warn("[WorkerManager] Task [{}] failed (attempt {}/{}). Scheduling retry in {}s. Error: {}",
                taskId, task.getAttemptCount(), task.getMaxAttempts(), delaySeconds, errorMsg);

            boolean retryScheduled = taskDao.failAndScheduleRetry(taskId, workerId, fencingToken, errorMsg, delaySeconds);
            if (retryScheduled) {
                attempt.setOutcome("RETRY_SCHEDULED");
                metrics.incrementRetried();
            } else {
                attempt.setOutcome("FENCED_OUT");
                log.warn("[WorkerManager] Could not schedule retry for task [{}] (fenced out)", taskId);
            }
        } else {
            // Maximum attempts reached or non-retryable failure: Dead Letter Queue
            log.error("[WorkerManager] Task [{}] permanently failed (attempts={}/{}, retryable={}). Moving to DLQ. Error: {}",
                taskId, task.getAttemptCount(), task.getMaxAttempts(), isRetryable, errorMsg);

            boolean dlqMarked = taskDao.markDeadLettered(taskId, workerId, fencingToken, errorMsg);
            if (dlqMarked) {
                attempt.setOutcome("DEAD_LETTERED");
                metrics.incrementDeadLettered();
                metrics.incrementFailed();

                // Persist in dead_letter_tasks table
                DeadLetterTask dlq = new DeadLetterTask(
                    taskId,
                    task.getTaskType(),
                    task.getTaskName(),
                    task.getPayload(),
                    (short) task.getPriority().getLevel(),
                    task.getAttemptCount(),
                    errorMsg
                );
                try {
                    deadLetterRepository.save(dlq);
                } catch (Exception ex) {
                    log.error("[WorkerManager] Failed to persist DLQ record for task [{}]: {}", taskId, ex.getMessage());
                }
            } else {
                attempt.setOutcome("FENCED_OUT");
            }
        }

        attemptRepository.save(attempt);
    }

    public int getActiveJobsCount() {
        return activeJobs.get();
    }

    @PreDestroy
    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        String workerId = properties.getId();
        log.info("[WorkerManager] Initiating graceful shutdown for worker [{}]...", workerId);

        // Update status to DRAINING in database
        try {
            workerRepository.findById(workerId).ifPresent(w -> {
                w.setStatus(WorkerStatus.DRAINING);
                workerRepository.save(w);
            });
        } catch (Exception ignored) {}

        // Stop polling
        if (pollingExecutor != null) {
            pollingExecutor.shutdown();
        }

        // Wait for active tasks to complete gracefully
        if (workerPool != null) {
            workerPool.shutdown();
            try {
                if (!workerPool.awaitTermination(Math.min(5, properties.getLeaseDurationSeconds()), TimeUnit.SECONDS)) {
                    log.warn("[WorkerManager] Pool did not finish within timeout. Forcing shutdown.");
                    workerPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                workerPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (maintenanceExecutor != null) {
            maintenanceExecutor.shutdownNow();
        }

        // Mark OFFLINE in database
        try {
            workerRepository.findById(workerId).ifPresent(w -> {
                w.setStatus(WorkerStatus.OFFLINE);
                w.setActiveJobs(0);
                workerRepository.save(w);
            });
        } catch (Exception ignored) {}

        log.info("[WorkerManager] Graceful shutdown complete for worker [{}].", workerId);
    }
}
