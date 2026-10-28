package com.nexus.orchestrator.service;

import com.nexus.orchestrator.core.engine.TaskExecutionEngine;
import com.nexus.orchestrator.core.metrics.SystemMetricsCollector;
import com.nexus.orchestrator.core.ratelimit.RateLimiter;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.model.WorkerStatus;
import com.nexus.orchestrator.model.dto.TaskResponse;
import com.nexus.orchestrator.model.dto.TaskSubmissionRequest;
import com.nexus.orchestrator.repository.TaskRepository;
import com.nexus.orchestrator.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);

    private final TaskRepository taskRepository;
    private final WorkerRepository workerRepository;
    private final TaskExecutionEngine executionEngine;
    private final RateLimiter rateLimiter;
    private final SystemMetricsCollector metrics;

    public TaskService(
        TaskRepository taskRepository,
        WorkerRepository workerRepository,
        TaskExecutionEngine executionEngine,
        RateLimiter rateLimiter,
        SystemMetricsCollector metrics
    ) {
        this.taskRepository = taskRepository;
        this.workerRepository = workerRepository;
        this.executionEngine = executionEngine;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
    }

    public Task submitTask(TaskSubmissionRequest request) {
        String clientId = request.getSubmittedBy();

        // 1. Enforce Sliding Window Rate Limiting
        RateLimiter.Result rateCheck = rateLimiter.tryAcquire(clientId, 1);
        if (!rateCheck.allowed()) {
            metrics.incrementRateLimitViolations();
            log.warn("[TaskService] Rate limit exceeded for client [{}]. Retry after {}ms", clientId, rateCheck.retryAfterMs());
            throw new RateLimitExceededException(
                "Rate limit quota exceeded for client [" + clientId + "]. Limit: " + rateCheck.limit() +
                " requests / " + rateCheck.windowSeconds() + "s. Retry after " + rateCheck.retryAfterMs() + "ms",
                rateCheck.retryAfterMs()
            );
        }

        // 2. Load Balancing: Select worker with least active task load
        String assignedWorkerId = selectOptimalWorker();

        String payload = request.getPayload();
        if (request.isShouldSimulateFailure()) {
            payload = (payload == null ? "" : payload) + " [SIMULATE_FAILURE]";
        }

        // 3. Create Task entity
        Task task = new Task(
            request.getTaskName(),
            payload,
            request.getPriority(),
            clientId,
            request.getSimulatedExecutionDurationMs()
        );

        // 4. Submit to execution engine
        executionEngine.submit(task, assignedWorkerId);
        return task;
    }

    public Optional<Task> getTaskById(String taskId) {
        return taskRepository.findByTaskId(taskId);
    }

    public List<TaskResponse> listRecentTasks() {
        return taskRepository.findRecentTasks().stream()
            .limit(100)
            .map(TaskResponse::fromEntity)
            .toList();
    }

    public boolean cancelTask(String taskId) {
        Optional<Task> opt = taskRepository.findByTaskId(taskId);
        if (opt.isPresent()) {
            Task task = opt.get();
            if (task.getStatus() == TaskStatus.QUEUED || task.getStatus() == TaskStatus.SUBMITTED) {
                task.setStatus(TaskStatus.CANCELLED);
                taskRepository.save(task);
                return true;
            }
        }
        return false;
    }

    public Optional<Task> retryFailedTask(String taskId) {
        Optional<Task> opt = taskRepository.findByTaskId(taskId);
        if (opt.isPresent()) {
            Task task = opt.get();
            if (task.getStatus() == TaskStatus.FAILED || task.getStatus() == TaskStatus.DEAD_LETTERED) {
                task.setRetryCount(0);
                task.setStatus(TaskStatus.SUBMITTED);
                String worker = selectOptimalWorker();
                executionEngine.submit(task, worker);
                return Optional.of(task);
            }
        }
        return Optional.empty();
    }

    private String selectOptimalWorker() {
        return workerRepository.findAll().stream()
            .filter(w -> w.getStatus() == WorkerStatus.HEALTHY)
            .min(Comparator.comparingInt(WorkerNode::getActiveTaskCount))
            .map(WorkerNode::getWorkerId)
            .orElse("default-worker-01");
    }

    public static class RateLimitExceededException extends RuntimeException {
        private final long retryAfterMs;

        public RateLimitExceededException(String message, long retryAfterMs) {
            super(message);
            this.retryAfterMs = retryAfterMs;
        }

        public long getRetryAfterMs() {
            return retryAfterMs;
        }
    }
}
