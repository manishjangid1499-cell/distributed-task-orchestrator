package com.nexus.orchestrator.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.nexus.orchestrator.core.metrics.SystemMetricsCollector;
import com.nexus.orchestrator.core.ratelimit.RateLimiter;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.model.dto.TaskResponse;
import com.nexus.orchestrator.model.dto.TaskSubmissionRequest;
import com.nexus.orchestrator.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final TaskRepository taskRepository;
    private final RateLimiter rateLimiter;
    private final SystemMetricsCollector metrics;
    private final int defaultMaxRetries;

    public enum CancelResult {
        CANCELLED,
        CONFLICT_RUNNING,
        NOT_FOUND,
        ALREADY_TERMINAL
    }

    public TaskService(
        TaskRepository taskRepository,
        RateLimiter rateLimiter,
        SystemMetricsCollector metrics,
        @Value("${nexus.scheduler.default-max-retries:3}") int defaultMaxRetries
    ) {
        this.taskRepository = taskRepository;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
        this.defaultMaxRetries = defaultMaxRetries;
    }

    @Transactional
    public Task submitTask(TaskSubmissionRequest request) {
        String clientId = request.getSubmittedBy();

        // 1. Sliding Window Rate Limiting (per API process)
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

        // 2. Parse payload into JsonNode
        JsonNode payloadNode = parsePayload(request.getPayload(), request.getSimulatedExecutionDurationMs(), request.isShouldSimulateFailure());
        String payloadHash = calculateSha256(payloadNode.toString());

        // 3. Idempotency Key Validation
        String idempotencyKey = request.getIdempotencyKey();
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            Optional<Task> existing = taskRepository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                Task existingTask = existing.get();
                if (existingTask.getPayloadHash().equals(payloadHash)) {
                    log.info("[TaskService] Idempotency match for key [{}]. Returning existing task [{}].", idempotencyKey, existingTask.getId());
                    return existingTask;
                } else {
                    log.error("[TaskService] Idempotency conflict for key [{}]: existing payload hash [{}] != new payload hash [{}]",
                        idempotencyKey, existingTask.getPayloadHash(), payloadHash);
                    throw new IdempotencyConflictException("Task submission conflict: Idempotency key '" + idempotencyKey + "' already exists with a different payload.");
                }
            }
        }

        // 4. Create and persist Task in PostgreSQL (authoritative source of pending work)
        Task task = new Task(
            request.getTaskType(),
            request.getTaskName(),
            payloadNode,
            request.getPriority(),
            (idempotencyKey != null && !idempotencyKey.isBlank()) ? idempotencyKey : null,
            payloadHash,
            request.getMaxAttempts() > 0 ? request.getMaxAttempts() : defaultMaxRetries,
            clientId
        );

        try {
            taskRepository.saveAndFlush(task);
            log.info("[TaskService] Task [{}] ({}) persisted with priority {} and status PENDING",
                task.getId(), task.getTaskType(), task.getPriority());
            return task;
        } catch (DataIntegrityViolationException dive) {
            // Check if concurrent submission collided on idempotency key
            if (idempotencyKey != null) {
                Task existing = taskRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> dive);
                if (existing.getPayloadHash().equals(payloadHash)) {
                    return existing;
                }
                throw new IdempotencyConflictException("Concurrent submission conflict on idempotency key '" + idempotencyKey + "'");
            }
            throw dive;
        }
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

    public Page<TaskResponse> listTasks(TaskStatus status, TaskPriority priority, int page, int size) {
        Pageable pageable = PageRequest.of(Math.max(0, page), Math.max(1, Math.min(size, 100)), Sort.by("createdAt").descending());
        Page<Task> taskPage;
        if (status != null && priority != null) {
            taskPage = taskRepository.findByStatusAndPriority(status, priority, pageable);
        } else if (status != null) {
            taskPage = taskRepository.findByStatus(status, pageable);
        } else if (priority != null) {
            taskPage = taskRepository.findByPriority(priority, pageable);
        } else {
            taskPage = taskRepository.findAll(pageable);
        }
        return taskPage.map(TaskResponse::fromEntity);
    }

    @Transactional
    public CancelResult cancelTask(String taskId) {
        Optional<Task> opt = taskRepository.findByTaskId(taskId);
        if (opt.isEmpty()) {
            return CancelResult.NOT_FOUND;
        }

        Task task = opt.get();
        if (task.getStatus() == TaskStatus.RUNNING) {
            log.warn("[TaskService] Cannot cancel running task [{}] without cooperative interruption.", taskId);
            return CancelResult.CONFLICT_RUNNING;
        }

        if (task.getStatus() == TaskStatus.PENDING || task.getStatus() == TaskStatus.RETRY_WAIT || task.getStatus() == TaskStatus.QUEUED || task.getStatus() == TaskStatus.SUBMITTED) {
            task.setStatus(TaskStatus.CANCELLED);
            taskRepository.save(task);
            log.info("[TaskService] Task [{}] marked CANCELLED.", taskId);
            return CancelResult.CANCELLED;
        }

        return CancelResult.ALREADY_TERMINAL;
    }

    @Transactional
    public Optional<Task> retryFailedTask(String taskId) {
        Optional<Task> opt = taskRepository.findByTaskId(taskId);
        if (opt.isPresent()) {
            Task task = opt.get();
            if (task.getStatus() == TaskStatus.DEAD_LETTERED || task.getStatus() == TaskStatus.CANCELLED || task.getStatus() == TaskStatus.FAILED) {
                task.setAttemptCount(0);
                task.setStatus(TaskStatus.PENDING);
                task.setAvailableAt(Instant.now());
                task.setLastError(null);
                task.setLockedBy(null);
                task.setLeaseUntil(null);
                taskRepository.save(task);
                log.info("[TaskService] Manually reset task [{}] to PENDING.", taskId);
                return Optional.of(task);
            }
        }
        return Optional.empty();
    }

    private JsonNode parsePayload(String rawPayload, long simulatedDuration, boolean simulateFailure) {
        ObjectNode node;
        if (rawPayload != null && !rawPayload.isBlank()) {
            try {
                JsonNode parsed = OBJECT_MAPPER.readTree(rawPayload);
                if (parsed.isObject()) {
                    node = (ObjectNode) parsed;
                } else {
                    node = OBJECT_MAPPER.createObjectNode();
                    node.set("raw", parsed);
                }
            } catch (Exception e) {
                node = OBJECT_MAPPER.createObjectNode();
                node.put("raw", rawPayload);
            }
        } else {
            node = OBJECT_MAPPER.createObjectNode();
        }

        if (simulatedDuration > 0 && !node.has("durationMs")) {
            node.put("durationMs", simulatedDuration);
        }
        if (simulateFailure) {
            node.put("failAttempts", 999);
            node.put("error", "Simulated failure requested in submission");
        }
        return node;
    }

    private String calculateSha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
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
