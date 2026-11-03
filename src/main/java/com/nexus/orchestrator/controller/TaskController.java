package com.nexus.orchestrator.controller;

import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskAttempt;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.model.dto.TaskDetailResponse;
import com.nexus.orchestrator.model.dto.TaskResponse;
import com.nexus.orchestrator.model.dto.TaskSubmissionRequest;
import com.nexus.orchestrator.repository.TaskAttemptRepository;
import com.nexus.orchestrator.service.IdempotencyConflictException;
import com.nexus.orchestrator.service.TaskService;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/tasks")
@CrossOrigin(origins = "*")
public class TaskController {

    private final TaskService taskService;
    private final TaskAttemptRepository attemptRepository;

    public TaskController(TaskService taskService, TaskAttemptRepository attemptRepository) {
        this.taskService = taskService;
        this.attemptRepository = attemptRepository;
    }

    @PostMapping
    public ResponseEntity<?> submitTask(@Valid @RequestBody TaskSubmissionRequest request) {
        try {
            Task task = taskService.submitTask(request);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(TaskResponse.fromEntity(task));
        } catch (IdempotencyConflictException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "error", "Conflict",
                "message", e.getMessage()
            ));
        } catch (TaskService.RateLimitExceededException e) {
            HttpHeaders headers = new HttpHeaders();
            headers.add("Retry-After", String.valueOf(Math.max(1, e.getRetryAfterMs() / 1000)));
            headers.add("X-RateLimit-Error", "QuotaExceeded");
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .headers(headers)
                .body(Map.of(
                    "error", "Too Many Requests",
                    "message", e.getMessage(),
                    "retryAfterMs", e.getRetryAfterMs()
                ));
        }
    }

    @GetMapping("/{taskId}")
    public ResponseEntity<?> getTask(@PathVariable String taskId) {
        return taskService.getTaskById(taskId)
            .map(t -> {
                List<TaskAttempt> attempts = attemptRepository.findByTaskIdOrderByAttemptNumberAsc(t.getId());
                return ResponseEntity.ok(TaskDetailResponse.of(t, attempts));
            })
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<?> listTasks(
        @RequestParam(required = false) TaskStatus status,
        @RequestParam(required = false) TaskPriority priority,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "50") int size
    ) {
        Page<TaskResponse> taskPage = taskService.listTasks(status, priority, page, size);
        return ResponseEntity.ok(taskPage.getContent());
    }

    @PostMapping("/{taskId}/cancel")
    public ResponseEntity<?> cancelTask(@PathVariable String taskId) {
        TaskService.CancelResult res = taskService.cancelTask(taskId);
        return switch (res) {
            case CANCELLED -> ResponseEntity.ok(Map.of("status", "success", "message", "Task successfully cancelled"));
            case CONFLICT_RUNNING -> ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "status", "conflict",
                "message", "Cannot cancel task: task is currently RUNNING and cannot be preempted"
            ));
            case NOT_FOUND -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                "status", "not_found",
                "message", "Task does not exist"
            ));
            case ALREADY_TERMINAL -> ResponseEntity.badRequest().body(Map.of(
                "status", "failed",
                "message", "Task is already in a terminal state"
            ));
        };
    }

    @PostMapping("/{taskId}/retry")
    public ResponseEntity<?> retryTask(@PathVariable String taskId) {
        return taskService.retryFailedTask(taskId)
            .map(t -> ResponseEntity.ok(TaskResponse.fromEntity(t)))
            .orElse(ResponseEntity.badRequest().body(TaskResponse.fromEntity(null)));
    }
}
