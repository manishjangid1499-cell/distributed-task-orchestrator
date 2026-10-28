package com.nexus.orchestrator.controller;

import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.dto.TaskResponse;
import com.nexus.orchestrator.model.dto.TaskSubmissionRequest;
import com.nexus.orchestrator.service.TaskService;
import jakarta.validation.Valid;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/tasks")
@CrossOrigin(origins = "*")
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping
    public ResponseEntity<?> submitTask(@Valid @RequestBody TaskSubmissionRequest request) {
        try {
            Task task = taskService.submitTask(request);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(TaskResponse.fromEntity(task));
        } catch (TaskService.RateLimitExceededException e) {
            HttpHeaders headers = new HttpHeaders();
            headers.add("Retry-After", String.valueOf(e.getRetryAfterMs() / 1000));
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
            .map(t -> ResponseEntity.ok(TaskResponse.fromEntity(t)))
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<TaskResponse>> listTasks() {
        return ResponseEntity.ok(taskService.listRecentTasks());
    }

    @PostMapping("/{taskId}/cancel")
    public ResponseEntity<?> cancelTask(@PathVariable String taskId) {
        boolean cancelled = taskService.cancelTask(taskId);
        if (cancelled) {
            return ResponseEntity.ok(Map.of("status", "success", "message", "Task cancelled"));
        }
        return ResponseEntity.badRequest().body(Map.of("status", "failed", "message", "Task could not be cancelled or does not exist"));
    }

    @PostMapping("/{taskId}/retry")
    public ResponseEntity<?> retryTask(@PathVariable String taskId) {
        return taskService.retryFailedTask(taskId)
            .map(t -> ResponseEntity.ok(TaskResponse.fromEntity(t)))
            .orElse(ResponseEntity.badRequest().build());
    }
}
