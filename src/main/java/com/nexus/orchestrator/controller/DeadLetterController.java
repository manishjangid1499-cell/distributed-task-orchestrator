package com.nexus.orchestrator.controller;

import com.nexus.orchestrator.model.DeadLetterTask;
import com.nexus.orchestrator.service.AdminAuthService;
import com.nexus.orchestrator.service.DeadLetterService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/dlq")
@CrossOrigin(origins = "*")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;
    private final AdminAuthService adminAuthService;

    public DeadLetterController(DeadLetterService deadLetterService, AdminAuthService adminAuthService) {
        this.deadLetterService = deadLetterService;
        this.adminAuthService = adminAuthService;
    }

    @GetMapping
    public ResponseEntity<List<DeadLetterTask>> listDeadLetters() {
        return ResponseEntity.ok(deadLetterService.listQuarantinedTasks());
    }

    @PostMapping("/{taskId}/replay")
    public ResponseEntity<?> replayTask(
        @PathVariable String taskId,
        @RequestHeader(value = "X-API-KEY", required = false) String apiKey,
        @RequestParam(defaultValue = "admin-operator") String replayedBy
    ) {
        if (!adminAuthService.isAuthorized(apiKey)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized: Invalid or missing API key"));
        }
        boolean replayed = deadLetterService.replayTask(taskId, replayedBy);
        if (replayed) {
            return ResponseEntity.ok(Map.of("status", "success", "message", "Task successfully replayed from DLQ"));
        }
        return ResponseEntity.notFound().build();
    }

    @DeleteMapping("/purge")
    public ResponseEntity<?> purgeDlq(
        @RequestHeader(value = "X-API-KEY", required = false) String apiKey
    ) {
        if (!adminAuthService.isAuthorized(apiKey)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized: Invalid or missing API key"));
        }
        deadLetterService.purgeAll();
        return ResponseEntity.ok(Map.of("status", "purged"));
    }
}
