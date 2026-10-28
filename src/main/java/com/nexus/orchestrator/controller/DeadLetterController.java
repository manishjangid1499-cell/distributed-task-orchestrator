package com.nexus.orchestrator.controller;

import com.nexus.orchestrator.model.DeadLetterRecord;
import com.nexus.orchestrator.service.DeadLetterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/dlq")
@CrossOrigin(origins = "*")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    @GetMapping
    public ResponseEntity<List<DeadLetterRecord>> listDeadLetters() {
        return ResponseEntity.ok(deadLetterService.listQuarantinedTasks());
    }

    @PostMapping("/{taskId}/replay")
    public ResponseEntity<?> replayTask(@PathVariable String taskId) {
        boolean replayed = deadLetterService.replayTask(taskId);
        if (replayed) {
            return ResponseEntity.ok(Map.of("status", "success", "message", "Task replayed from DLQ"));
        }
        return ResponseEntity.notFound().build();
    }

    @DeleteMapping("/purge")
    public ResponseEntity<?> purgeDlq() {
        deadLetterService.purgeAll();
        return ResponseEntity.ok(Map.of("status", "purged"));
    }
}
