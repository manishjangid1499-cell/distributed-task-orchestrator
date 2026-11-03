package com.nexus.orchestrator.controller;

import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.service.AdminAuthService;
import com.nexus.orchestrator.service.WorkerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/workers")
@CrossOrigin(origins = "*")
public class WorkerController {

    private final WorkerService workerService;
    private final AdminAuthService adminAuthService;

    public WorkerController(WorkerService workerService, AdminAuthService adminAuthService) {
        this.workerService = workerService;
        this.adminAuthService = adminAuthService;
    }

    @GetMapping
    public ResponseEntity<List<WorkerNode>> listWorkers() {
        return ResponseEntity.ok(workerService.listAllWorkers());
    }

    @PostMapping("/{workerId}/heartbeat")
    public ResponseEntity<?> recordHeartbeat(@PathVariable String workerId) {
        boolean ok = workerService.recordHeartbeat(workerId);
        if (ok) {
            return ResponseEntity.ok(Map.of("status", "success", "workerId", workerId));
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping("/{workerId}/crash")
    public ResponseEntity<?> simulateCrash(
        @PathVariable String workerId,
        @RequestHeader(value = "X-API-KEY", required = false) String apiKey
    ) {
        if (!adminAuthService.isAuthorized(apiKey)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized: Invalid or missing API key"));
        }
        boolean ok = workerService.simulateWorkerCrash(workerId);
        if (ok) {
            return ResponseEntity.ok(Map.of("status", "injected", "message", "Worker status set to CRASHED"));
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping("/{workerId}/recover")
    public ResponseEntity<?> recoverWorker(
        @PathVariable String workerId,
        @RequestHeader(value = "X-API-KEY", required = false) String apiKey
    ) {
        if (!adminAuthService.isAuthorized(apiKey)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized: Invalid or missing API key"));
        }
        boolean ok = workerService.recoverWorker(workerId);
        if (ok) {
            return ResponseEntity.ok(Map.of("status", "recovered", "message", "Worker restored to HEALTHY state."));
        }
        return ResponseEntity.notFound().build();
    }
}
