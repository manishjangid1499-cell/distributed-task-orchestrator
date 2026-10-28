package com.nexus.orchestrator.controller;

import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.service.WorkerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/workers")
@CrossOrigin(origins = "*")
public class WorkerController {

    private final WorkerService workerService;

    public WorkerController(WorkerService workerService) {
        this.workerService = workerService;
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
    public ResponseEntity<?> simulateCrash(@PathVariable String workerId) {
        boolean ok = workerService.simulateWorkerCrash(workerId);
        if (ok) {
            return ResponseEntity.ok(Map.of("status", "injected", "message", "Worker marked CRASHED. Supervisor will reclaim tasks."));
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping("/{workerId}/recover")
    public ResponseEntity<?> recoverWorker(@PathVariable String workerId) {
        boolean ok = workerService.recoverWorker(workerId);
        if (ok) {
            return ResponseEntity.ok(Map.of("status", "recovered", "message", "Worker restored to HEALTHY state."));
        }
        return ResponseEntity.notFound().build();
    }
}
