package com.nexus.orchestrator.controller;

import com.nexus.orchestrator.core.engine.TaskExecutionEngine;
import com.nexus.orchestrator.core.metrics.SystemMetricsCollector;
import com.nexus.orchestrator.model.WorkerStatus;
import com.nexus.orchestrator.model.dto.MetricsSummaryResponse;
import com.nexus.orchestrator.repository.WorkerRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/metrics")
@CrossOrigin(origins = "*")
public class MetricsController {

    private final SystemMetricsCollector metrics;
    private final TaskExecutionEngine executionEngine;
    private final WorkerRepository workerRepository;

    public MetricsController(
        SystemMetricsCollector metrics,
        TaskExecutionEngine executionEngine,
        WorkerRepository workerRepository
    ) {
        this.metrics = metrics;
        this.executionEngine = executionEngine;
        this.workerRepository = workerRepository;
    }

    @GetMapping("/summary")
    public ResponseEntity<MetricsSummaryResponse> getSummary() {
        int healthyWorkers = (int) workerRepository.countByStatus(WorkerStatus.HEALTHY);
        int totalWorkers = (int) workerRepository.count();

        MetricsSummaryResponse res = new MetricsSummaryResponse(
            metrics.getTotalSubmitted(),
            metrics.getTotalCompleted(),
            metrics.getTotalFailed(),
            metrics.getTotalRetried(),
            metrics.getTotalDeadLettered(),
            executionEngine.getQueueSize(),
            totalWorkers,
            healthyWorkers,
            Math.round(metrics.getAverageLatencyMs() * 100.0) / 100.0,
            metrics.getRateLimitViolations()
        );
        return ResponseEntity.ok(res);
    }
}
