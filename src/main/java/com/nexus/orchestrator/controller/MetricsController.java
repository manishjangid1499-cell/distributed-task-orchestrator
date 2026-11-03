package com.nexus.orchestrator.controller;

import com.nexus.orchestrator.core.metrics.SystemMetricsCollector;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.model.WorkerStatus;
import com.nexus.orchestrator.model.dto.MetricsSummaryResponse;
import com.nexus.orchestrator.repository.TaskRepository;
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
    private final TaskRepository taskRepository;
    private final WorkerRepository workerRepository;

    public MetricsController(
        SystemMetricsCollector metrics,
        TaskRepository taskRepository,
        WorkerRepository workerRepository
    ) {
        this.metrics = metrics;
        this.taskRepository = taskRepository;
        this.workerRepository = workerRepository;
    }

    @GetMapping("/summary")
    public ResponseEntity<MetricsSummaryResponse> getSummary() {
        int healthyWorkers = (int) workerRepository.countByStatus(WorkerStatus.HEALTHY);
        int totalWorkers = (int) workerRepository.count();

        long pendingCount = taskRepository.countByStatus(TaskStatus.PENDING) + taskRepository.countByStatus(TaskStatus.RETRY_WAIT);
        long runningCount = taskRepository.countByStatus(TaskStatus.RUNNING);
        long completedCount = taskRepository.countByStatus(TaskStatus.COMPLETED);
        long dlqCount = taskRepository.countByStatus(TaskStatus.DEAD_LETTERED);

        long submitted = Math.max(metrics.getTotalSubmitted(), completedCount + runningCount + pendingCount + dlqCount);

        MetricsSummaryResponse res = new MetricsSummaryResponse(
            submitted,
            completedCount,
            metrics.getTotalFailed() > 0 ? metrics.getTotalFailed() : dlqCount,
            metrics.getTotalRetried(),
            dlqCount,
            (int) pendingCount,
            totalWorkers,
            healthyWorkers,
            Math.round(metrics.getAverageLatencyMs() * 100.0) / 100.0,
            metrics.getRateLimitViolations(),
            pendingCount,
            runningCount
        );
        return ResponseEntity.ok(res);
    }
}
