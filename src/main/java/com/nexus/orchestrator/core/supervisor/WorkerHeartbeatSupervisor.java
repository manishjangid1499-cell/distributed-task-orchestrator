package com.nexus.orchestrator.core.supervisor;

import com.nexus.orchestrator.core.engine.TaskExecutionEngine;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.model.WorkerStatus;
import com.nexus.orchestrator.repository.TaskRepository;
import com.nexus.orchestrator.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Distributed Heartbeat Supervisor.
 * Periodically monitors worker node health, detects worker crashes,
 * and recovers orphan tasks to guarantee At-Least-Once execution.
 */
@Component
public class WorkerHeartbeatSupervisor {

    private static final Logger log = LoggerFactory.getLogger(WorkerHeartbeatSupervisor.class);

    private final WorkerRepository workerRepository;
    private final TaskRepository taskRepository;
    private final TaskExecutionEngine taskExecutionEngine;
    private final long heartbeatTimeoutSeconds;

    public WorkerHeartbeatSupervisor(
        WorkerRepository workerRepository,
        TaskRepository taskRepository,
        TaskExecutionEngine taskExecutionEngine,
        @Value("${nexus.scheduler.heartbeat-timeout-seconds:15}") long heartbeatTimeoutSeconds
    ) {
        this.workerRepository = workerRepository;
        this.taskRepository = taskRepository;
        this.taskExecutionEngine = taskExecutionEngine;
        this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds;
    }

    @Scheduled(fixedRateString = "${nexus.scheduler.supervisor-interval-ms:5000}")
    public void monitorClusterHealth() {
        Instant threshold = Instant.now().minusSeconds(heartbeatTimeoutSeconds);

        List<WorkerNode> allWorkers = workerRepository.findAll();
        for (WorkerNode worker : allWorkers) {
            if (worker.getStatus() == WorkerStatus.OFFLINE) {
                continue;
            }

            if (worker.getLastHeartbeat().isBefore(threshold)) {
                if (worker.getStatus() != WorkerStatus.CRASHED) {
                    log.error("[Supervisor] Heartbeat missed! Worker [{}] on host [{}] declared CRASHED.",
                        worker.getWorkerId(), worker.getHostname());
                    worker.setStatus(WorkerStatus.CRASHED);
                    workerRepository.save(worker);

                    // Reclaim orphan tasks assigned to this crashed worker
                    reclaimOrphanTasks(worker.getWorkerId());
                }
            } else if (worker.getStatus() == WorkerStatus.CRASHED || worker.getStatus() == WorkerStatus.UNRESPONSIVE) {
                // Recovered worker
                log.info("[Supervisor] Worker [{}] recovered and sent heartbeat.", worker.getWorkerId());
                worker.setStatus(WorkerStatus.HEALTHY);
                workerRepository.save(worker);
            }
        }
    }

    private void reclaimOrphanTasks(String crashedWorkerId) {
        List<Task> orphanTasks = taskRepository.findByAssignedWorkerIdAndStatus(crashedWorkerId, TaskStatus.RUNNING);
        orphanTasks.addAll(taskRepository.findByAssignedWorkerIdAndStatus(crashedWorkerId, TaskStatus.QUEUED));

        if (orphanTasks.isEmpty()) {
            return;
        }

        log.warn("[Supervisor] Recovering {} orphan tasks from crashed worker [{}]", orphanTasks.size(), crashedWorkerId);

        // Find a healthy replacement worker
        String targetWorkerId = findHealthyWorker();

        for (Task task : orphanTasks) {
            log.info("[Supervisor] Reclaiming Task [{}] (Attempt {}/{}) and re-routing to [{}]",
                task.getTaskId(), task.getRetryCount() + 1, task.getMaxRetries(), targetWorkerId);
            
            task.setRetryCount(task.getRetryCount() + 1);
            task.setErrorMessage("Worker node [" + crashedWorkerId + "] crashed during task execution. Auto-recovered.");
            taskExecutionEngine.submit(task, targetWorkerId);
        }
    }

    private String findHealthyWorker() {
        return workerRepository.findAll().stream()
            .filter(w -> w.getStatus() == WorkerStatus.HEALTHY && !w.isOverCapacity())
            .map(WorkerNode::getWorkerId)
            .findFirst()
            .orElse("supervisor-fallback-worker");
    }
}
