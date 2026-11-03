package com.nexus.orchestrator.core.supervisor;

import com.nexus.orchestrator.core.WorkerProperties;
import com.nexus.orchestrator.core.engine.TaskExecutionEngine;
import com.nexus.orchestrator.model.DeadLetterTask;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.model.WorkerStatus;
import com.nexus.orchestrator.repository.DeadLetterRepository;
import com.nexus.orchestrator.repository.DistributedTaskDao;
import com.nexus.orchestrator.repository.TaskRepository;
import com.nexus.orchestrator.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Worker Supervisor.
 * Periodically detects unresponsive workers and sweeps expired tasks whose attempts have exhausted into DLQ.
 * In distributed mode, task re-claiming is strictly coordinated through PostgreSQL SELECT ... FOR UPDATE SKIP LOCKED.
 */
@Component
public class WorkerHeartbeatSupervisor {

    private static final Logger log = LoggerFactory.getLogger(WorkerHeartbeatSupervisor.class);

    private final WorkerRepository workerRepository;
    private TaskRepository taskRepository;
    private TaskExecutionEngine taskExecutionEngine;
    private DistributedTaskDao taskDao;
    private DeadLetterRepository deadLetterRepository;
    private WorkerProperties properties;
    private long heartbeatTimeoutSeconds = 15;

    @Autowired
    public WorkerHeartbeatSupervisor(
        WorkerRepository workerRepository,
        TaskRepository taskRepository,
        DistributedTaskDao taskDao,
        DeadLetterRepository deadLetterRepository,
        WorkerProperties properties
    ) {
        this.workerRepository = workerRepository;
        this.taskRepository = taskRepository;
        this.taskDao = taskDao;
        this.deadLetterRepository = deadLetterRepository;
        this.properties = properties;
        this.heartbeatTimeoutSeconds = properties.getHeartbeatTimeoutSeconds();
    }

    public WorkerHeartbeatSupervisor(
        WorkerRepository workerRepository,
        TaskRepository taskRepository,
        TaskExecutionEngine taskExecutionEngine,
        long heartbeatTimeoutSeconds
    ) {
        this.workerRepository = workerRepository;
        this.taskRepository = taskRepository;
        this.taskExecutionEngine = taskExecutionEngine;
        this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds;
    }

    @Scheduled(fixedRateString = "${nexus.scheduler.supervisor-interval-ms:5000}")
    public void monitorClusterHealth() {
        if (properties != null && !properties.isApiMode()) {
            return;
        }

        Instant threshold = Instant.now().minusSeconds(heartbeatTimeoutSeconds);

        List<WorkerNode> allWorkers = workerRepository.findAll();
        for (WorkerNode worker : allWorkers) {
            if (worker.getStatus() == WorkerStatus.OFFLINE || worker.getStatus() == WorkerStatus.DRAINING) {
                continue;
            }

            if (worker.getLastHeartbeatAt().isBefore(threshold)) {
                if (worker.getStatus() != WorkerStatus.CRASHED && worker.getStatus() != WorkerStatus.UNRESPONSIVE) {
                    log.error("[Supervisor] Heartbeat missed! Worker [{}] on host [{}] declared CRASHED.",
                        worker.getWorkerId(), worker.getHostname());
                    worker.setStatus(WorkerStatus.CRASHED);
                    workerRepository.save(worker);

                    if (taskExecutionEngine != null && taskRepository != null) {
                        reclaimOrphanTasks(worker.getWorkerId());
                    }
                }
            } else if (worker.getStatus() == WorkerStatus.CRASHED || worker.getStatus() == WorkerStatus.UNRESPONSIVE) {
                log.info("[Supervisor] Worker [{}] recovered and sent heartbeat.", worker.getWorkerId());
                worker.setStatus(WorkerStatus.HEALTHY);
                workerRepository.save(worker);
            }
        }

        // Distributed sweep for exhausted expired tasks into DLQ
        if (taskDao != null && deadLetterRepository != null) {
            try {
                List<Task> exhaustedTasks = taskDao.sweepExpiredExhaustedTasks(10);
                for (Task task : exhaustedTasks) {
                    log.warn("[Supervisor] Task [{}] timed out with attempt_count >= max_attempts. Quarantined to DLQ.", task.getId());
                    DeadLetterTask dlq = new DeadLetterTask(
                        task.getId(),
                        task.getTaskType(),
                        task.getTaskName(),
                        task.getPayload(),
                        (short) task.getPriority().getLevel(),
                        task.getAttemptCount(),
                        task.getLastError()
                    );
                    deadLetterRepository.save(dlq);
                }
            } catch (Exception e) {
                log.error("[Supervisor] Error sweeping expired exhausted tasks: {}", e.getMessage());
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
