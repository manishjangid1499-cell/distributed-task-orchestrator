package com.nexus.orchestrator.service;

import com.nexus.orchestrator.core.engine.TaskExecutionEngine;
import com.nexus.orchestrator.model.DeadLetterRecord;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.repository.DeadLetterRepository;
import com.nexus.orchestrator.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final DeadLetterRepository deadLetterRepository;
    private final TaskRepository taskRepository;
    private final TaskExecutionEngine executionEngine;

    public DeadLetterService(
        DeadLetterRepository deadLetterRepository,
        TaskRepository taskRepository,
        TaskExecutionEngine executionEngine
    ) {
        this.deadLetterRepository = deadLetterRepository;
        this.taskRepository = taskRepository;
        this.executionEngine = executionEngine;
    }

    public List<DeadLetterRecord> listQuarantinedTasks() {
        return deadLetterRepository.findByReplayedFalseOrderByQuarantinedAtDesc();
    }

    public boolean replayTask(String taskId) {
        Optional<DeadLetterRecord> dlqOpt = deadLetterRepository.findByTaskId(taskId);
        if (dlqOpt.isEmpty()) {
            return false;
        }

        DeadLetterRecord dlq = dlqOpt.get();
        dlq.setReplayed(true);
        deadLetterRepository.save(dlq);

        // Re-create a clean task and submit
        Task task = new Task(
            dlq.getTaskName(),
            dlq.getOriginalPayload(),
            TaskPriority.HIGH,
            "dlq-replay-operator",
            200
        );

        executionEngine.submit(task, "worker-us-east-1a");
        log.info("[DLQ] Task [{}] replayed from quarantine into the scheduler.", taskId);
        return true;
    }

    public void purgeAll() {
        deadLetterRepository.deleteAll();
        log.info("[DLQ] Purged all dead letter records.");
    }
}
