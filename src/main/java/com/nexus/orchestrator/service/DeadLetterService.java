package com.nexus.orchestrator.service;

import com.nexus.orchestrator.model.DeadLetterTask;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.repository.DeadLetterRepository;
import com.nexus.orchestrator.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final DeadLetterRepository deadLetterRepository;
    private final TaskRepository taskRepository;

    public DeadLetterService(DeadLetterRepository deadLetterRepository, TaskRepository taskRepository) {
        this.deadLetterRepository = deadLetterRepository;
        this.taskRepository = taskRepository;
    }

    public List<DeadLetterTask> listQuarantinedTasks() {
        return deadLetterRepository.findAllByOrderByCreatedAtDesc();
    }

    @Transactional
    public boolean replayTask(String rawId, String replayedBy) {
        UUID idOrOriginal;
        try {
            idOrOriginal = UUID.fromString(rawId);
        } catch (IllegalArgumentException e) {
            return false;
        }

        // Look up either by DLQ primary id or original task id
        Optional<DeadLetterTask> dlqOpt = deadLetterRepository.findById(idOrOriginal);
        if (dlqOpt.isEmpty()) {
            dlqOpt = deadLetterRepository.findByOriginalTaskId(idOrOriginal);
        }

        if (dlqOpt.isEmpty()) {
            return false;
        }

        DeadLetterTask dlq = dlqOpt.get();
        Instant now = Instant.now();
        dlq.setReplayedAt(now);
        dlq.setReplayedBy(replayedBy != null ? replayedBy : "admin-operator");
        deadLetterRepository.save(dlq);

        // Re-activate task in persistent tasks table preserving original UUID and audit history
        Optional<Task> taskOpt = taskRepository.findById(dlq.getOriginalTaskId());
        if (taskOpt.isPresent()) {
            Task task = taskOpt.get();
            task.setStatus(TaskStatus.PENDING);
            task.setAttemptCount(0);
            task.setAvailableAt(now);
            task.setLastError(null);
            task.setLockedBy(null);
            task.setLeaseUntil(null);
            taskRepository.save(task);
            log.info("[DLQ] Task [{}] replayed by [{}] back to PENDING queue.", dlq.getOriginalTaskId(), dlq.getReplayedBy());
            return true;
        }

        return false;
    }

    @Transactional
    public void purgeAll() {
        deadLetterRepository.deleteAll();
        log.info("[DLQ] Purged all dead letter records.");
    }
}
