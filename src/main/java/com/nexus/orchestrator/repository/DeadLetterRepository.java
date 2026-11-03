package com.nexus.orchestrator.repository;

import com.nexus.orchestrator.model.DeadLetterTask;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface DeadLetterRepository extends JpaRepository<DeadLetterTask, UUID> {

    Optional<DeadLetterTask> findByOriginalTaskId(UUID originalTaskId);

    List<DeadLetterTask> findByReplayedAtIsNullOrderByCreatedAtDesc();

    List<DeadLetterTask> findAllByOrderByCreatedAtDesc();

    long countByReplayedAtIsNull();

    // Backward compatibility helper
    default Optional<DeadLetterTask> findByTaskId(String taskId) {
        try {
            return findByOriginalTaskId(UUID.fromString(taskId));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
