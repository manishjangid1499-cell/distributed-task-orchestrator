package com.nexus.orchestrator.repository;

import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<Task, UUID> {

    Optional<Task> findByIdempotencyKey(String idempotencyKey);

    List<Task> findByStatus(TaskStatus status);

    List<Task> findByPriority(TaskPriority priority);

    List<Task> findByLockedByAndStatus(String workerId, TaskStatus status);

    @Query("SELECT t FROM Task t ORDER BY t.createdAt DESC")
    List<Task> findRecentTasks();

    long countByStatus(TaskStatus status);

    Page<Task> findByStatusAndPriority(TaskStatus status, TaskPriority priority, Pageable pageable);

    Page<Task> findByStatus(TaskStatus status, Pageable pageable);

    Page<Task> findByPriority(TaskPriority priority, Pageable pageable);

    // Backward compatibility helper
    default Optional<Task> findByTaskId(String taskId) {
        try {
            return findById(UUID.fromString(taskId));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    default List<Task> findByAssignedWorkerIdAndStatus(String workerId, TaskStatus status) {
        return findByLockedByAndStatus(workerId, status);
    }
}
