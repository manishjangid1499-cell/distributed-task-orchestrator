package com.nexus.orchestrator.repository;

import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TaskRepository extends JpaRepository<Task, Long> {

    Optional<Task> findByTaskId(String taskId);

    List<Task> findByStatus(TaskStatus status);

    List<Task> findByPriority(TaskPriority priority);

    List<Task> findByAssignedWorkerIdAndStatus(String workerId, TaskStatus status);

    @Query("SELECT t FROM Task t ORDER BY t.createdAt DESC")
    List<Task> findRecentTasks();

    long countByStatus(TaskStatus status);
}
