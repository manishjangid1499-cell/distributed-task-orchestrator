package com.nexus.orchestrator;

import com.nexus.orchestrator.core.engine.TaskExecutionEngine;
import com.nexus.orchestrator.core.supervisor.WorkerHeartbeatSupervisor;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.model.WorkerStatus;
import com.nexus.orchestrator.repository.TaskRepository;
import com.nexus.orchestrator.repository.WorkerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class WorkerHeartbeatSupervisorTest {

    private WorkerRepository workerRepository;
    private TaskRepository taskRepository;
    private TaskExecutionEngine taskExecutionEngine;
    private WorkerHeartbeatSupervisor supervisor;

    @BeforeEach
    public void setup() {
        workerRepository = mock(WorkerRepository.class);
        taskRepository = mock(TaskRepository.class);
        taskExecutionEngine = mock(TaskExecutionEngine.class);

        // Timeout of 10 seconds
        supervisor = new WorkerHeartbeatSupervisor(workerRepository, taskRepository, taskExecutionEngine, 10);
    }

    @Test
    public void testDetectsDeadWorkerAndReclaimsOrphanTasks() {
        // Worker 1 is dead (last heartbeat 25 seconds ago)
        WorkerNode deadWorker = new WorkerNode("worker-dead-1", "host-crashed.internal", 10);
        deadWorker.setLastHeartbeat(Instant.now().minusSeconds(25));
        deadWorker.setStatus(WorkerStatus.HEALTHY);

        // Worker 2 is healthy
        WorkerNode healthyWorker = new WorkerNode("worker-healthy-2", "host-alive.internal", 10);
        healthyWorker.setLastHeartbeat(Instant.now());
        healthyWorker.setStatus(WorkerStatus.HEALTHY);

        when(workerRepository.findAll()).thenReturn(List.of(deadWorker, healthyWorker));

        // Orphan task that was RUNNING on the dead worker
        Task orphanTask = new Task("Reclaim-Target", "data", TaskPriority.HIGH, "client", 100);
        orphanTask.setStatus(TaskStatus.RUNNING);
        orphanTask.setAssignedWorkerId(deadWorker.getWorkerId());

        List<Task> orphanList = new ArrayList<>();
        orphanList.add(orphanTask);
        when(taskRepository.findByAssignedWorkerIdAndStatus(eq(deadWorker.getWorkerId()), eq(TaskStatus.RUNNING)))
            .thenReturn(orphanList);
        when(taskRepository.findByAssignedWorkerIdAndStatus(eq(deadWorker.getWorkerId()), eq(TaskStatus.QUEUED)))
            .thenReturn(new ArrayList<>());

        supervisor.monitorClusterHealth();

        // Worker should be transitioned to CRASHED
        assertEquals(WorkerStatus.CRASHED, deadWorker.getStatus());
        verify(workerRepository, atLeastOnce()).save(deadWorker);

        // Orphan task should be resubmitted to the healthy worker
        verify(taskExecutionEngine, times(1)).submit(eq(orphanTask), eq("worker-healthy-2"));
        assertEquals(1, orphanTask.getRetryCount());
    }
}
