package com.nexus.orchestrator;

import com.nexus.orchestrator.core.engine.TaskExecutionEngine;
import com.nexus.orchestrator.core.metrics.SystemMetricsCollector;
import com.nexus.orchestrator.model.DeadLetterRecord;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.repository.DeadLetterRepository;
import com.nexus.orchestrator.repository.TaskRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TaskExecutionEngineTest {

    private TaskRepository taskRepository;
    private DeadLetterRepository deadLetterRepository;
    private SystemMetricsCollector metrics;
    private TaskExecutionEngine engine;

    @BeforeEach
    public void setup() {
        taskRepository = mock(TaskRepository.class);
        deadLetterRepository = mock(DeadLetterRepository.class);
        metrics = new SystemMetricsCollector();

        when(taskRepository.save(any(Task.class))).thenAnswer(i -> i.getArgument(0));

        engine = new TaskExecutionEngine(taskRepository, deadLetterRepository, metrics, 4, 8, 100);
        engine.initialize();
    }

    @AfterEach
    public void teardown() {
        engine.shutdown();
    }

    @Test
    public void testTaskExecutionSuccess() throws InterruptedException {
        Task task = new Task("Index-Azure-Disks", "{'disk': 'disk-01'}", TaskPriority.HIGH, "client-1", 50);

        engine.submit(task, "worker-1");

        // Wait for asynchronous execution
        Thread.sleep(300);

        assertEquals(TaskStatus.COMPLETED, task.getStatus());
        assertNotNull(task.getCompletedAt());
        assertTrue(task.getExecutionDurationMs() >= 40);
        assertEquals(1, metrics.getTotalCompleted());
    }

    @Test
    public void testTaskFailureRoutesToDLQAfterMaxRetries() throws InterruptedException {
        Task task = new Task("Failing-Task", "SIMULATE_FAILURE", TaskPriority.CRITICAL, "client-fail", 10);
        task.setMaxRetries(1); // Set to 1 so it exhausts quickly

        engine.submit(task, "worker-1");

        // Allow retry and backoff loop to execute
        Thread.sleep(2500);

        assertEquals(TaskStatus.DEAD_LETTERED, task.getStatus());
        verify(deadLetterRepository, atLeastOnce()).save(any(DeadLetterRecord.class));
        assertEquals(1, metrics.getTotalDeadLettered());
    }
}
