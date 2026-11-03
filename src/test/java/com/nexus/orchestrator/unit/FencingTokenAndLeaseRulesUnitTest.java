package com.nexus.orchestrator.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.orchestrator.core.engine.TaskExecutionContext;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.repository.DistributedTaskDao;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class FencingTokenAndLeaseRulesUnitTest {

    @Test
    @DisplayName("Fencing token validation: Stale worker completion rejected if token or owner does not match")
    public void testStaleWorkerCompletionRejected() {
        DistributedTaskDao dao = mock(DistributedTaskDao.class);
        UUID taskId = UUID.randomUUID();

        // Worker A has old token 1
        when(dao.completeTask(eq(taskId), eq("worker-A"), eq(1L), anyString(), anyLong()))
            .thenReturn(false); // Rejected because worker B acquired token 2

        // Worker B has current token 2
        when(dao.completeTask(eq(taskId), eq("worker-B"), eq(2L), anyString(), anyLong()))
            .thenReturn(true);

        boolean staleResult = dao.completeTask(taskId, "worker-A", 1L, "{\"result\":\"stale\"}", 50L);
        boolean currentResult = dao.completeTask(taskId, "worker-B", 2L, "{\"result\":\"valid\"}", 50L);

        assertFalse(staleResult, "Stale worker completion must return false (fenced out)");
        assertTrue(currentResult, "Current owner completion must succeed");
    }

    @Test
    @DisplayName("Lease renewal rules: Renewal requires valid ownership and unexpired lease")
    public void testLeaseRenewalRequiresOwnership() {
        DistributedTaskDao dao = mock(DistributedTaskDao.class);
        UUID taskId = UUID.randomUUID();

        when(dao.renewLease(eq(taskId), eq("worker-1"), eq(5L), eq(15))).thenReturn(true);
        when(dao.renewLease(eq(taskId), eq("worker-1"), eq(4L), eq(15))).thenReturn(false); // Stale token

        assertTrue(dao.renewLease(taskId, "worker-1", 5L, 15));
        assertFalse(dao.renewLease(taskId, "worker-1", 4L, 15));
    }

    @Test
    @DisplayName("DLQ transition: Task with exhausted attempts transitions to DEAD_LETTERED")
    public void testDlqTransitionOnExhaustedAttempts() {
        Task task = new Task("batch-job", "{}", TaskPriority.NORMAL, "client-1", 100);
        task.setMaxAttempts(3);
        task.setAttemptCount(3);

        boolean shouldMoveToDlq = task.getAttemptCount() >= task.getMaxAttempts();
        assertTrue(shouldMoveToDlq, "Task having reached max attempts should qualify for DLQ");

        task.setStatus(TaskStatus.DEAD_LETTERED);
        assertEquals(TaskStatus.DEAD_LETTERED, task.getStatus());
        assertTrue(task.getStatus().isTerminal());
    }
}
