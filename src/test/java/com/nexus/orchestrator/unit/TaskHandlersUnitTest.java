package com.nexus.orchestrator.unit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.orchestrator.core.engine.*;
import com.nexus.orchestrator.core.engine.handlers.*;
import com.nexus.orchestrator.model.ExecutionRecord;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.repository.ExecutionRecordRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TaskHandlersUnitTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @DisplayName("NoOpTaskHandler should complete immediately")
    public void testNoOpHandler() {
        NoOpTaskHandler handler = new NoOpTaskHandler();
        assertEquals("NO_OP", handler.taskType());

        UUID taskId = UUID.randomUUID();
        TaskExecutionContext context = new TaskExecutionContext(taskId, "w1", 1, 1L, TaskPriority.NORMAL, null, () -> true);
        JsonNode payload = MAPPER.createObjectNode();

        JsonNode result = handler.execute(payload, context);
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status").asText());
        assertEquals("w1", result.get("workerId").asText());
    }

    @Test
    @DisplayName("SleepTaskHandler should sleep and return elapsed time")
    public void testSleepHandler() throws Exception {
        SleepTaskHandler handler = new SleepTaskHandler();
        assertEquals("SLEEP", handler.taskType());

        UUID taskId = UUID.randomUUID();
        TaskExecutionContext context = new TaskExecutionContext(taskId, "w1", 1, 1L, TaskPriority.NORMAL, null, () -> true);
        JsonNode payload = MAPPER.createObjectNode().put("durationMs", 50);

        JsonNode result = handler.execute(payload, context);
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status").asText());
        assertEquals(50, result.get("sleptMs").asLong());
    }

    @Test
    @DisplayName("FailNTimesTaskHandler should throw RetryableTaskException until target reached")
    public void testFailNTimesHandler() {
        FailNTimesTaskHandler handler = new FailNTimesTaskHandler();
        assertEquals("FAIL_N_TIMES", handler.taskType());

        UUID taskId = UUID.randomUUID();
        JsonNode payload = MAPPER.createObjectNode().put("failAttempts", 2);

        // Attempt 1 when target is 2 should fail
        TaskExecutionContext ctx1 = new TaskExecutionContext(taskId, "w1", 1, 1L, TaskPriority.NORMAL, null, () -> true);
        assertThrows(RetryableTaskException.class, () -> handler.execute(payload, ctx1));

        // Attempt 2 when target is 2 should fail
        TaskExecutionContext ctx2 = new TaskExecutionContext(taskId, "w1", 2, 2L, TaskPriority.NORMAL, null, () -> true);
        assertThrows(RetryableTaskException.class, () -> handler.execute(payload, ctx2));

        // Attempt 3 should succeed
        TaskExecutionContext ctx3 = new TaskExecutionContext(taskId, "w1", 3, 3L, TaskPriority.NORMAL, null, () -> true);
        JsonNode result = handler.execute(payload, ctx3);
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status").asText());
        assertEquals(3, result.get("totalAttemptsRequired").asInt());
    }

    @Test
    @DisplayName("RecordExecutionTaskHandler should persist execution record idempotently")
    public void testRecordExecutionHandler() {
        ExecutionRecordRepository recordRepo = mock(ExecutionRecordRepository.class);
        RecordExecutionTaskHandler handler = new RecordExecutionTaskHandler(recordRepo);
        assertEquals("RECORD_EXECUTION", handler.taskType());

        UUID taskId = UUID.randomUUID();
        TaskExecutionContext ctx = new TaskExecutionContext(taskId, "w1", 1, 1L, TaskPriority.NORMAL, "REC-001", () -> true);
        JsonNode payload = MAPPER.createObjectNode().put("amount", 150.0);

        when(recordRepo.findByIdempotencyKey("REC-001")).thenReturn(Optional.empty());
        when(recordRepo.save(any(ExecutionRecord.class))).thenAnswer(invocation -> invocation.getArgument(0));

        JsonNode result = handler.execute(payload, ctx);
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status").asText());
        assertEquals("REC-001", result.get("key").asText());
        assertFalse(result.get("idempotentReplay").asBoolean());
        verify(recordRepo, times(1)).save(any(ExecutionRecord.class));

        // Second execution should hit idempotent cache
        ExecutionRecord existing = new ExecutionRecord("REC-001", taskId, "w1", "hash", MAPPER.createObjectNode());
        when(recordRepo.findByIdempotencyKey("REC-001")).thenReturn(Optional.of(existing));

        JsonNode result2 = handler.execute(payload, ctx);
        assertNotNull(result2);
        assertTrue(result2.get("idempotentReplay").asBoolean());
    }
}
