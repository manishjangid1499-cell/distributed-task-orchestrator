package com.nexus.orchestrator.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.orchestrator.core.metrics.SystemMetricsCollector;
import com.nexus.orchestrator.core.ratelimit.RateLimiter;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.dto.TaskSubmissionRequest;
import com.nexus.orchestrator.repository.TaskRepository;
import com.nexus.orchestrator.service.IdempotencyConflictException;
import com.nexus.orchestrator.service.TaskService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class IdempotencyValidationUnitTest {

    private TaskRepository taskRepository;
    private RateLimiter rateLimiter;
    private SystemMetricsCollector metrics;
    private TaskService taskService;

    @BeforeEach
    public void setup() {
        taskRepository = mock(TaskRepository.class);
        rateLimiter = mock(RateLimiter.class);
        metrics = new SystemMetricsCollector();

        when(rateLimiter.tryAcquire(any(), anyInt())).thenReturn(new RateLimiter.Result(true, 10, 10, 10, 0));
        when(taskRepository.saveAndFlush(any(Task.class))).thenAnswer(i -> i.getArgument(0));

        taskService = new TaskService(taskRepository, rateLimiter, metrics, 3);
    }

    @Test
    @DisplayName("Idempotency: Same key and identical payload returns existing task without duplicate insert")
    public void testSameKeySamePayloadReturnsExistingTask() {
        TaskSubmissionRequest req = new TaskSubmissionRequest();
        req.setTaskName("Payment-Sync");
        req.setPayload("{\"invoiceId\":\"INV-1001\"}");
        req.setIdempotencyKey("KEY-12345");

        Task initial = taskService.submitTask(req);
        when(taskRepository.findByIdempotencyKey(eq("KEY-12345"))).thenReturn(Optional.of(initial));

        Task reSubmitted = taskService.submitTask(req);

        assertEquals(initial.getId(), reSubmitted.getId());
        assertEquals(initial.getPayloadHash(), reSubmitted.getPayloadHash());
    }

    @Test
    @DisplayName("Idempotency conflict: Same key with different payload throws IdempotencyConflictException")
    public void testSameKeyDifferentPayloadThrowsConflict() {
        TaskSubmissionRequest req1 = new TaskSubmissionRequest();
        req1.setTaskName("Payment-Sync");
        req1.setPayload("{\"invoiceId\":\"INV-1001\"}");
        req1.setIdempotencyKey("KEY-12345");

        Task initial = taskService.submitTask(req1);
        when(taskRepository.findByIdempotencyKey(eq("KEY-12345"))).thenReturn(Optional.of(initial));

        TaskSubmissionRequest req2 = new TaskSubmissionRequest();
        req2.setTaskName("Payment-Sync");
        req2.setPayload("{\"invoiceId\":\"INV-9999-DIFFERENT\"}");
        req2.setIdempotencyKey("KEY-12345");

        assertThrows(IdempotencyConflictException.class, () -> taskService.submitTask(req2));
    }
}
