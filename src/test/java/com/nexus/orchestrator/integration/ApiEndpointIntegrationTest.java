package com.nexus.orchestrator.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.orchestrator.model.DeadLetterTask;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.repository.DeadLetterRepository;
import com.nexus.orchestrator.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
public class ApiEndpointIntegrationTest {

    static {
        System.setProperty("api.version", "1.44");
        System.setProperty("docker.api.version", "1.44");
        System.setProperty("testcontainers.ryuk.disabled", "true");
    }

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("nexus_api_test_db")
        .withUsername("nexus_test")
        .withPassword("nexus_secret_test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("nexus.api.key", () -> "admin-secret-test-key");
        registry.add("nexus.mode", () -> "API");
        registry.add("nexus.scheduler.rate-limit.default-requests-per-window", () -> "10");
        registry.add("nexus.scheduler.rate-limit.window-seconds", () -> "5");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private DeadLetterRepository deadLetterRepository;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    public void setup() {
        deadLetterRepository.deleteAll();
        taskRepository.deleteAll();
    }

    @Test
    @DisplayName("POST /api/v1/tasks: Returns 202 Accepted on valid task submission")
    public void testSubmitTaskAccepted() throws Exception {
        String body = """
            {
                "taskName": "Cloud-Data-Index",
                "taskType": "NO_OP",
                "priority": "HIGH",
                "payload": "{\\"bucket\\":\\"prod-data\\"}",
                "submittedBy": "client-test-1"
            }
            """;

        mockMvc.perform(post("/api/v1/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.taskId").isNotEmpty())
            .andExpect(jsonPath("$.taskName").value("Cloud-Data-Index"))
            .andExpect(jsonPath("$.priority").value("HIGH"))
            .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    @DisplayName("POST /api/v1/tasks: Returns 409 Conflict when same idempotency key is submitted with different payload")
    public void testIdempotencyConflict() throws Exception {
        String body1 = """
            {
                "taskName": "Payment-Transfer",
                "payload": "{\\"amount\\": 100}",
                "idempotencyKey": "PAY-IDEMP-001",
                "submittedBy": "client-idemp"
            }
            """;

        String body2 = """
            {
                "taskName": "Payment-Transfer",
                "payload": "{\\"amount\\": 200}",
                "idempotencyKey": "PAY-IDEMP-001",
                "submittedBy": "client-idemp"
            }
            """;

        // First submission succeeds
        mockMvc.perform(post("/api/v1/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body1))
            .andExpect(status().isAccepted());

        // Same submission again succeeds with same task (idempotency)
        mockMvc.perform(post("/api/v1/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body1))
            .andExpect(status().isAccepted());

        // Different payload with same key must return 409 Conflict
        mockMvc.perform(post("/api/v1/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body2))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.error").value("Conflict"));
    }

    @Test
    @DisplayName("GET /api/v1/tasks/{id}: Returns 404 Not Found for non-existent task")
    public void testGetTaskNotFound() throws Exception {
        mockMvc.perform(get("/api/v1/tasks/" + UUID.randomUUID()))
            .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("POST /api/v1/tasks/{id}/cancel: Cancels pending task, but returns 409 Conflict if task is already RUNNING")
    public void testCancelTaskRules() throws Exception {
        Task pending = new Task();
        pending.setTaskType("PENDING-TASK");
        pending.setStatus(TaskStatus.PENDING);
        pending.setPriority(TaskPriority.NORMAL);
        pending.setPayloadFromString("{}");
        pending.setPayloadHash("hash1");
        pending.setAvailableAt(Instant.now());
        taskRepository.saveAndFlush(pending);

        // Cancel pending task -> 200 OK
        mockMvc.perform(post("/api/v1/tasks/" + pending.getId() + "/cancel"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("success"));

        // Cancel running task -> 409 Conflict
        Task running = new Task();
        running.setTaskType("RUNNING-TASK");
        running.setStatus(TaskStatus.RUNNING);
        running.setPriority(TaskPriority.CRITICAL);
        running.setPayloadFromString("{}");
        running.setPayloadHash("hash2");
        running.setAvailableAt(Instant.now());
        taskRepository.saveAndFlush(running);

        mockMvc.perform(post("/api/v1/tasks/" + running.getId() + "/cancel"))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.status").value("conflict"));
    }

    @Test
    @DisplayName("POST /api/v1/tasks: Returns 429 Too Many Requests when rate limit is exceeded")
    public void testRateLimitingHeaderAndStatus() throws Exception {
        String client = "throttled-client";

        // Consume all 10 tokens
        for (int i = 0; i < 10; i++) {
            String body = "{\"taskName\":\"job\",\"submittedBy\":\"" + client + "\"}";
            mockMvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted());
        }

        // 11th request returns 429
        String body = "{\"taskName\":\"job-exceeded\",\"submittedBy\":\"" + client + "\"}";
        mockMvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
            .andExpect(status().isTooManyRequests())
            .andExpect(header().exists("Retry-After"))
            .andExpect(header().string("X-RateLimit-Error", "QuotaExceeded"))
            .andExpect(jsonPath("$.error").value("Too Many Requests"));
    }

    @Test
    @DisplayName("DLQ Replay: Requires API key authorization")
    public void testDlqReplayAuthorization() throws Exception {
        Task task = new Task();
        task.setTaskType("DLQ-TARGET");
        task.setStatus(TaskStatus.DEAD_LETTERED);
        task.setPriority(TaskPriority.HIGH);
        task.setPayloadFromString("{}");
        task.setPayloadHash("hash");
        task.setAvailableAt(Instant.now());
        taskRepository.saveAndFlush(task);

        DeadLetterTask dlq = new DeadLetterTask(
            task.getId(),
            "DLQ-TARGET",
            "DLQ-TARGET",
            task.getPayload(),
            (short) 1,
            3,
            "Permanent crash"
        );
        deadLetterRepository.saveAndFlush(dlq);

        // 1. Without API key -> 401 Unauthorized
        mockMvc.perform(post("/api/v1/dlq/" + task.getId() + "/replay"))
            .andExpect(status().isUnauthorized());

        // 2. With valid API key -> 200 OK
        mockMvc.perform(post("/api/v1/dlq/" + task.getId() + "/replay")
                .header("X-API-KEY", "admin-secret-test-key"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("success"));
    }
}
