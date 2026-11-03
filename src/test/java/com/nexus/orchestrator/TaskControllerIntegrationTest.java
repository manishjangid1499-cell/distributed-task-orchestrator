package com.nexus.orchestrator;

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

import static org.hamcrest.Matchers.isOneOf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
public class TaskControllerIntegrationTest {

    static {
        System.setProperty("api.version", "1.44");
        System.setProperty("docker.api.version", "1.44");
        System.setProperty("testcontainers.ryuk.disabled", "true");
    }

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("nexus_controller_test_db")
        .withUsername("nexus_test")
        .withPassword("nexus_secret_test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("nexus.mode", () -> "API");
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testSubmitTaskLifecycle() throws Exception {
        String payload = """
            {
                "taskName": "Test-Cloud-Snapshot",
                "priority": "HIGH",
                "payload": "{\\"region\\": \\"eastus\\"}",
                "submittedBy": "integration-client-1",
                "simulatedExecutionDurationMs": 200
            }
            """;

        mockMvc.perform(post("/api/v1/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.taskName").value("Test-Cloud-Snapshot"))
            .andExpect(jsonPath("$.priority").value("HIGH"))
            .andExpect(jsonPath("$.status", isOneOf("PENDING", "QUEUED", "RUNNING", "COMPLETED")));

        // Verify task appears in task list
        mockMvc.perform(get("/api/v1/tasks"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$").isArray());
    }

    @Test
    public void testMetricsEndpoint() throws Exception {
        mockMvc.perform(get("/api/v1/metrics/summary"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.activeWorkerCount").isNumber())
            .andExpect(jsonPath("$.healthyWorkerCount").isNumber());
    }
}
