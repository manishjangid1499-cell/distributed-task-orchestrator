package com.nexus.orchestrator;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.isOneOf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class TaskControllerIntegrationTest {

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
            .andExpect(jsonPath("$.status", isOneOf("QUEUED", "RUNNING", "COMPLETED")));

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
