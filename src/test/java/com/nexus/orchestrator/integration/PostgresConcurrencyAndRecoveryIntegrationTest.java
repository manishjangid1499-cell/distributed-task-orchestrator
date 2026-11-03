package com.nexus.orchestrator.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.orchestrator.core.engine.RetryPolicy;
import com.nexus.orchestrator.model.DeadLetterTask;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import com.nexus.orchestrator.repository.DeadLetterRepository;
import com.nexus.orchestrator.repository.DistributedTaskDao;
import com.nexus.orchestrator.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
public class PostgresConcurrencyAndRecoveryIntegrationTest {

    static {
        System.setProperty("api.version", "1.44");
        System.setProperty("docker.api.version", "1.44");
        System.setProperty("testcontainers.ryuk.disabled", "true");
    }

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("nexus_test_db")
        .withUsername("nexus_test")
        .withPassword("nexus_secret_test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("nexus.mode", () -> "API"); // Run tests in API mode so background worker polling doesn't race tests
    }

    @Autowired
    private DistributedTaskDao taskDao;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private DeadLetterRepository deadLetterRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    public void cleanup() {
        deadLetterRepository.deleteAll();
        taskRepository.deleteAll();
    }

    private Task createAndSaveTask(String name, TaskPriority priority, Instant availableAt, int maxAttempts, String idempotencyKey) {
        Task t = new Task();
        t.setTaskType(name);
        t.setTaskName(name);
        t.setPriority(priority);
        t.setStatus(TaskStatus.PENDING);
        t.setAvailableAt(availableAt != null ? availableAt : Instant.now());
        t.setMaxAttempts(maxAttempts);
        t.setIdempotencyKey(idempotencyKey);
        t.setPayloadHash(UUID.randomUUID().toString());
        t.setPayloadFromString("{\"test\":\"data\"}");
        return taskRepository.saveAndFlush(t);
    }

    @Test
    @DisplayName("1 & 2. Concurrent Claiming: Two concurrent workers claim different tasks; no task is claimed twice simultaneously")
    public void testConcurrentClaimingExclusivity() throws Exception {
        int taskCount = 10;
        for (int i = 0; i < taskCount; i++) {
            createAndSaveTask("Job-" + i, TaskPriority.NORMAL, Instant.now().minusSeconds(1), 3, null);
        }

        int workerCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);

        Set<UUID> claimedTaskIds = ConcurrentHashMap.newKeySet();
        AtomicInteger totalClaims = new AtomicInteger(0);

        for (int w = 0; w < workerCount; w++) {
            final String workerId = "worker-concurrent-" + w;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (true) {
                        Optional<Task> claimed = taskDao.claimNextTask(workerId, 15);
                        if (claimed.isEmpty()) {
                            break;
                        }
                        totalClaims.incrementAndGet();
                        boolean added = claimedTaskIds.add(claimed.get().getId());
                        assertTrue(added, "Task was claimed more than once concurrently!");
                    }
                } catch (Exception e) {
                    fail("Worker failed with exception: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals(taskCount, totalClaims.get(), "All tasks should be claimed exactly once");
        assertEquals(taskCount, claimedTaskIds.size(), "Each claimed task must be unique");
    }

    @Test
    @DisplayName("3. Priority Scheduling: CRITICAL tasks are claimed before HIGH, NORMAL, and LOW")
    public void testPriorityOrderingInClaim() {
        createAndSaveTask("low-task", TaskPriority.LOW, Instant.now().minusSeconds(10), 3, null);
        createAndSaveTask("crit-task", TaskPriority.CRITICAL, Instant.now().minusSeconds(1), 3, null);
        createAndSaveTask("norm-task", TaskPriority.NORMAL, Instant.now().minusSeconds(5), 3, null);
        createAndSaveTask("high-task", TaskPriority.HIGH, Instant.now().minusSeconds(3), 3, null);

        Optional<Task> firstClaim = taskDao.claimNextTask("worker-test", 15);
        assertTrue(firstClaim.isPresent());
        assertEquals(TaskPriority.CRITICAL, firstClaim.get().getPriority());

        Optional<Task> secondClaim = taskDao.claimNextTask("worker-test", 15);
        assertTrue(secondClaim.isPresent());
        assertEquals(TaskPriority.HIGH, secondClaim.get().getPriority());

        Optional<Task> thirdClaim = taskDao.claimNextTask("worker-test", 15);
        assertTrue(thirdClaim.isPresent());
        assertEquals(TaskPriority.NORMAL, thirdClaim.get().getPriority());

        Optional<Task> fourthClaim = taskDao.claimNextTask("worker-test", 15);
        assertTrue(fourthClaim.isPresent());
        assertEquals(TaskPriority.LOW, fourthClaim.get().getPriority());
    }

    @Test
    @DisplayName("4, 5 & 6. Lease Expiry & Recovery: Second worker reclaims expired task; fencing token increments")
    public void testLeaseExpirationAndReclaimWithFencingTokenIncrement() {
        Task task = createAndSaveTask("crash-candidate", TaskPriority.HIGH, Instant.now().minusSeconds(1), 3, null);

        // Worker 1 claims task with 1-second lease
        Optional<Task> claim1 = taskDao.claimNextTask("worker-1", 1);
        assertTrue(claim1.isPresent());
        assertEquals("worker-1", claim1.get().getLockedBy());
        long token1 = claim1.get().getFencingToken();
        assertEquals(1, claim1.get().getAttemptCount());

        // Wait for lease to expire
        jdbcTemplate.update("UPDATE tasks SET lease_until = CURRENT_TIMESTAMP - INTERVAL '2 second' WHERE id = ?", task.getId());

        // Worker 2 reclaims expired task
        Optional<Task> claim2 = taskDao.claimNextTask("worker-2", 15);
        assertTrue(claim2.isPresent(), "Worker 2 should reclaim expired task");
        assertEquals("worker-2", claim2.get().getLockedBy());
        assertEquals(token1 + 1, claim2.get().getFencingToken(), "Fencing token must increment on re-claim");
        assertEquals(2, claim2.get().getAttemptCount(), "Attempt count must increment on re-claim");
    }

    @Test
    @DisplayName("7 & 8. Fencing Token Rejection: Old worker cannot complete or renew reclaimed task")
    public void testOldWorkerCannotCompleteOrRenewReclaimedTask() {
        Task task = createAndSaveTask("reclaim-fence-test", TaskPriority.NORMAL, Instant.now().minusSeconds(1), 3, null);

        // Worker 1 claims task
        Optional<Task> claim1 = taskDao.claimNextTask("worker-1", 1);
        assertTrue(claim1.isPresent());
        long staleToken = claim1.get().getFencingToken();

        // Simulate lease expiration and Worker 2 takeover
        jdbcTemplate.update("UPDATE tasks SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?", task.getId());
        Optional<Task> claim2 = taskDao.claimNextTask("worker-2", 15);
        assertTrue(claim2.isPresent());
        long activeToken = claim2.get().getFencingToken();

        // 1. Worker 1 attempts to renew lease -> MUST FAIL
        boolean renewAttempt = taskDao.renewLease(task.getId(), "worker-1", staleToken, 15);
        assertFalse(renewAttempt, "Stale worker cannot renew lease");

        // 2. Worker 1 attempts to complete task -> MUST FAIL
        boolean completeAttempt = taskDao.completeTask(task.getId(), "worker-1", staleToken, "{\"result\":\"stale\"}", 100);
        assertFalse(completeAttempt, "Stale worker completion must be rejected by fencing token validation");

        // 3. Worker 2 completes task with active token -> MUST SUCCEED
        boolean validComplete = taskDao.completeTask(task.getId(), "worker-2", activeToken, "{\"result\":\"valid\"}", 120);
        assertTrue(validComplete, "Active owner with valid fencing token must succeed in completion");

        Task finalState = taskRepository.findById(task.getId()).orElseThrow();
        assertEquals(TaskStatus.COMPLETED, finalState.getStatus());
        assertNull(finalState.getLockedBy());
    }

    @Test
    @DisplayName("9. DLQ Transition: Sweeping expired tasks that reached max_attempts moves them to DEAD_LETTERED")
    public void testExhaustedTaskMovesToDlq() {
        Task task = createAndSaveTask("exhausted-task", TaskPriority.NORMAL, Instant.now().minusSeconds(1), 2, null);

        // Claim 1
        taskDao.claimNextTask("worker-1", 1);
        jdbcTemplate.update("UPDATE tasks SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?", task.getId());

        // Claim 2 (reaches max_attempts = 2)
        taskDao.claimNextTask("worker-2", 1);
        jdbcTemplate.update("UPDATE tasks SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?", task.getId());

        // Candidate has status=RUNNING, lease_until in past, attempt_count=2, max_attempts=2
        List<Task> swept = taskDao.sweepExpiredExhaustedTasks(5);
        assertEquals(1, swept.size(), "Exhausted expired task should be swept");
        assertEquals(TaskStatus.DEAD_LETTERED, swept.get(0).getStatus());

        Task persisted = taskRepository.findById(task.getId()).orElseThrow();
        assertEquals(TaskStatus.DEAD_LETTERED, persisted.getStatus());
    }

    @Test
    @DisplayName("10. Retry Scheduling: Task with RETRY_WAIT becomes eligible only after available_at")
    public void testRetryEligibleOnlyAfterAvailableAt() {
        Task task = createAndSaveTask("retry-timing-task", TaskPriority.NORMAL, Instant.now().minusSeconds(1), 3, null);

        Optional<Task> claim = taskDao.claimNextTask("worker-1", 15);
        assertTrue(claim.isPresent());

        // Schedule retry for 5 seconds in future
        taskDao.failAndScheduleRetry(task.getId(), "worker-1", claim.get().getFencingToken(), "Simulated failure", 5);

        // Immediately try to claim: must NOT be eligible yet
        Optional<Task> immediateClaim = taskDao.claimNextTask("worker-2", 15);
        assertTrue(immediateClaim.isEmpty(), "Task in RETRY_WAIT must not be eligible before available_at");

        // Advance database clock / available_at into past
        jdbcTemplate.update("UPDATE tasks SET available_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?", task.getId());

        // Now it must be eligible
        Optional<Task> retriedClaim = taskDao.claimNextTask("worker-2", 15);
        assertTrue(retriedClaim.isPresent(), "Task must become eligible after available_at timestamp arrives");
        assertEquals(2, retriedClaim.get().getAttemptCount());
    }

    @Test
    @DisplayName("11. Idempotency Key Constraint: Duplicate key rejection at PostgreSQL constraint level")
    public void testDuplicateIdempotencyKeyConstraint() {
        createAndSaveTask("idemp-1", TaskPriority.NORMAL, Instant.now(), 3, "IDEMP-UNIQUE-KEY");

        Task duplicate = new Task();
        duplicate.setTaskType("idemp-2");
        duplicate.setTaskName("idemp-2");
        duplicate.setPriority(TaskPriority.NORMAL);
        duplicate.setStatus(TaskStatus.PENDING);
        duplicate.setIdempotencyKey("IDEMP-UNIQUE-KEY");
        duplicate.setPayloadHash("hash2");
        duplicate.setPayloadFromString("{}");

        assertThrows(DataIntegrityViolationException.class, () -> {
            taskRepository.saveAndFlush(duplicate);
        });
    }

    @Test
    @DisplayName("12. Idempotent Demonstration Handler: RECORD_EXECUTION safely survives repeated execution")
    public void testRecordExecutionHandlerIdempotentReplay() {
        Task task = createAndSaveTask("record-task", TaskPriority.NORMAL, Instant.now().minusSeconds(1), 3, "PAYMENT-REQ-999");
        Optional<Task> claim = taskDao.claimNextTask("worker-1", 15);
        assertTrue(claim.isPresent());

        // First execution write
        String insertSql = """
            INSERT INTO execution_records (id, idempotency_key, task_id, worker_id, recorded_at, payload_hash, execution_data)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CAST(? AS jsonb))
        """;
        int inserted = jdbcTemplate.update(insertSql, UUID.randomUUID(), "PAYMENT-REQ-999", task.getId(), "worker-1", "hash", "{\"amount\":100}");
        assertEquals(1, inserted);

        // Duplicate write fails with unique constraint
        assertThrows(Exception.class, () -> {
            jdbcTemplate.update(insertSql, UUID.randomUUID(), "PAYMENT-REQ-999", task.getId(), "worker-2", "hash", "{\"amount\":100}");
        });

        // Verify exactly one ledger row exists
        Integer count = jdbcTemplate.queryForObject("SELECT count(*) FROM execution_records WHERE idempotency_key = 'PAYMENT-REQ-999'", Integer.class);
        assertEquals(1, count);
    }
}
