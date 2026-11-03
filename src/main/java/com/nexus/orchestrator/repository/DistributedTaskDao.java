package com.nexus.orchestrator.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import com.nexus.orchestrator.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class DistributedTaskDao {

    private static final Logger log = LoggerFactory.getLogger(DistributedTaskDao.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final JdbcTemplate jdbcTemplate;

    public DistributedTaskDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static class TaskRowMapper implements RowMapper<Task> {
        @Override
        public Task mapRow(ResultSet rs, int rowNum) throws SQLException {
            Task task = new Task();
            task.setId((UUID) rs.getObject("id"));
            task.setTaskType(rs.getString("task_type"));
            task.setTaskName(rs.getString("task_name"));
            
            String payloadStr = rs.getString("payload");
            if (payloadStr != null) {
                try {
                    task.setPayload(OBJECT_MAPPER.readTree(payloadStr));
                } catch (Exception e) {
                    task.setPayloadFromString(payloadStr);
                }
            }

            short prioLevel = rs.getShort("priority");
            task.setPriority(TaskPriority.fromLevel(prioLevel));

            String statusStr = rs.getString("status");
            task.setStatus(statusStr != null ? TaskStatus.valueOf(statusStr) : TaskStatus.PENDING);

            task.setIdempotencyKey(rs.getString("idempotency_key"));
            task.setPayloadHash(rs.getString("payload_hash"));
            task.setAttemptCount(rs.getInt("attempt_count"));
            task.setMaxAttempts(rs.getInt("max_attempts"));

            Timestamp availTs = rs.getTimestamp("available_at");
            if (availTs != null) task.setAvailableAt(availTs.toInstant());

            Timestamp createdTs = rs.getTimestamp("created_at");
            if (createdTs != null) task.setCreatedAt(createdTs.toInstant());

            Timestamp startedTs = rs.getTimestamp("started_at");
            if (startedTs != null) task.setStartedAt(startedTs.toInstant());

            Timestamp completedTs = rs.getTimestamp("completed_at");
            if (completedTs != null) task.setCompletedAt(completedTs.toInstant());

            task.setLockedBy(rs.getString("locked_by"));

            Timestamp leaseTs = rs.getTimestamp("lease_until");
            if (leaseTs != null) task.setLeaseUntil(leaseTs.toInstant());

            task.setFencingToken(rs.getLong("fencing_token"));
            task.setLastError(rs.getString("last_error"));

            String resultStr = rs.getString("result");
            if (resultStr != null) {
                try {
                    task.setResult(OBJECT_MAPPER.readTree(resultStr));
                } catch (Exception ignored) {}
            }

            task.setSubmittedBy(rs.getString("submitted_by"));

            long dur = rs.getLong("execution_duration_ms");
            if (!rs.wasNull()) task.setExecutionDurationMs(dur);

            return task;
        }
    }

    private static final TaskRowMapper TASK_ROW_MAPPER = new TaskRowMapper();

    /**
     * Atomically claims the next eligible task ordered by priority ASC, created_at ASC using
     * PostgreSQL SELECT ... FOR UPDATE SKIP LOCKED.
     * Increments attempt_count, fencing_token, updates locked_by, lease_until, started_at.
     */
    @Transactional
    public Optional<Task> claimNextTask(String workerId, int leaseDurationSeconds) {
        String claimSql = """
            WITH candidate AS (
                SELECT id
                FROM tasks
                WHERE (status IN ('PENDING', 'RETRY_WAIT') AND available_at <= CURRENT_TIMESTAMP)
                   OR (status = 'RUNNING' AND lease_until <= CURRENT_TIMESTAMP AND attempt_count < max_attempts)
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks t
            SET status = 'RUNNING',
                locked_by = ?,
                lease_until = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'),
                fencing_token = t.fencing_token + 1,
                attempt_count = t.attempt_count + 1,
                started_at = COALESCE(t.started_at, CURRENT_TIMESTAMP)
            FROM candidate
            WHERE t.id = candidate.id
            RETURNING t.*;
        """;

        try {
            List<Task> results = jdbcTemplate.query(claimSql, TASK_ROW_MAPPER, workerId, leaseDurationSeconds);
            if (results.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(results.get(0));
        } catch (Exception e) {
            log.error("[DistributedTaskDao] Error claiming next task for worker [{}]: {}", workerId, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Renews task lease if still owned by worker with matching fencing token and lease has not yet expired.
     */
    @Transactional
    public boolean renewLease(UUID taskId, String workerId, long fencingToken, int leaseDurationSeconds) {
        String renewSql = """
            UPDATE tasks
            SET lease_until = CURRENT_TIMESTAMP + (? * INTERVAL '1 second')
            WHERE id = ?
              AND locked_by = ?
              AND fencing_token = ?
              AND status = 'RUNNING'
              AND lease_until > CURRENT_TIMESTAMP
        """;
        int updated = jdbcTemplate.update(renewSql, leaseDurationSeconds, taskId, workerId, fencingToken);
        return updated > 0;
    }

    /**
     * Completes task execution conditionally verifying worker ownership and fencing token.
     * Stale workers will fail this update (returning false).
     */
    @Transactional
    public boolean completeTask(UUID taskId, String workerId, long fencingToken, String resultJson, long durationMs) {
        String completeSql = """
            UPDATE tasks
            SET status = 'COMPLETED',
                result = CAST(? AS jsonb),
                completed_at = CURRENT_TIMESTAMP,
                execution_duration_ms = ?,
                lease_until = NULL,
                locked_by = NULL
            WHERE id = ?
              AND locked_by = ?
              AND fencing_token = ?
              AND status = 'RUNNING'
        """;
        int updated = jdbcTemplate.update(completeSql, resultJson, durationMs, taskId, workerId, fencingToken);
        return updated > 0;
    }

    /**
     * Schedules a persistent retry with exponential backoff delay and releases worker lease.
     */
    @Transactional
    public boolean failAndScheduleRetry(UUID taskId, String workerId, long fencingToken, String errorMessage, int delaySeconds) {
        String retrySql = """
            UPDATE tasks
            SET status = 'RETRY_WAIT',
                available_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'),
                last_error = ?,
                lease_until = NULL,
                locked_by = NULL
            WHERE id = ?
              AND locked_by = ?
              AND fencing_token = ?
              AND status = 'RUNNING'
        """;
        int updated = jdbcTemplate.update(retrySql, delaySeconds, errorMessage, taskId, workerId, fencingToken);
        return updated > 0;
    }

    /**
     * Marks task DEAD_LETTERED conditionally verifying ownership and fencing token.
     */
    @Transactional
    public boolean markDeadLettered(UUID taskId, String workerId, long fencingToken, String finalError) {
        String dlqSql = """
            UPDATE tasks
            SET status = 'DEAD_LETTERED',
                last_error = ?,
                lease_until = NULL,
                locked_by = NULL
            WHERE id = ?
              AND locked_by = ?
              AND fencing_token = ?
              AND status = 'RUNNING'
        """;
        int updated = jdbcTemplate.update(dlqSql, finalError, taskId, workerId, fencingToken);
        return updated > 0;
    }

    /**
     * Sweeps running tasks whose leases have expired and attempts have reached max_attempts.
     * Transitions them directly to DEAD_LETTERED so they do not remain stuck.
     */
    @Transactional
    public List<Task> sweepExpiredExhaustedTasks(int limit) {
        String sweepSql = """
            WITH expired_candidates AS (
                SELECT id
                FROM tasks
                WHERE status = 'RUNNING'
                  AND lease_until <= CURRENT_TIMESTAMP
                  AND attempt_count >= max_attempts
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks t
            SET status = 'DEAD_LETTERED',
                last_error = 'Lease expired and attempt count exceeded maximum retries. Moved to DLQ.',
                lease_until = NULL,
                locked_by = NULL
            FROM expired_candidates
            WHERE t.id = expired_candidates.id
            RETURNING t.*;
        """;
        return jdbcTemplate.query(sweepSql, TASK_ROW_MAPPER, limit);
    }
}
