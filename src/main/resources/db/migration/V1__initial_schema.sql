-- Nexus 2.0 Distributed Task Orchestration Schema
-- Authoritative persistent store for tasks, workers, attempts, and DLQ

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    task_type VARCHAR(128) NOT NULL,
    task_name VARCHAR(128),
    payload JSONB NOT NULL,
    priority SMALLINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    idempotency_key VARCHAR(128) UNIQUE,
    payload_hash VARCHAR(64) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    available_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    locked_by VARCHAR(64),
    lease_until TIMESTAMPTZ,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    result JSONB,
    submitted_by VARCHAR(64),
    execution_duration_ms BIGINT
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id VARCHAR(64) PRIMARY KEY,
    hostname VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    capacity INTEGER NOT NULL DEFAULT 10,
    active_jobs INTEGER NOT NULL DEFAULT 0,
    total_processed_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS task_attempts (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    worker_id VARCHAR(64) NOT NULL,
    attempt_number INTEGER NOT NULL,
    fencing_token BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    outcome VARCHAR(32),
    error_code VARCHAR(64),
    error_message TEXT,
    execution_duration_ms BIGINT
);

CREATE TABLE IF NOT EXISTS dead_letter_tasks (
    id UUID PRIMARY KEY,
    original_task_id UUID NOT NULL UNIQUE,
    task_type VARCHAR(128) NOT NULL,
    task_name VARCHAR(128),
    payload JSONB NOT NULL,
    priority SMALLINT NOT NULL,
    total_attempts INTEGER NOT NULL,
    final_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    replayed_at TIMESTAMPTZ,
    replayed_by VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS execution_records (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    task_id UUID NOT NULL,
    worker_id VARCHAR(64) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    execution_data JSONB
);

-- Indexes for performance and high-throughput polling
CREATE INDEX IF NOT EXISTS idx_tasks_ready_claim 
    ON tasks (status, available_at, priority, created_at);

CREATE INDEX IF NOT EXISTS idx_tasks_expired_lease 
    ON tasks (status, lease_until, attempt_count);

CREATE INDEX IF NOT EXISTS idx_tasks_status 
    ON tasks (status);

CREATE INDEX IF NOT EXISTS idx_tasks_locked_by 
    ON tasks (locked_by);

CREATE INDEX IF NOT EXISTS idx_tasks_idempotency_key 
    ON tasks (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_workers_heartbeat 
    ON workers (last_heartbeat_at, status);

CREATE INDEX IF NOT EXISTS idx_attempts_task_id 
    ON task_attempts (task_id);

CREATE INDEX IF NOT EXISTS idx_attempts_worker_id 
    ON task_attempts (worker_id);

CREATE INDEX IF NOT EXISTS idx_dlq_created_at 
    ON dead_letter_tasks (created_at);
