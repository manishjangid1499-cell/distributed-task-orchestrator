// Nexus Distributed Orchestrator Dashboard Logic

document.addEventListener("DOMContentLoaded", () => {
    refreshAll();
    setInterval(refreshAll, 3000);

    document.getElementById("btn-submit-task").addEventListener("click", promptSubmitTask);
    document.getElementById("btn-flood-traffic").addEventListener("click", floodTrafficForRateLimit);
    document.getElementById("btn-chaos-crash").addEventListener("click", killRandomWorker);
});

async function refreshAll() {
    fetchMetrics();
    fetchWorkers();
    fetchTasks();
    fetchDLQ();
}

async function fetchMetrics() {
    try {
        const res = await fetch("/api/v1/metrics/summary");
        if (!res.ok) return;
        const d = await res.json();

        document.getElementById("val-submitted").innerText = d.totalTasksSubmitted;
        document.getElementById("val-completed").innerText = `${d.totalTasksCompleted} Completed | ${d.totalTasksFailed} Failed`;
        document.getElementById("val-queue-depth").innerText = d.queueDepth;
        document.getElementById("val-workers").innerText = `${d.healthyWorkerCount} / ${d.activeWorkerCount}`;
        document.getElementById("val-rate-limit-breaches").innerText = d.rateLimitViolationsCount;
        document.getElementById("val-avg-latency").innerText = `Avg Latency: ${d.averageExecutionLatencyMs}ms`;
    } catch (e) {
        console.error("Metrics fetch failed", e);
    }
}

async function fetchWorkers() {
    try {
        const res = await fetch("/api/v1/workers");
        if (!res.ok) return;
        const workers = await res.json();

        const grid = document.getElementById("worker-grid");
        grid.innerHTML = workers.map(w => {
            let statusBadge = "badge-success";
            if (w.status === "CRASHED") statusBadge = "badge-danger";
            else if (w.status === "BUSY") statusBadge = "badge-warning";

            return `
                <div class="worker-card">
                    <div class="worker-header">
                        <span class="worker-name">${w.workerId}</span>
                        <span class="badge ${statusBadge}">${w.status}</span>
                    </div>
                    <div class="worker-host">${w.hostname}</div>
                    <div class="worker-stats">
                        <span>Load: <strong>${w.activeTaskCount} / ${w.maxCapacity}</strong></span>
                        <span>Processed: <strong>${w.totalProcessedCount}</strong></span>
                    </div>
                </div>
            `;
        }).join("");
    } catch (e) {
        console.error("Workers fetch failed", e);
    }
}

async function fetchTasks() {
    try {
        const res = await fetch("/api/v1/tasks");
        if (!res.ok) return;
        const tasks = await res.json();
        const tbody = document.getElementById("task-table-body");

        if (tasks.length === 0) {
            tbody.innerHTML = `<tr><td colspan="7" class="text-center text-muted">No tasks currently queued. Dispatch a task above!</td></tr>`;
            return;
        }

        tbody.innerHTML = tasks.slice(0, 15).map(t => {
            let priorityBadge = "badge-muted";
            if (t.priority === "CRITICAL") priorityBadge = "badge-danger";
            else if (t.priority === "HIGH") priorityBadge = "badge-warning";
            else if (t.priority === "NORMAL") priorityBadge = "badge-info";

            let statusBadge = "badge-muted";
            if (t.status === "COMPLETED") statusBadge = "badge-success";
            else if (t.status === "RUNNING") statusBadge = "badge-warning";
            else if (t.status === "DEAD_LETTERED" || t.status === "FAILED") statusBadge = "badge-danger";

            const dur = t.executionDurationMs ? `${t.executionDurationMs}ms` : "-";

            return `
                <tr>
                    <td><code>${t.taskId.substring(0, 8)}...</code></td>
                    <td><strong>${t.taskName}</strong></td>
                    <td><span class="badge ${priorityBadge}">${t.priority}</span></td>
                    <td><code>${t.assignedWorkerId || "unassigned"}</code></td>
                    <td>${t.retryCount} / ${t.maxRetries}</td>
                    <td>${dur}</td>
                    <td><span class="badge ${statusBadge}">${t.status}</span></td>
                </tr>
            `;
        }).join("");
    } catch (e) {
        console.error("Tasks fetch failed", e);
    }
}

async function fetchDLQ() {
    try {
        const res = await fetch("/api/v1/dlq");
        if (!res.ok) return;
        const dlqs = await res.json();
        const tbody = document.getElementById("dlq-table-body");

        if (dlqs.length === 0) {
            tbody.innerHTML = `<tr><td colspan="6" class="text-center text-muted">Dead Letter Queue is empty. System healthy.</td></tr>`;
            return;
        }

        tbody.innerHTML = dlqs.map(d => {
            const time = new Date(d.quarantinedAt).toLocaleTimeString();
            return `
                <tr>
                    <td><code>${d.taskId.substring(0, 8)}...</code></td>
                    <td>${d.taskName}</td>
                    <td class="text-danger">${d.failureReason}</td>
                    <td>${d.totalRetryAttempts}</td>
                    <td>${time}</td>
                    <td>
                        <button class="btn btn-sm btn-warning" onclick="replayDLQ('${d.taskId}')">Replay</button>
                    </td>
                </tr>
            `;
        }).join("");
    } catch (e) {
        console.error("DLQ fetch failed", e);
    }
}

async function promptSubmitTask() {
    const taskName = prompt("Enter Task Name:", "Cloud-Data-Index-Job");
    if (!taskName) return;

    const priority = prompt("Enter Priority (CRITICAL, HIGH, NORMAL, LOW):", "HIGH") || "NORMAL";
    const fail = confirm("Simulate task execution failure (to test exponential retry & DLQ routing)?");

    await fetch("/api/v1/tasks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            taskName: taskName,
            priority: priority.toUpperCase(),
            payload: JSON.stringify({ action: "SYNC", timestamp: Date.now() }),
            shouldSimulateFailure: fail,
            simulatedExecutionDurationMs: 400
        })
    });
    refreshAll();
}

async function floodTrafficForRateLimit() {
    alert("Simulating 30 rapid requests in 1 second. Limit is 20 req/10s. Watch the 429 rate limit counter spike!");
    for (let i = 1; i <= 30; i++) {
        fetch("/api/v1/tasks", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                taskName: `Burst-Request-#${i}`,
                priority: "NORMAL",
                submittedBy: "burst-client",
                simulatedExecutionDurationMs: 50
            })
        });
    }
    setTimeout(refreshAll, 1200);
}

async function killRandomWorker() {
    const res = await fetch("/api/v1/workers");
    const workers = await res.json();
    const healthy = workers.filter(w => w.status === "HEALTHY");
    if (healthy.length === 0) {
        alert("No healthy workers left to crash!");
        return;
    }
    const target = healthy[0];
    await fetch(`/api/v1/workers/${target.workerId}/crash`, { method: "POST" });
    alert(`Injected crash into ${target.workerId}! Heartbeat supervisor will detect and reclaim orphan tasks.`);
    refreshAll();
}

async function replayDLQ(taskId) {
    await fetch(`/api/v1/dlq/${taskId}/replay`, { method: "POST" });
    refreshAll();
}
