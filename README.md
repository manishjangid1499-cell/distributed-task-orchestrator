# Nexus: High-Throughput Distributed Task Orchestrator & Rate Limiter

<div align="center">

[![Java Version](https://img.shields.io/badge/Java-21%20LTS-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.5-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/Tests-7%20Passed-10B981?style=for-the-badge&logo=junit5&logoColor=white)]()
[![License](https://img.shields.io/badge/License-MIT-F59E0B?style=for-the-badge)]()

<br/>

**High-throughput distributed task scheduling and rate limiting platform built on Java 21, Spring Boot, and `java.util.concurrent`.**  
Engineered with preemptive priority queuing, sliding-window rate limiting, heartbeat supervisor crash recovery, and automated dead-letter queue (DLQ) retry routing.

</div>

---

## 📸 Application Dashboard

Real-time cluster overview displaying live worker node topology, preemptive task execution states, sliding window rate-limiting telemetry, and quarantined dead-letter records:

![Dashboard Overview](docs/screenshots/dashboard_overview.png)

---

## 🏛️ System Architecture

Nexus is organized into four core layers: an API Control Plane, a Concurrency Scheduling Engine, a Cluster Supervisor with Fault Tolerance, and an In-Memory / Persistent Metadata Store.

```mermaid
flowchart TD
    subgraph Clients ["Client & External Ingestion"]
        REST["REST API Clients<br/>(/api/v1/tasks)"]
        Dashboard["Web Operations Dashboard<br/>(HTML5 / JetBrains Mono)"]
    end

    subgraph CoreEngine ["Nexus Orchestration Core"]
        Limiter["Sliding Window Rate Limiter<br/>Atomic Deque / ConcurrentHashMap"]
        Queue["Preemptive Priority Blocking Queue<br/>CRITICAL > HIGH > NORMAL > LOW"]
        ThreadPool["Thread Pool Executor<br/>Custom Bounded Workers"]
        DLQ["Dead Letter Queue (DLQ)<br/>Exponential Backoff Retrier"]
    end

    subgraph ClusterTopology ["Distributed Worker Mesh"]
        Worker1["Worker Node 1<br/>(US-East)"]
        Worker2["Worker Node 2<br/>(EU-West)"]
        Supervisor["Heartbeat Supervisor Daemon<br/>(Detects Crashes / Reclaims Orphans)"]
    end

    subgraph Storage ["Persistence Layer"]
        DB[(H2 Database / PostgreSQL<br/>JPA Hibernate)]
    end

    REST --> Limiter
    Dashboard --> Limiter
    Limiter --> Queue
    Queue --> ThreadPool
    ThreadPool --> Worker1
    ThreadPool --> Worker2
    Supervisor --> Worker1
    Supervisor --> Worker2
    ThreadPool -. Failure Exhausted .-> DLQ
    Supervisor -. Reclaim Orphan Tasks .-> Queue
    ThreadPool --> DB
    Supervisor --> DB
    DLQ --> DB
```

---

## 🔄 Preemptive Scheduling & Failure Recovery Flow

When tasks are submitted, they are ordered by priority level before worker thread assignment. Failed tasks transition through exponential retry cycles before quarantine:

```mermaid
sequenceDiagram
    autonumber
    participant Client as API Client
    participant Limiter as Sliding Window Limiter
    participant Queue as Priority Blocking Queue
    participant Engine as Thread Pool Worker
    participant DLQ as Dead Letter Queue
    participant DB as Persistence Layer

    Client->>Limiter: POST /api/v1/tasks (Priority: CRITICAL)
    alt Rate Quota Breached
        Limiter-->>Client: HTTP 429 Too Many Requests (Retry-After: X ms)
    else Quota Allowed
        Limiter->>Queue: Enqueue Task (CRITICAL level 0)
        Limiter-->>Client: HTTP 202 Accepted (Task ID)
        Queue->>Engine: Dequeue highest priority task
        Engine->>DB: Status: RUNNING
        alt Task Succeeds
            Engine->>DB: Status: COMPLETED (Duration: X ms)
        else Task Fails
            Engine->>Engine: Increment Retry Count (Attempt N/3)
            alt Retries Remaining
                Engine->>Queue: Re-enqueue with Exponential Backoff (2^attempt * 500ms)
            else Max Retries Exceeded
                Engine->>DLQ: Route to Dead Letter Queue
                Engine->>DB: Status: DEAD_LETTERED
            end
        end
    end
```

---

## 🔄 Worker Heartbeat & Crash Recovery State Machine

Workers send periodic heartbeats to maintain active status. If a worker fails, the supervisor reclaims running tasks to guarantee at-least-once execution:

```mermaid
stateDiagram-v2
    [*] --> HEALTHY: Worker Registered
    HEALTHY --> BUSY: Task Capacity Saturated
    BUSY --> HEALTHY: Tasks Completed
    HEALTHY --> CRASHED: Heartbeat Timeout Exceeded (>15s)
    BUSY --> CRASHED: Heartbeat Timeout Exceeded (>15s)
    
    state CRASHED {
        Reclaim_Orphans: Supervisor detects missed heartbeat
        Re-enqueue: Orphan tasks re-routed to healthy workers
        Reclaim_Orphans --> Re-enqueue
    }

    CRASHED --> HEALTHY: Worker Restored & Sends Heartbeat
    CRASHED --> OFFLINE: Operator Decommissions Node
    OFFLINE --> [*]
```

---

## 🔬 Core Algorithms & Mathematical Foundations

### 1. Sliding Window Rate Limiter
Standard fixed-window counters allow $2\times$ traffic spikes at window boundaries. Nexus implements an atomic **Sliding Window Log** counter per client:
$$\text{Window} = [t_{\text{now}} - \Delta t, t_{\text{now}}]$$
* Evicts expired timestamps in $O(\log n)$ using an atomic double-ended queue (`ArrayDeque<Long>`).
* Uses per-client fine-grained `ReentrantLock` instances inside a `ConcurrentHashMap` to eliminate global lock contention across tenants.
* If $\text{Size}(\text{Window}) + \text{Tokens} > \text{Limit}$, returns:
  $$\text{Retry-After} = (t_{\text{oldest}} + \Delta t) - t_{\text{now}}$$

### 2. Preemptive Priority Queue Scheduling
Tasks implement `Comparable<Task>` based on a two-tier comparator:
```java
int priorityCompare = Integer.compare(this.priority.getLevel(), other.priority.getLevel());
if (priorityCompare != 0) return priorityCompare;
return this.createdAt.compareTo(other.createdAt); // FIFO tie-breaker
```
Tasks marked **`CRITICAL (0)`** immediately jump ahead of `HIGH (1)`, `NORMAL (2)`, and `LOW (3)` tasks awaiting execution in the bounded queue.

### 3. Exponential Backoff with DLQ Quarantine
When tasks experience transient external failures, retry intervals scale exponentially to protect downstream dependencies:
$$\text{Delay}(n) = 2^n \times 500\text{ ms}$$
Tasks exceeding $N_{\max} = 3$ retries are quarantined in the **Dead Letter Queue (DLQ)** along with the full failure stacktrace for manual inspection or replay.

---

## ⚡ Quickstart Guide

### Prerequisites
* **Java 21 LTS**
* **Maven 3.9+** (or use Docker)

### Run Locally

```bash
# Clone the repository
git clone https://github.com/yourusername/nexus-task-orchestrator.git
cd nexus-task-orchestrator

# Run automated test suite (7 tests)
mvn test

# Package and run the executable JAR
mvn package -DskipTests
java -jar target/distributed-task-orchestrator-1.0.0.jar
```
Navigate to **`http://localhost:8080`** in your browser to access the control plane dashboard.

---

### Run with Docker Compose

```bash
docker compose up -d --build
```

---

## 🌐 REST API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Web Operations Dashboard |
| `POST` | `/api/v1/tasks` | Submits task with priority and rate limiting |
| `GET` | `/api/v1/tasks` | Lists recent tasks with status & execution latencies |
| `GET` | `/api/v1/tasks/{id}` | Fetches task details and retry history |
| `POST` | `/api/v1/tasks/{id}/cancel` | Cancels queued task before execution |
| `POST` | `/api/v1/tasks/{id}/retry` | Manually re-queues a failed/dead-lettered task |
| `GET` | `/api/v1/workers` | Lists worker cluster nodes and load metrics |
| `POST` | `/api/v1/workers/{id}/heartbeat`| Records worker heartbeat |
| `POST` | `/api/v1/workers/{id}/crash` | Injects simulated worker crash (Chaos testing) |
| `POST` | `/api/v1/workers/{id}/recover` | Recovers crashed worker to HEALTHY state |
| `GET` | `/api/v1/dlq` | Lists quarantined dead-letter tasks |
| `POST` | `/api/v1/dlq/{id}/replay` | Replays dead-lettered task into the scheduler |
| `GET` | `/api/v1/metrics/summary` | Cluster throughput, latency & queue metrics |

---

## 🧪 Automated Test Suite

Verified via JUnit 5, Mockito, and Spring Boot MockMvc:

* `SlidingWindowRateLimiterTest`: Validates window bounds, quota exhaustion, and thread safety under 20 concurrent threads.
* `TaskExecutionEngineTest`: Validates priority dispatch, asynchronous execution, and DLQ routing.
* `WorkerHeartbeatSupervisorTest`: Validates missed heartbeat detection and orphan task reclamation.
* `TaskControllerIntegrationTest`: End-to-end HTTP integration test for submission, retrieval, and 429 response validation.

---

## 📄 License

Distributed under the **MIT License**. See `LICENSE` for details.
