package com.nexus.orchestrator.repository;

import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.model.WorkerStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface WorkerRepository extends JpaRepository<WorkerNode, String> {

    List<WorkerStatus> findByStatus(WorkerStatus status);

    List<WorkerNode> findByLastHeartbeatBeforeAndStatusNot(Instant threshold, WorkerStatus excludedStatus);

    long countByStatus(WorkerStatus status);
}
