package com.nexus.orchestrator.repository;

import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.model.WorkerStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Repository
public interface WorkerRepository extends JpaRepository<WorkerNode, String> {

    List<WorkerNode> findByStatus(WorkerStatus status);

    List<WorkerNode> findByLastHeartbeatAtBeforeAndStatusNot(Instant threshold, WorkerStatus excludedStatus);

    long countByStatus(WorkerStatus status);

    @Transactional
    @Modifying
    @Query("UPDATE WorkerNode w SET w.lastHeartbeatAt = :now, w.activeJobs = :activeJobs WHERE w.workerId = :workerId")
    int updateHeartbeat(@Param("workerId") String workerId, @Param("activeJobs") int activeJobs, @Param("now") Instant now);
}
