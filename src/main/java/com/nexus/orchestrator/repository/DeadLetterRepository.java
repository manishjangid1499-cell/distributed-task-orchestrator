package com.nexus.orchestrator.repository;

import com.nexus.orchestrator.model.DeadLetterRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DeadLetterRepository extends JpaRepository<DeadLetterRecord, Long> {

    Optional<DeadLetterRecord> findByTaskId(String taskId);

    List<DeadLetterRecord> findByReplayedFalseOrderByQuarantinedAtDesc();

    long countByReplayedFalse();
}
