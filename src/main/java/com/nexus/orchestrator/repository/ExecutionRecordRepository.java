package com.nexus.orchestrator.repository;

import com.nexus.orchestrator.model.ExecutionRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ExecutionRecordRepository extends JpaRepository<ExecutionRecord, UUID> {

    Optional<ExecutionRecord> findByIdempotencyKey(String idempotencyKey);

    boolean existsByIdempotencyKey(String idempotencyKey);
}
