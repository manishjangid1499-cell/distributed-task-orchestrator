package com.nexus.orchestrator.service;

import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.model.WorkerStatus;
import com.nexus.orchestrator.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
public class WorkerService {

    private static final Logger log = LoggerFactory.getLogger(WorkerService.class);
    private final WorkerRepository workerRepository;

    public WorkerService(WorkerRepository workerRepository) {
        this.workerRepository = workerRepository;
    }

    public List<WorkerNode> listAllWorkers() {
        return workerRepository.findAll();
    }

    @Transactional
    public boolean recordHeartbeat(String workerId) {
        Optional<WorkerNode> opt = workerRepository.findById(workerId);
        if (opt.isPresent()) {
            WorkerNode worker = opt.get();
            worker.recordHeartbeat();
            workerRepository.save(worker);
            return true;
        }
        return false;
    }

    @Transactional
    public boolean simulateWorkerCrash(String workerId) {
        Optional<WorkerNode> opt = workerRepository.findById(workerId);
        if (opt.isPresent()) {
            WorkerNode worker = opt.get();
            worker.setStatus(WorkerStatus.CRASHED);
            // Simulate last heartbeat 30 seconds ago to immediately trigger supervisor
            worker.setLastHeartbeatAt(Instant.now().minusSeconds(30));
            workerRepository.save(worker);
            log.warn("[WorkerService] Worker [{}] status updated to CRASHED.", workerId);
            return true;
        }
        return false;
    }

    @Transactional
    public boolean recoverWorker(String workerId) {
        Optional<WorkerNode> opt = workerRepository.findById(workerId);
        if (opt.isPresent()) {
            WorkerNode worker = opt.get();
            worker.setStatus(WorkerStatus.HEALTHY);
            worker.setLastHeartbeatAt(Instant.now());
            workerRepository.save(worker);
            log.info("[WorkerService] Worker [{}] status updated to HEALTHY.", workerId);
            return true;
        }
        return false;
    }
}
