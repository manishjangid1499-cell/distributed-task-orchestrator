package com.nexus.orchestrator.service;

import com.nexus.orchestrator.model.WorkerNode;
import com.nexus.orchestrator.model.WorkerStatus;
import com.nexus.orchestrator.repository.WorkerRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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

    @PostConstruct
    public void seedInitialWorkers() {
        if (workerRepository.count() == 0) {
            List<WorkerNode> defaultWorkers = List.of(
                new WorkerNode("worker-us-east-1a", "node-cluster-01.internal", 10),
                new WorkerNode("worker-us-east-1b", "node-cluster-02.internal", 10),
                new WorkerNode("worker-eu-west-1a", "node-cluster-03.internal", 15),
                new WorkerNode("worker-ap-south-1a", "node-cluster-04.internal", 10)
            );
            workerRepository.saveAll(defaultWorkers);
            log.info("[WorkerService] Pre-seeded {} default worker nodes into the cluster.", defaultWorkers.size());
        }
    }

    public List<WorkerNode> listAllWorkers() {
        return workerRepository.findAll();
    }

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

    public boolean simulateWorkerCrash(String workerId) {
        Optional<WorkerNode> opt = workerRepository.findById(workerId);
        if (opt.isPresent()) {
            WorkerNode worker = opt.get();
            worker.setStatus(WorkerStatus.CRASHED);
            // Simulate last heartbeat 30 seconds ago to immediately trigger supervisor
            worker.setLastHeartbeat(Instant.now().minusSeconds(30));
            workerRepository.save(worker);
            log.warn("[ChaosEngineering] Simulated crash injected into Worker [{}]!", workerId);
            return true;
        }
        return false;
    }

    public boolean recoverWorker(String workerId) {
        Optional<WorkerNode> opt = workerRepository.findById(workerId);
        if (opt.isPresent()) {
            WorkerNode worker = opt.get();
            worker.setStatus(WorkerStatus.HEALTHY);
            worker.setLastHeartbeat(Instant.now());
            workerRepository.save(worker);
            log.info("[WorkerService] Worker [{}] brought back ONLINE.", workerId);
            return true;
        }
        return false;
    }
}
