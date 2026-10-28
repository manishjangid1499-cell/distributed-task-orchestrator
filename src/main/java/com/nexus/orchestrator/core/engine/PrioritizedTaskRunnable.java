package com.nexus.orchestrator.core.engine;

import com.nexus.orchestrator.model.Task;

/**
 * Comparable Runnable wrapper to enforce priority queuing inside standard ThreadPoolExecutor.
 */
public class PrioritizedTaskRunnable implements Runnable, Comparable<PrioritizedTaskRunnable> {

    private final Task task;
    private final Runnable targetRunnable;

    public PrioritizedTaskRunnable(Task task, Runnable targetRunnable) {
        this.task = task;
        this.targetRunnable = targetRunnable;
    }

    public Task getTask() {
        return task;
    }

    @Override
    public void run() {
        targetRunnable.run();
    }

    @Override
    public int compareTo(PrioritizedTaskRunnable other) {
        if (other == null) return -1;
        return this.task.compareTo(other.getTask());
    }
}
