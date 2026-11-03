package com.nexus.orchestrator.unit;

import com.nexus.orchestrator.model.Task;
import com.nexus.orchestrator.model.TaskPriority;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PriorityAndFifoOrderingTest {

    @Test
    @DisplayName("Priority comparator: CRITICAL (0) sorts before HIGH (1), NORMAL (2), and LOW (3)")
    public void testPriorityOrdering() {
        Task low = new Task("low-task", "{}", TaskPriority.LOW, "client-1", 100);
        Task normal = new Task("normal-task", "{}", TaskPriority.NORMAL, "client-1", 100);
        Task high = new Task("high-task", "{}", TaskPriority.HIGH, "client-1", 100);
        Task critical = new Task("crit-task", "{}", TaskPriority.CRITICAL, "client-1", 100);

        List<Task> list = new ArrayList<>(List.of(low, normal, high, critical));
        Collections.sort(list);

        assertEquals(TaskPriority.CRITICAL, list.get(0).getPriority());
        assertEquals(TaskPriority.HIGH, list.get(1).getPriority());
        assertEquals(TaskPriority.NORMAL, list.get(2).getPriority());
        assertEquals(TaskPriority.LOW, list.get(3).getPriority());
    }

    @Test
    @DisplayName("FIFO tie-breaking: Same priority tasks preserve submission order based on createdAt")
    public void testFifoTieBreakingWithinSamePriority() {
        Instant base = Instant.parse("2026-01-01T12:00:00Z");

        Task t1 = new Task("t1", "{}", TaskPriority.HIGH, "client-1", 100);
        t1.setCreatedAt(base.plusMillis(10));

        Task t2 = new Task("t2", "{}", TaskPriority.HIGH, "client-1", 100);
        t2.setCreatedAt(base.plusMillis(20));

        Task t3 = new Task("t3", "{}", TaskPriority.HIGH, "client-1", 100);
        t3.setCreatedAt(base.plusMillis(30));

        List<Task> list = new ArrayList<>(List.of(t3, t1, t2));
        Collections.sort(list);

        assertEquals("t1", list.get(0).getTaskName());
        assertEquals("t2", list.get(1).getTaskName());
        assertEquals("t3", list.get(2).getTaskName());
    }
}
