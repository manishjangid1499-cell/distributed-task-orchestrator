package com.nexus.orchestrator.model;

import com.fasterxml.jackson.databind.node.TextNode;

import java.util.UUID;

/**
 * Backward-compatible wrapper around DeadLetterTask.
 */
public class DeadLetterRecord extends DeadLetterTask {

    public DeadLetterRecord() {
        super();
    }

    public DeadLetterRecord(String taskId, String taskName, String payload, String reason, int attempts) {
        super(
            safeParseUuid(taskId),
            taskName != null ? taskName : "NO_OP",
            taskName,
            new TextNode(payload != null ? payload : ""),
            (short) 2,
            attempts,
            reason
        );
    }

    private static UUID safeParseUuid(String val) {
        if (val == null) return UUID.randomUUID();
        try {
            return UUID.fromString(val);
        } catch (IllegalArgumentException e) {
            return UUID.nameUUIDFromBytes(val.getBytes());
        }
    }
}
