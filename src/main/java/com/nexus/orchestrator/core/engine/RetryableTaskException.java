package com.nexus.orchestrator.core.engine;

public class RetryableTaskException extends RuntimeException {
    public RetryableTaskException(String message) {
        super(message);
    }

    public RetryableTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
