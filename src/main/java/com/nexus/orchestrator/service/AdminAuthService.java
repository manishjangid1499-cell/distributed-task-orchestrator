package com.nexus.orchestrator.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class AdminAuthService {

    private final String configuredApiKey;

    public AdminAuthService(@Value("${nexus.api.key:}") String configuredApiKey) {
        this.configuredApiKey = configuredApiKey != null ? configuredApiKey.trim() : "";
    }

    public boolean isAuthorized(String providedKey) {
        if (configuredApiKey.isEmpty()) {
            return true; // No key configured, allow in development
        }
        if (providedKey == null) {
            return false;
        }
        return configuredApiKey.equals(providedKey.trim());
    }
}
