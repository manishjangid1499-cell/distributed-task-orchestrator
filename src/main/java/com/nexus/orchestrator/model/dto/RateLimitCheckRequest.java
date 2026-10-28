package com.nexus.orchestrator.model.dto;

public class RateLimitCheckRequest {
    private String clientId = "default-client";
    private int tokensRequested = 1;

    public RateLimitCheckRequest() {}
    public RateLimitCheckRequest(String clientId, int tokensRequested) {
        this.clientId = clientId;
        this.tokensRequested = tokensRequested;
    }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public int getTokensRequested() { return tokensRequested; }
    public void setTokensRequested(int tokensRequested) { this.tokensRequested = tokensRequested; }
}
