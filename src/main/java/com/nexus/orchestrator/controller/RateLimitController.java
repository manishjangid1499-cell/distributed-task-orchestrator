package com.nexus.orchestrator.controller;

import com.nexus.orchestrator.core.ratelimit.RateLimiter;
import com.nexus.orchestrator.model.dto.RateLimitCheckRequest;
import com.nexus.orchestrator.model.dto.RateLimitResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/rate-limit")
@CrossOrigin(origins = "*")
public class RateLimitController {

    private final RateLimiter rateLimiter;

    public RateLimitController(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @PostMapping("/verify")
    public ResponseEntity<RateLimitResponse> verifyRateLimit(@RequestBody RateLimitCheckRequest request) {
        RateLimiter.Result res = rateLimiter.tryAcquire(request.getClientId(), request.getTokensRequested());
        RateLimitResponse response = new RateLimitResponse(
            request.getClientId(),
            res.allowed(),
            res.remainingQuota(),
            res.limit(),
            res.windowSeconds(),
            res.retryAfterMs()
        );
        return ResponseEntity.ok(response);
    }
}
