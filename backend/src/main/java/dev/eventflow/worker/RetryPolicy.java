package dev.eventflow.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class RetryPolicy {
    private final long baseSeconds;
    private final long maxSeconds;

    public RetryPolicy(@Value("${eventflow.retry-base-seconds:5}") long baseSeconds,
                       @Value("${eventflow.retry-max-seconds:300}") long maxSeconds) {
        if (baseSeconds < 1 || maxSeconds < baseSeconds) throw new IllegalArgumentException("Invalid retry delay");
        this.baseSeconds = baseSeconds;
        this.maxSeconds = maxSeconds;
    }

    public Outcome classify(int status) {
        if (status >= 200 && status < 300) return Outcome.SUCCESS;
        if (status == 0 || status == 408 || status == 429 || status >= 500) return Outcome.TRANSIENT_FAILURE;
        return Outcome.PERMANENT_FAILURE;
    }

    public boolean shouldRetry(Outcome outcome, int cycleAttempts, int maxAttempts) {
        return outcome == Outcome.TRANSIENT_FAILURE && cycleAttempts < maxAttempts;
    }

    public Duration delay(int cycleAttempts) {
        long factor = 1L << Math.min(Math.max(cycleAttempts - 1, 0), 30);
        return Duration.ofSeconds(Math.min(maxSeconds, baseSeconds * factor));
    }

    public enum Outcome { SUCCESS, TRANSIENT_FAILURE, PERMANENT_FAILURE }
}
