package dev.eventflow.worker;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "eventflow")
public class DeliveryProperties {
    @Min(1)
    @Max(128)
    private int localConcurrency = 1;
    @NotNull
    private Duration shutdownGrace = Duration.ofSeconds(5);
    @NotNull
    private Duration httpRequestTimeout = Duration.ofSeconds(5);
    @Min(1)
    private int leaseSeconds = 15;
    @Min(1)
    private int pendingIdleSeconds = 20;

    public int getLocalConcurrency() { return localConcurrency; }
    public void setLocalConcurrency(int localConcurrency) { this.localConcurrency = localConcurrency; }
    public Duration getShutdownGrace() { return shutdownGrace; }
    public void setShutdownGrace(Duration shutdownGrace) { this.shutdownGrace = shutdownGrace; }
    public Duration getHttpRequestTimeout() { return httpRequestTimeout; }
    public void setHttpRequestTimeout(Duration httpRequestTimeout) { this.httpRequestTimeout = httpRequestTimeout; }
    public int getLeaseSeconds() { return leaseSeconds; }
    public void setLeaseSeconds(int leaseSeconds) { this.leaseSeconds = leaseSeconds; }
    public int getPendingIdleSeconds() { return pendingIdleSeconds; }
    public void setPendingIdleSeconds(int pendingIdleSeconds) { this.pendingIdleSeconds = pendingIdleSeconds; }

    @AssertTrue(message = "http request timeout must be shorter than the job lease, and pending idle must exceed the lease")
    public boolean isTimingSafe() {
        return httpRequestTimeout != null
                && shutdownGrace != null
                && !httpRequestTimeout.isNegative()
                && !httpRequestTimeout.isZero()
                && !shutdownGrace.isNegative()
                && !shutdownGrace.isZero()
                && httpRequestTimeout.compareTo(Duration.ofSeconds(leaseSeconds)) < 0
                && pendingIdleSeconds > leaseSeconds;
    }
}
