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
    @Min(1)
    @Max(10000)
    private int globalConcurrency = 1;
    @Min(1)
    @Max(10000)
    private int endpointConcurrency = 1;
    @NotNull
    private Duration permitLease = Duration.ofSeconds(30);
    @NotNull
    private Duration permitRenewal = Duration.ofSeconds(10);
    @NotNull
    private Duration capacityDeferral = Duration.ofSeconds(20);
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
    public int getGlobalConcurrency() { return globalConcurrency; }
    public void setGlobalConcurrency(int globalConcurrency) { this.globalConcurrency = globalConcurrency; }
    public int getEndpointConcurrency() { return endpointConcurrency; }
    public void setEndpointConcurrency(int endpointConcurrency) { this.endpointConcurrency = endpointConcurrency; }
    public Duration getPermitLease() { return permitLease; }
    public void setPermitLease(Duration permitLease) { this.permitLease = permitLease; }
    public Duration getPermitRenewal() { return permitRenewal; }
    public void setPermitRenewal(Duration permitRenewal) { this.permitRenewal = permitRenewal; }
    public Duration getCapacityDeferral() { return capacityDeferral; }
    public void setCapacityDeferral(Duration capacityDeferral) { this.capacityDeferral = capacityDeferral; }

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

    @AssertTrue(message = "permit renewal must be positive and no greater than one third of the permit lease")
    public boolean isDistributedTimingSafe() {
        return permitLease != null && permitRenewal != null && capacityDeferral != null
                && !permitLease.isNegative() && !permitLease.isZero()
                && !permitRenewal.isNegative() && !permitRenewal.isZero()
                && permitLease.toMillis() > 0 && permitRenewal.toMillis() > 0
                && capacityDeferral.toMillis() > 0
                && capacityDeferral.compareTo(Duration.ofSeconds(pendingIdleSeconds)) >= 0
                && permitRenewal.multipliedBy(3).compareTo(permitLease) <= 0;
    }
}
