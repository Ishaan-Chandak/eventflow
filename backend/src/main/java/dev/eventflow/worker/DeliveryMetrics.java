package dev.eventflow.worker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class DeliveryMetrics {
    private final MeterRegistry registry;
    private final AtomicReference<Double> globalUtilization = new AtomicReference<>(0.0);

    public DeliveryMetrics(MeterRegistry registry) {
        this.registry = registry;
        try { registry.gauge("eventflow.delivery.admission.global.utilization", globalUtilization, AtomicReference::get); }
        catch (RuntimeException ignored) { }
    }

    public void recordHttp(Duration duration, HttpResult result) {
        try {
            Timer.builder("eventflow.delivery.http.duration")
                    .tag("result", label(result))
                    .register(registry)
                    .record(duration);
        } catch (RuntimeException ignored) { }
    }

    public void recordDurableOutcome(DurableOutcome outcome) {
        try {
            Counter.builder("eventflow.delivery.outcomes")
                    .tag("outcome", label(outcome))
                    .register(registry)
                    .increment();
        } catch (RuntimeException ignored) { }
    }

    public void recordAdmission(Duration duration, AdmissionResult result) {
        try {
            Timer.builder("eventflow.delivery.admission.acquire.duration")
                    .tag("result", label(result)).register(registry).record(duration);
        } catch (RuntimeException ignored) { }
        if (result != AdmissionResult.ACQUIRED)
            increment("eventflow.delivery.admission.rejections", "reason", label(result), 1);
    }

    public void recordUtilization(long globalUsed, long globalLimit, long endpointUsed, long endpointLimit) {
        if (globalLimit <= 0 || endpointLimit <= 0) return;
        globalUtilization.set(Math.min(1.0, (double) globalUsed / globalLimit));
        try {
            io.micrometer.core.instrument.DistributionSummary.builder("eventflow.delivery.admission.endpoint.utilization")
                    .register(registry).record(Math.min(1.0, (double) endpointUsed / endpointLimit));
        } catch (RuntimeException ignored) { }
    }

    public void recordDeferral(AdmissionResult reason) {
        increment("eventflow.delivery.admission.deferrals", "reason", label(reason), 1);
    }

    public void recordPermitRecovery(long global, long endpoint) {
        increment("eventflow.delivery.admission.recovered", "scope", "global", global);
        increment("eventflow.delivery.admission.recovered", "scope", "endpoint", endpoint);
    }

    public void recordRenewalFailure(RenewalFailure reason) {
        increment("eventflow.delivery.admission.renewal.failures", "reason", label(reason), 1);
    }

    public void bindActivePermits(AtomicInteger active) {
        try { registry.gauge("eventflow.delivery.admission.permits.active", active, AtomicInteger::get); }
        catch (RuntimeException ignored) { }
    }

    private void increment(String name, String tag, String value, long amount) {
        if (amount <= 0) return;
        try { Counter.builder(name).tag(tag, value).register(registry).increment(amount); }
        catch (RuntimeException ignored) { }
    }

    private static String label(Enum<?> value) { return value.name().toLowerCase(Locale.ROOT); }

    public enum HttpResult { SUCCESS, TRANSIENT_FAILURE, PERMANENT_FAILURE, TIMEOUT, NETWORK_ERROR }
    public enum DurableOutcome { SUCCEEDED, RETRY_SCHEDULED, DEAD, UNFINALIZED }
    public enum AdmissionResult { ACQUIRED, GLOBAL_SATURATED, ENDPOINT_SATURATED, REDIS_UNAVAILABLE, OWNERSHIP_LOST, EXECUTOR_REJECTED }
    public enum RenewalFailure { REDIS_UNAVAILABLE, OWNERSHIP_LOST }
}
