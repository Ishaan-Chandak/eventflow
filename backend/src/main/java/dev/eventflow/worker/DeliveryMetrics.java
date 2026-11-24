package dev.eventflow.worker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Locale;

@Component
public class DeliveryMetrics {
    private final MeterRegistry registry;

    public DeliveryMetrics(MeterRegistry registry) { this.registry = registry; }

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

    private static String label(Enum<?> value) { return value.name().toLowerCase(Locale.ROOT); }

    public enum HttpResult { SUCCESS, TRANSIENT_FAILURE, PERMANENT_FAILURE, TIMEOUT, NETWORK_ERROR }
    public enum DurableOutcome { SUCCEEDED, RETRY_SCHEDULED, DEAD, UNFINALIZED }
}
