package dev.eventflow.worker;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DeliveryMetricsTest {
    @Test
    void recordsOnlyLowCardinalityResultTypes() {
        var registry = new SimpleMeterRegistry();
        var metrics = new DeliveryMetrics(registry);

        metrics.recordHttp(Duration.ofMillis(25), DeliveryMetrics.HttpResult.SUCCESS);
        metrics.recordHttp(Duration.ofMillis(30), DeliveryMetrics.HttpResult.TIMEOUT);
        metrics.recordDurableOutcome(DeliveryMetrics.DurableOutcome.SUCCEEDED);

        assertThat(registry.find("eventflow.delivery.http.duration").timers())
                .extracting(timer -> timer.getId().getTag("result"))
                .containsExactlyInAnyOrder("success", "timeout");
        assertThat(registry.get("eventflow.delivery.outcomes").tag("outcome", "succeeded").counter().count())
                .isEqualTo(1);
    }

    @Test
    void registryFailureNeverEscapesInstrumentation() {
        MeterRegistry registry = mock(MeterRegistry.class);
        when(registry.config()).thenThrow(new IllegalStateException("metrics unavailable"));
        var metrics = new DeliveryMetrics(registry);

        assertThatCode(() -> metrics.recordHttp(Duration.ofMillis(1), DeliveryMetrics.HttpResult.NETWORK_ERROR))
                .doesNotThrowAnyException();
        assertThatCode(() -> metrics.recordDurableOutcome(DeliveryMetrics.DurableOutcome.UNFINALIZED))
                .doesNotThrowAnyException();
    }
}
