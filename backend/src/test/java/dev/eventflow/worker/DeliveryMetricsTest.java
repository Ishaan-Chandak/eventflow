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
    void admissionMetricsUseOnlyBoundedResultLabels() {
        var registry = new SimpleMeterRegistry();
        var metrics = new DeliveryMetrics(registry);

        metrics.recordAdmission(Duration.ofMillis(5), DeliveryMetrics.AdmissionResult.ACQUIRED);
        metrics.recordAdmission(Duration.ofMillis(7), DeliveryMetrics.AdmissionResult.ENDPOINT_SATURATED);
        metrics.recordDeferral(DeliveryMetrics.AdmissionResult.REDIS_UNAVAILABLE);
        metrics.recordPermitRecovery(2, 3);
        metrics.recordRenewalFailure(DeliveryMetrics.RenewalFailure.OWNERSHIP_LOST);
        metrics.recordUtilization(1, 2, 1, 4);

        assertThat(registry.getMeters()).filteredOn(meter -> meter.getId().getName().startsWith("eventflow.delivery.admission"))
                .allSatisfy(meter -> assertThat(meter.getId().getTags())
                        .allSatisfy(tag -> assertThat(tag.getKey()).isIn("result", "reason", "scope")));
        assertThat(registry.get("eventflow.delivery.admission.deferrals").tag("reason", "redis_unavailable").counter().count()).isEqualTo(1);
        assertThat(registry.get("eventflow.delivery.admission.recovered").tag("scope", "global").counter().count()).isEqualTo(2);
        assertThat(registry.get("eventflow.delivery.admission.recovered").tag("scope", "endpoint").counter().count()).isEqualTo(3);
        assertThat(registry.get("eventflow.delivery.admission.global.utilization").gauge().value()).isEqualTo(0.5);
        assertThat(registry.get("eventflow.delivery.admission.endpoint.utilization").summary().max()).isEqualTo(0.25);
    }
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
