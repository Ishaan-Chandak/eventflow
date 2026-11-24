package dev.eventflow.worker;

import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class DeliveryPropertiesTest {
    @Test
    void defaultsPreserveSingleDeliveryAndSafeTiming() {
        var properties = new DeliveryProperties();

        assertThat(properties.getLocalConcurrency()).isEqualTo(1);
        assertThat(Validation.buildDefaultValidatorFactory().getValidator().validate(properties)).isEmpty();
    }

    @Test
    void rejectsTimeoutAtLeaseAndPendingIdleAtLease() {
        var properties = new DeliveryProperties();
        properties.setHttpRequestTimeout(Duration.ofSeconds(15));
        properties.setPendingIdleSeconds(15);

        assertThat(Validation.buildDefaultValidatorFactory().getValidator().validate(properties))
                .extracting(violation -> violation.getPropertyPath().toString())
                .contains("timingSafe");
    }

    @Test
    void rejectsUnboundedConcurrencyAndZeroShutdownGrace() {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(129);
        properties.setShutdownGrace(Duration.ZERO);

        assertThat(Validation.buildDefaultValidatorFactory().getValidator().validate(properties))
                .extracting(violation -> violation.getPropertyPath().toString())
                .contains("localConcurrency","timingSafe");
    }
}
