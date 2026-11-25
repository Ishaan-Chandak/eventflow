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
        assertThat(properties.getGlobalConcurrency()).isEqualTo(1);
        assertThat(properties.getEndpointConcurrency()).isEqualTo(1);
        assertThat(properties.getPermitLease()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.getPermitRenewal()).isEqualTo(Duration.ofSeconds(10));
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

    @Test
    void rejectsRenewalThatCannotRunSafelyBeforePermitExpiry() {
        var properties = new DeliveryProperties();
        properties.setPermitLease(Duration.ofSeconds(30));
        properties.setPermitRenewal(Duration.ofSeconds(11));

        assertThat(Validation.buildDefaultValidatorFactory().getValidator().validate(properties))
                .extracting(violation -> violation.getPropertyPath().toString())
                .contains("distributedTimingSafe");
    }

    @Test
    void rejectsNonPositiveDistributedLimits() {
        var properties = new DeliveryProperties();
        properties.setGlobalConcurrency(0);
        properties.setEndpointConcurrency(0);

        assertThat(Validation.buildDefaultValidatorFactory().getValidator().validate(properties))
                .extracting(violation -> violation.getPropertyPath().toString())
                .contains("globalConcurrency", "endpointConcurrency");
    }

    @Test
    void rejectsSubMillisecondPermitDurations() {
        var properties = new DeliveryProperties();
        properties.setPermitLease(Duration.ofNanos(900_000));
        properties.setPermitRenewal(Duration.ofNanos(300_000));

        assertThat(Validation.buildDefaultValidatorFactory().getValidator().validate(properties))
                .extracting(violation -> violation.getPropertyPath().toString())
                .contains("distributedTimingSafe");
    }
}
