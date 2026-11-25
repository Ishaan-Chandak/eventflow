package dev.eventflow.worker;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DistributedAdmissionRedisIntegrationTest {
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4.2-alpine").withExposedPorts(6379);

    private LettuceConnectionFactory connectionFactory;
    private DistributedAdmissionController first;
    private DistributedAdmissionController second;
    private StringRedisTemplate template;

    @AfterEach
    void closeResources() {
        if (first != null) first.stop();
        if (second != null) second.stop();
        if (connectionFactory != null) connectionFactory.destroy();
    }

    @Test
    void independentReplicasAtomicallyEnforceGlobalLimit() {
        controllers(1, 1, Duration.ofSeconds(30), Duration.ofSeconds(10));
        var endpoint = UUID.randomUUID();

        var acquired = first.tryAcquire(endpoint);
        var rejected = second.tryAcquire(endpoint);

        assertThat(acquired.outcome()).isEqualTo(DistributedAdmissionController.Outcome.ACQUIRED);
        assertThat(rejected.outcome()).isEqualTo(DistributedAdmissionController.Outcome.GLOBAL_SATURATED);
        acquired.permit().close();
        assertThat(second.tryAcquire(endpoint).outcome()).isEqualTo(DistributedAdmissionController.Outcome.ACQUIRED);
    }

    @Test
    void endpointLimitDoesNotBlockIndependentEndpoint() {
        controllers(2, 1, Duration.ofSeconds(30), Duration.ofSeconds(10));
        var saturated = UUID.randomUUID();
        var independent = UUID.randomUUID();

        var held = first.tryAcquire(saturated);

        assertThat(second.tryAcquire(saturated).outcome()).isEqualTo(DistributedAdmissionController.Outcome.ENDPOINT_SATURATED);
        assertThat(second.tryAcquire(independent).outcome()).isEqualTo(DistributedAdmissionController.Outcome.ACQUIRED);
        held.permit().close();
    }

    @Test
    void duplicateAndStaleReleaseCannotReleaseNewPermit() throws Exception {
        controllers(1, 1, Duration.ofMillis(300), Duration.ofMillis(100));
        var endpoint = UUID.randomUUID();
        var stale = first.tryAcquire(endpoint).permit();
        first.stop();
        Thread.sleep(400);
        var newer = second.tryAcquire(endpoint);

        stale.close();
        stale.close();

        assertThat(newer.outcome()).isEqualTo(DistributedAdmissionController.Outcome.ACQUIRED);
        assertThat(second.tryAcquire(endpoint).outcome()).isEqualTo(DistributedAdmissionController.Outcome.GLOBAL_SATURATED);
        newer.permit().close();
        assertThat(second.tryAcquire(endpoint).outcome()).isEqualTo(DistributedAdmissionController.Outcome.ACQUIRED);
    }

    @Test
    void renewalKeepsLongRunningPermitOwnedBeyondOriginalLease() throws Exception {
        controllers(1, 1, Duration.ofMillis(300), Duration.ofMillis(100));
        var endpoint = UUID.randomUUID();
        var held = first.tryAcquire(endpoint);

        Thread.sleep(450);

        assertThat(second.tryAcquire(endpoint).outcome()).isEqualTo(DistributedAdmissionController.Outcome.GLOBAL_SATURATED);
        held.permit().close();
        assertThat(second.tryAcquire(endpoint).outcome()).isEqualTo(DistributedAdmissionController.Outcome.ACQUIRED);
    }

    @Test
    void redisUnavailabilityFailsClosedAndRecoveryAllowsNewAdmission() {
        controllers(1, 1, Duration.ofSeconds(30), Duration.ofSeconds(10));
        var endpoint = UUID.randomUUID();
        connectionFactory.stop();

        assertThat(first.tryAcquire(endpoint).outcome()).isEqualTo(DistributedAdmissionController.Outcome.REDIS_UNAVAILABLE);

        connectionFactory.start();
        assertThat(first.tryAcquire(endpoint).outcome()).isEqualTo(DistributedAdmissionController.Outcome.ACQUIRED);
    }

    private void controllers(int global, int endpoint, Duration lease, Duration renewal) {
        connectionFactory = new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        template = new StringRedisTemplate(connectionFactory);
        template.afterPropertiesSet();
        template.getConnectionFactory().getConnection().serverCommands().flushAll();
        var properties = new DeliveryProperties();
        properties.setGlobalConcurrency(global);
        properties.setEndpointConcurrency(endpoint);
        properties.setPermitLease(lease);
        properties.setPermitRenewal(renewal);
        first = new DistributedAdmissionController(template, properties, new DeliveryMetrics(new SimpleMeterRegistry()));
        second = new DistributedAdmissionController(template, properties, new DeliveryMetrics(new SimpleMeterRegistry()));
    }
}
