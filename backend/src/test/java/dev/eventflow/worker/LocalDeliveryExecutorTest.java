package dev.eventflow.worker;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class LocalDeliveryExecutorTest {
    @Test
    void neverRunsMoreTasksThanConfiguredConcurrency() throws Exception {
        var executor = executor(2);
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var running = new AtomicInteger();
        var maximum = new AtomicInteger();
        try {
            List<LocalDeliveryExecutor.Permit> permits = executor.reserve(3);
            assertThat(permits).hasSize(2);
            permits.forEach(permit -> assertThat(permit.submit(() -> {
                int active = running.incrementAndGet();
                maximum.accumulateAndGet(active, Math::max);
                entered.countDown();
                try { release.await(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                finally { running.decrementAndGet(); }
            })).isTrue());

            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(executor.reserve(1)).isEmpty();
            assertThat(maximum.get()).isEqualTo(2);
        } finally {
            release.countDown();
            executor.stop();
        }
    }

    @Test
    void taskFailureReturnsCapacity() throws Exception {
        var executor = executor(1);
        try {
            var failed = executor.reserve(1).getFirst();
            assertThat(failed.submit(() -> { throw new IllegalStateException("boom"); })).isTrue();
            awaitAvailable(executor);
        } finally {
            executor.stop();
        }
    }

    @Test
    void exhaustedAdmissionIncrementsSaturationCount() {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        var registry = new SimpleMeterRegistry();
        var executor = new LocalDeliveryExecutor(properties, registry);
        try (var permit = executor.reserve(1).getFirst()) {
            assertThat(executor.reserve(1)).isEmpty();
            assertThat(registry.get("eventflow.delivery.executor.saturation").counter().count()).isEqualTo(1);
        } finally {
            executor.stop();
        }
    }

    @Test
    void batchRequestLargerThanConfiguredCapacityIsNotItselfSaturation() {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        var registry = new SimpleMeterRegistry();
        var executor = new LocalDeliveryExecutor(properties, registry);
        try (var permit = executor.reserve(10).getFirst()) {
            assertThat(registry.get("eventflow.delivery.executor.saturation").counter().count()).isZero();
        } finally {
            executor.stop();
        }
    }

    @Test
    void rejectedSubmissionReturnsCapacity() {
        var executor = executor(1);
        var permit = executor.reserve(1).getFirst();
        executor.stop();

        assertThat(permit.submit(() -> {})).isFalse();
        assertThat(executor.availableCapacity()).isEqualTo(1);
    }

    @Test
    void completionCallbackCanRefillReleasedCapacity() throws Exception {
        var executor = executor(1);
        var secondRan = new CountDownLatch(1);
        try {
            var first = executor.reserve(1).getFirst();
            assertThat(first.submit(() -> {}, () -> {
                var next = executor.reserve(1);
                if (!next.isEmpty()) next.getFirst().submit(secondRan::countDown);
            })).isTrue();

            assertThat(secondRan.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.stop();
        }
    }

    @Test
    void stopInterruptsOutstandingTaskWithinBoundedGraceAndReturnsCapacity() throws Exception {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        properties.setShutdownGrace(Duration.ofMillis(50));
        var executor = new LocalDeliveryExecutor(properties, new SimpleMeterRegistry());
        var entered = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var permit = executor.reserve(1).getFirst();
        assertThat(permit.submit(() -> {
            entered.countDown();
            try { Thread.sleep(10_000); }
            catch (InterruptedException e) { interrupted.countDown(); Thread.currentThread().interrupt(); }
        })).isTrue();
        assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();

        long started = System.nanoTime();
        executor.stop();

        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(1));
        assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.availableCapacity()).isEqualTo(1);
    }

    private static LocalDeliveryExecutor executor(int concurrency) {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(concurrency);
        properties.setShutdownGrace(Duration.ofSeconds(1));
        return new LocalDeliveryExecutor(properties, new SimpleMeterRegistry());
    }

    private static void awaitAvailable(LocalDeliveryExecutor executor) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (executor.availableCapacity() == 0 && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(executor.availableCapacity()).isEqualTo(1);
    }
}
