package dev.eventflow.worker;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.ArgumentCaptor;

class DeliveryPipelineConcurrencyTest {
    @Test
    void exhaustedLocalCapacityPreventsRedisRead() throws Exception {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        properties.setShutdownGrace(Duration.ofSeconds(1));
        var registry = new SimpleMeterRegistry();
        var executor = new LocalDeliveryExecutor(properties, registry);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try {
            var permit = executor.reserve(1).getFirst();
            assertThat(permit.submit(() -> {
                entered.countDown();
                try { release.await(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            })).isTrue();
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            var redis = mock(StringRedisTemplate.class);
            var pipeline = new DeliveryPipeline(mock(JdbcTemplate.class), redis,
                    mock(TransactionTemplate.class), new RetryPolicy(5, 300), executor,
                    new DeliveryMetrics(registry), properties, true);

            pipeline.consume();
            pipeline.reclaimPending();

            verifyNoInteractions(redis);
        } finally {
            release.countDown();
            executor.stop();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void emptyReadUsesOnlyReservedCapacityAndReleasesIt() {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        var registry = new SimpleMeterRegistry();
        var executor = new LocalDeliveryExecutor(properties, registry);
        try {
            var redis = mock(StringRedisTemplate.class);
            StreamOperations<String,Object,Object> streams=mock(StreamOperations.class);
            when(redis.opsForStream()).thenReturn(streams);
            var pipeline = new DeliveryPipeline(mock(JdbcTemplate.class), redis,
                    mock(TransactionTemplate.class), new RetryPolicy(5, 300), executor,
                    new DeliveryMetrics(registry), properties, true);

            pipeline.consume();

            var options=ArgumentCaptor.forClass(StreamReadOptions.class);
            verify(streams).read(any(Consumer.class),options.capture(),(StreamOffset<String>[])any(StreamOffset[].class));
            assertThat(options.getValue().getCount()).isEqualTo(1);
            assertThat(executor.availableCapacity()).isEqualTo(1);
        } finally {
            executor.stop();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void redisReadExceptionReleasesReservedCapacity() {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        var registry = new SimpleMeterRegistry();
        var executor = new LocalDeliveryExecutor(properties, registry);
        try {
            var redis = mock(StringRedisTemplate.class);
            StreamOperations<String,Object,Object> streams=mock(StreamOperations.class);
            when(redis.opsForStream()).thenReturn(streams);
            when(streams.read(any(Consumer.class),any(StreamReadOptions.class),(StreamOffset<String>[])any(StreamOffset[].class)))
                    .thenThrow(new IllegalStateException("redis unavailable"));
            var pipeline = new DeliveryPipeline(mock(JdbcTemplate.class), redis,
                    mock(TransactionTemplate.class), new RetryPolicy(5, 300), executor,
                    new DeliveryMetrics(registry), properties, true);

            pipeline.consume();

            assertThat(executor.availableCapacity()).isEqualTo(1);
        } finally {
            executor.stop();
        }
    }
}
