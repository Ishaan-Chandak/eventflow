package dev.eventflow.worker;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
                    new DeliveryMetrics(registry), mock(DistributedAdmissionController.class), properties, true);

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
                    new DeliveryMetrics(registry), mock(DistributedAdmissionController.class), properties, true);

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
                    new DeliveryMetrics(registry), mock(DistributedAdmissionController.class), properties, true);

            pipeline.consume();

            assertThat(executor.availableCapacity()).isEqualTo(1);
        } finally {
            executor.stop();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void distributedSaturationDefersWithoutClaimingOrSubmitting() {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        var registry = new SimpleMeterRegistry();
        var executor = new LocalDeliveryExecutor(properties, registry);
        try {
            UUID jobId = UUID.randomUUID();
            UUID endpointId = UUID.randomUUID();
            var db = mock(JdbcTemplate.class);
            when(db.queryForObject(any(String.class), org.mockito.ArgumentMatchers.eq(UUID.class), org.mockito.ArgumentMatchers.eq(jobId)))
                    .thenReturn(endpointId);
            var redis = mock(StringRedisTemplate.class);
            StreamOperations<String,Object,Object> streams = mock(StreamOperations.class);
            when(redis.opsForStream()).thenReturn(streams);
            var record = StreamRecords.<String, Object, Object>mapBacked(Map.of("jobId", jobId.toString()))
                    .withStreamKey("eventflow:deliveries").withId(RecordId.of("1-0"));
            when(streams.read(any(Consumer.class), any(StreamReadOptions.class),
                    (StreamOffset<String>[]) any(StreamOffset[].class))).thenReturn(List.of(record), List.of());
            var admission = mock(DistributedAdmissionController.class);
            when(admission.tryAcquire(endpointId)).thenReturn(new DistributedAdmissionController.Admission(
                    DistributedAdmissionController.Outcome.ENDPOINT_SATURATED, null));
            var tx = mock(TransactionTemplate.class);
            var pipeline = new DeliveryPipeline(db, redis, tx, new RetryPolicy(5, 300), executor,
                    new DeliveryMetrics(registry), admission, properties, true);

            pipeline.consume();

            assertThat(executor.availableCapacity()).isEqualTo(1);
            verify(admission).tryAcquire(endpointId);
            verifyNoInteractions(tx);
            verify(streams, org.mockito.Mockito.never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        } finally {
            executor.stop();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void executorRejectionReleasesDistributedPermitWithoutClaiming() {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        var registry = new SimpleMeterRegistry();
        var executor = new LocalDeliveryExecutor(properties, registry);
        UUID jobId = UUID.randomUUID();
        UUID endpointId = UUID.randomUUID();
        var db = mock(JdbcTemplate.class);
        when(db.queryForObject(any(String.class), org.mockito.ArgumentMatchers.eq(UUID.class), org.mockito.ArgumentMatchers.eq(jobId))).thenReturn(endpointId);
        var redis = mock(StringRedisTemplate.class);
        StreamOperations<String,Object,Object> streams = mock(StreamOperations.class);
        when(redis.opsForStream()).thenReturn(streams);
        var record = StreamRecords.<String, Object, Object>mapBacked(Map.of("jobId", jobId.toString()))
                .withStreamKey("eventflow:deliveries").withId(RecordId.of("2-0"));
        when(streams.read(any(Consumer.class), any(StreamReadOptions.class),
                (StreamOffset<String>[]) any(StreamOffset[].class))).thenReturn(List.of(record));
        var admission = mock(DistributedAdmissionController.class);
        var distributedPermit = mock(DistributedAdmissionController.Permit.class);
        when(admission.tryAcquire(endpointId)).thenAnswer(invocation -> {
            executor.stop();
            return new DistributedAdmissionController.Admission(DistributedAdmissionController.Outcome.ACQUIRED, distributedPermit);
        });
        var tx = mock(TransactionTemplate.class);
        var pipeline = new DeliveryPipeline(db, redis, tx, new RetryPolicy(5, 300), executor,
                new DeliveryMetrics(registry), admission, properties, true);

        pipeline.consume();

        verify(distributedPermit).close();
        verifyNoInteractions(tx);
        assertThat(registry.get("eventflow.delivery.admission.deferrals").tag("reason", "executor_rejected").counter().count()).isEqualTo(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void deliveryExceptionReleasesDistributedPermitAndDoesNotAcknowledge() throws Exception {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        var registry = new SimpleMeterRegistry();
        var executor = new LocalDeliveryExecutor(properties, registry);
        try {
            UUID jobId = UUID.randomUUID();
            UUID endpointId = UUID.randomUUID();
            var db = mock(JdbcTemplate.class);
            when(db.queryForObject(any(String.class), org.mockito.ArgumentMatchers.eq(UUID.class), org.mockito.ArgumentMatchers.eq(jobId))).thenReturn(endpointId);
            var redis = mock(StringRedisTemplate.class);
            StreamOperations<String,Object,Object> streams = mock(StreamOperations.class);
            when(redis.opsForStream()).thenReturn(streams);
            var record = StreamRecords.<String, Object, Object>mapBacked(Map.of("jobId", jobId.toString()))
                    .withStreamKey("eventflow:deliveries").withId(RecordId.of("3-0"));
            when(streams.read(any(Consumer.class), any(StreamReadOptions.class),
                    (StreamOffset<String>[]) any(StreamOffset[].class))).thenReturn(List.of(record), List.of());
            var admission = mock(DistributedAdmissionController.class);
            var distributedPermit = mock(DistributedAdmissionController.Permit.class);
            when(admission.tryAcquire(endpointId)).thenReturn(new DistributedAdmissionController.Admission(
                    DistributedAdmissionController.Outcome.ACQUIRED, distributedPermit));
            var pipeline = spy(new DeliveryPipeline(db, redis, mock(TransactionTemplate.class), new RetryPolicy(5, 300), executor,
                    new DeliveryMetrics(registry), admission, properties, true));
            doThrow(new IllegalStateException("delivery failed")).when(pipeline).deliver(jobId);

            pipeline.consume();

            verify(distributedPermit, timeout(1000)).close();
            verify(streams, org.mockito.Mockito.never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        } finally {
            executor.stop();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void permitThatLosesOwnershipWhileQueuedNeverStartsDelivery() {
        var properties = new DeliveryProperties();
        properties.setLocalConcurrency(1);
        var registry = new SimpleMeterRegistry();
        var executor = new LocalDeliveryExecutor(properties, registry);
        try {
            UUID jobId = UUID.randomUUID();
            UUID endpointId = UUID.randomUUID();
            var db = mock(JdbcTemplate.class);
            when(db.queryForObject(any(String.class), org.mockito.ArgumentMatchers.eq(UUID.class), org.mockito.ArgumentMatchers.eq(jobId))).thenReturn(endpointId);
            var redis = mock(StringRedisTemplate.class);
            StreamOperations<String,Object,Object> streams = mock(StreamOperations.class);
            when(redis.opsForStream()).thenReturn(streams);
            var record = StreamRecords.<String, Object, Object>mapBacked(Map.of("jobId", jobId.toString()))
                    .withStreamKey("eventflow:deliveries").withId(RecordId.of("4-0"));
            when(streams.read(any(Consumer.class), any(StreamReadOptions.class),
                    (StreamOffset<String>[]) any(StreamOffset[].class))).thenReturn(List.of(record), List.of());
            var admission = mock(DistributedAdmissionController.class);
            var distributedPermit = mock(DistributedAdmissionController.Permit.class);
            when(distributedPermit.isOwned()).thenReturn(false);
            when(admission.tryAcquire(endpointId)).thenReturn(new DistributedAdmissionController.Admission(
                    DistributedAdmissionController.Outcome.ACQUIRED, distributedPermit));
            var tx = mock(TransactionTemplate.class);
            var pipeline = new DeliveryPipeline(db, redis, tx, new RetryPolicy(5, 300), executor,
                    new DeliveryMetrics(registry), admission, properties, true);

            pipeline.consume();

            verify(distributedPermit, timeout(1000)).close();
            verifyNoInteractions(tx);
            verify(streams, org.mockito.Mockito.never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        } finally {
            executor.stop();
        }
    }
}
