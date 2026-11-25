package dev.eventflow.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.models.stream.ClaimedMessages;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.*;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Component
public class DeliveryPipeline {
    private static final Logger log=LoggerFactory.getLogger(DeliveryPipeline.class);
    private static final String STREAM="eventflow:deliveries", GROUP="delivery-workers";
    private final JdbcTemplate db; private final StringRedisTemplate redis; private final TransactionTemplate tx;
    private final RetryPolicy retryPolicy;
    private final LocalDeliveryExecutor deliveryExecutor;
    private final DeliveryMetrics metrics;
    private final DistributedAdmissionController admission;
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).followRedirects(HttpClient.Redirect.NEVER).build();
    private final boolean enabled;
    private final String owner=UUID.randomUUID().toString();
    private final int leaseSeconds;
    private final int pendingIdleSeconds;
    private final Duration httpRequestTimeout;
    private final Duration capacityDeferral;
    private String pendingCursor="0-0";
    public DeliveryPipeline(JdbcTemplate db,StringRedisTemplate redis,TransactionTemplate tx,RetryPolicy retryPolicy,
                            LocalDeliveryExecutor deliveryExecutor,DeliveryMetrics metrics,DistributedAdmissionController admission,
                            DeliveryProperties properties,
                            @Value("${eventflow.worker-enabled:true}") boolean enabled) {
        this.db=db;this.redis=redis;this.tx=tx;this.retryPolicy=retryPolicy;this.deliveryExecutor=deliveryExecutor;
        this.metrics=metrics;this.admission=admission;this.enabled=enabled;this.leaseSeconds=properties.getLeaseSeconds();
        this.pendingIdleSeconds=properties.getPendingIdleSeconds();this.httpRequestTimeout=properties.getHttpRequestTimeout();
        this.capacityDeferral=properties.getCapacityDeferral();
    }

    @Scheduled(fixedDelay=1000)
    public void publish() {
        if(!enabled) return;
        try {
            var rows=db.queryForList("WITH picked AS (SELECT id FROM outbox_messages WHERE published_at IS NULL AND (lease_expires_at IS NULL OR lease_expires_at < now()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 20) UPDATE outbox_messages o SET lease_owner=?, lease_expires_at=now()+interval '30 seconds', publish_attempts=publish_attempts+1 FROM picked WHERE o.id=picked.id RETURNING o.id,o.delivery_job_id",owner);
            for(var row:rows) {
                UUID id=(UUID)row.get("id"); UUID job=(UUID)row.get("delivery_job_id");
                redis.opsForStream().add(STREAM,Map.of("jobId",job.toString()));
                db.update("UPDATE outbox_messages SET published_at=now(),lease_owner=NULL,lease_expires_at=NULL WHERE id=? AND lease_owner=? AND published_at IS NULL",id,owner);
            }
        } catch(Exception ignored) { /* PostgreSQL outbox remains for next pass. */ }
    }

    @Scheduled(fixedDelay=500)
    public void consume() {
        if(!enabled) return;
        var permits=deliveryExecutor.reserve(10);
        if(permits.isEmpty()) return;
        try {
            try { redis.opsForStream().createGroup(STREAM,ReadOffset.from("0-0"),GROUP); } catch(Exception ignored) { /* existing group or absent stream */ }
            var messages=redis.opsForStream().read(Consumer.from(GROUP,owner),StreamReadOptions.empty().count(permits.size()),StreamOffset.create(STREAM,ReadOffset.lastConsumed()));
            int used=messages==null?0:Math.min(messages.size(),permits.size());
            for(int i=0;i<used;i++) {
                var message=messages.get(i);
                submit(permits.get(i),decode(message.getValue().get("jobId")),message.getId());
            }
            for(int i=used;i<permits.size();i++) permits.get(i).close();
        } catch(Exception e) {
            permits.forEach(LocalDeliveryExecutor.Permit::close);
            log.warn("Redis delivery read failed",e);
        }
    }

    @Scheduled(fixedDelay=5000)
    public void reclaimPending() {
        if(!enabled) return;
        var permits=deliveryExecutor.reserve(20);
        if(permits.isEmpty()) return;
        try {
            ClaimedMessages<byte[],byte[]> claimed=redis.execute((RedisCallback<ClaimedMessages<byte[],byte[]>>) connection->{
                @SuppressWarnings("unchecked")
                RedisStreamAsyncCommands<byte[],byte[]> nativeConnection=(RedisStreamAsyncCommands<byte[],byte[]>)connection.getNativeConnection();
                var args=new XAutoClaimArgs<byte[]>().consumer(io.lettuce.core.Consumer.from(bytes(GROUP),bytes(owner)))
                        .minIdleTime(pendingIdleSeconds*1000L).startId(pendingCursor).count(permits.size());
                try { return nativeConnection.xautoclaim(bytes(STREAM),args).get(5,java.util.concurrent.TimeUnit.SECONDS); }
                catch(Exception e) { throw new IllegalStateException("XAUTOCLAIM failed",e); }
            });
            if(claimed==null) { permits.forEach(LocalDeliveryExecutor.Permit::close);return; }
            pendingCursor=claimed.getId();
            int used=Math.min(claimed.getMessages().size(),permits.size());
            for(int i=0;i<used;i++) {
                var message=claimed.getMessages().get(i);
                boolean found=false;
                for(var field:message.getBody().entrySet()) {
                    if(!"jobId".equals(decode(field.getKey()))) continue;
                    submit(permits.get(i),decode(field.getValue()),RecordId.of(message.getId()));
                    found=true;
                    break;
                }
                if(!found) permits.get(i).close();
            }
            for(int i=used;i<permits.size();i++) permits.get(i).close();
        } catch(Exception e) {
            permits.forEach(LocalDeliveryExecutor.Permit::close);
            log.warn("Redis pending delivery reclaim failed",e);
        }
    }
    private void submit(LocalDeliveryExecutor.Permit permit,String rawJobId,RecordId recordId) {
        final UUID jobId;
        try { jobId=UUID.fromString(rawJobId); }
        catch(Exception e) { permit.close();log.warn("Invalid delivery stream message recordId={} jobId={}",recordId,rawJobId);return; }
        final UUID endpointId;
        try {
            endpointId=db.queryForObject("SELECT endpoint_id FROM delivery_jobs WHERE id=?",UUID.class,jobId);
        } catch(Exception e) {
            permit.close();
            log.warn("Cannot resolve endpoint for delivery jobId={} recordId={}",jobId,recordId,e);
            return;
        }
        var distributed=admission.tryAcquire(endpointId);
        if(distributed.outcome()!=DistributedAdmissionController.Outcome.ACQUIRED) {
            deferCapacity(jobId);
            permit.close();
            log.debug("Distributed delivery admission deferred jobId={} recordId={} reason={}",jobId,recordId,distributed.outcome());
            return;
        }
        var distributedPermit=distributed.permit();
        boolean accepted=permit.submit(()->{
            try {
                if(!distributedPermit.isOwned()) {
                    deferCapacity(jobId);
                    metrics.recordDeferral(DeliveryMetrics.AdmissionResult.OWNERSHIP_LOST);
                    return;
                }
                boolean durable=deliver(jobId);
                if(durable && deliveryExecutor.isAccepting() && !Thread.currentThread().isInterrupted())
                    redis.opsForStream().acknowledge(STREAM,GROUP,recordId);
            } catch(InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Delivery interrupted jobId={} recordId={}",jobId,recordId);
            } catch(Exception e) {
                log.warn("Delivery failed without acknowledgement jobId={} recordId={}",jobId,recordId,e);
            } finally {
                distributedPermit.close();
            }
        },this::consume,distributedPermit::close);
        if(!accepted) {
            deferCapacity(jobId);
            distributedPermit.close();
            metrics.recordDeferral(DeliveryMetrics.AdmissionResult.EXECUTOR_REJECTED);
            log.warn("Local delivery executor rejected jobId={} recordId={}",jobId,recordId);
        }
    }
    private void deferCapacity(UUID jobId) {
        try {
            db.update("UPDATE delivery_jobs SET capacity_deferred_until=now()+(? * interval '1 millisecond'),updated_at=now() WHERE id=? AND status='QUEUED'",
                    capacityDeferral.toMillis(),jobId);
        } catch(Exception e) {
            log.warn("Failed to persist capacity deferral jobId={}",jobId,e);
        }
    }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String decode(Object value) { return value instanceof byte[] b?new String(b,StandardCharsets.UTF_8):String.valueOf(value); }
    public boolean deliver(UUID id) throws Exception {
        String claimOwner=owner+":"+UUID.randomUUID();
        Claimed claimed=tx.execute(s->{
            int changed=db.update("UPDATE delivery_jobs SET status='IN_PROGRESS',attempt_count=attempt_count+1,cycle_attempt_count=cycle_attempt_count+1,capacity_deferred_until=NULL,lease_owner=?,lease_expires_at=now()+(? * interval '1 second'),updated_at=now() WHERE id=? AND status='QUEUED'",claimOwner,leaseSeconds,id);
            if(changed==0) return null;
            var job=db.queryForMap("SELECT j.event_id,j.attempt_count,j.cycle_attempt_count,j.max_attempts,e.payload,p.url FROM delivery_jobs j JOIN events e ON e.id=j.event_id JOIN webhook_endpoints p ON p.id=j.endpoint_id WHERE j.id=?",id);
            UUID attemptId=UUID.randomUUID();
            int number=((Number)job.get("attempt_count")).intValue();
            db.update("INSERT INTO delivery_attempts(id,delivery_job_id,attempt_number,started_at) VALUES (?,?,?,now())",attemptId,id,number);
            return new Claimed(job,attemptId,number,claimOwner);
        });
        if(claimed==null) {
            var states=db.query("SELECT status FROM delivery_jobs WHERE id=?",(rs,n)->rs.getString(1),id);
            return !states.isEmpty() && !"IN_PROGRESS".equals(states.getFirst());
        }
        var job=claimed.job();
        int attempt=claimed.attemptNumber(); UUID attemptId=claimed.attemptId();
        Instant started=Instant.now();
        int status=0; String errorCode=null;
        try {
            var request=HttpRequest.newBuilder(URI.create((String)job.get("url"))).timeout(httpRequestTimeout).header("Content-Type","application/json").header("X-EventFlow-Delivery-Id",id.toString()).header("X-EventFlow-Event-Id",job.get("event_id").toString()).header("X-EventFlow-Attempt",Integer.toString(attempt)).POST(HttpRequest.BodyPublishers.ofString(job.get("payload").toString())).build();
            HttpResponse<InputStream> response=http.send(request,HttpResponse.BodyHandlers.ofInputStream());
            status=response.statusCode();
            try { response.body().close(); } catch(Exception ignored) { /* Status and headers define the result. */ }
        } catch(java.net.http.HttpTimeoutException e) { errorCode="TIMEOUT"; }
        catch(InterruptedException e) {
            metrics.recordHttp(Duration.between(started,Instant.now()),DeliveryMetrics.HttpResult.NETWORK_ERROR);
            Thread.currentThread().interrupt();
            throw e;
        }
        catch(Exception e) { errorCode="NETWORK_ERROR"; }
        RetryPolicy.Outcome outcome=retryPolicy.classify(status);
        DeliveryMetrics.HttpResult httpResult="TIMEOUT".equals(errorCode)?DeliveryMetrics.HttpResult.TIMEOUT:
                "NETWORK_ERROR".equals(errorCode)?DeliveryMetrics.HttpResult.NETWORK_ERROR:
                        switch(outcome) {
                            case SUCCESS -> DeliveryMetrics.HttpResult.SUCCESS;
                            case TRANSIENT_FAILURE -> DeliveryMetrics.HttpResult.TRANSIENT_FAILURE;
                            case PERMANENT_FAILURE -> DeliveryMetrics.HttpResult.PERMANENT_FAILURE;
                        };
        metrics.recordHttp(Duration.between(started,Instant.now()),httpResult);
        int cycleAttempts=((Number)job.get("cycle_attempt_count")).intValue();
        int maxAttempts=((Number)job.get("max_attempts")).intValue();
        boolean schedule=retryPolicy.shouldRetry(outcome,cycleAttempts,maxAttempts);
        Instant due=schedule ? Instant.now().plus(retryPolicy.delay(cycleAttempts)) : null;
        int finalStatus=status; String finalError=errorCode;
        boolean committed=Boolean.TRUE.equals(tx.execute(s->{
            int changed=db.update("UPDATE delivery_jobs SET status=?,next_attempt_at=?,last_http_status=?,last_error_code=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=? AND status='IN_PROGRESS' AND lease_owner=?",outcome==RetryPolicy.Outcome.SUCCESS?"SUCCEEDED":schedule?"RETRY_SCHEDULED":"DEAD",due==null?null:java.sql.Timestamp.from(due),finalStatus==0?null:finalStatus,finalError,id,claimed.leaseOwner());
            if(changed==0) return false;
            db.update("UPDATE delivery_attempts SET finished_at=now(),outcome=?,http_status=?,duration_ms=?,error_code=? WHERE id=?",outcome.name(),finalStatus==0?null:finalStatus,Duration.between(started,Instant.now()).toMillis(),finalError,attemptId);
            return true;
        }));
        if(committed && schedule) {
            try { redis.opsForZSet().add("eventflow:retry-due",id.toString(),due.toEpochMilli()); }
            catch(Exception ignored) { /* PostgreSQL due time is authoritative. */ }
        }
        metrics.recordDurableOutcome(!committed?DeliveryMetrics.DurableOutcome.UNFINALIZED:
                outcome==RetryPolicy.Outcome.SUCCESS?DeliveryMetrics.DurableOutcome.SUCCEEDED:
                        schedule?DeliveryMetrics.DurableOutcome.RETRY_SCHEDULED:DeliveryMetrics.DurableOutcome.DEAD);
        return committed;
    }
    private record Claimed(Map<String,Object> job,UUID attemptId,int attemptNumber,String leaseOwner) {}
}
