package dev.eventflow.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.models.stream.ClaimedMessages;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Component
public class DeliveryPipeline {
    private static final String STREAM="eventflow:deliveries", GROUP="delivery-workers";
    private final JdbcTemplate db; private final StringRedisTemplate redis; private final TransactionTemplate tx;
    private final RetryPolicy retryPolicy;
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).followRedirects(HttpClient.Redirect.NEVER).build();
    private final boolean enabled;
    private final String owner=UUID.randomUUID().toString();
    private final int leaseSeconds;
    private final int pendingIdleSeconds;
    private String pendingCursor="0-0";
    public DeliveryPipeline(JdbcTemplate db,StringRedisTemplate redis,TransactionTemplate tx,RetryPolicy retryPolicy,@Value("${eventflow.worker-enabled:true}") boolean enabled,@Value("${eventflow.lease-seconds:15}") int leaseSeconds,@Value("${eventflow.pending-idle-seconds:20}") int pendingIdleSeconds) {
        if(leaseSeconds<=5 || pendingIdleSeconds<=leaseSeconds) throw new IllegalArgumentException("Lease must exceed HTTP timeout and pending idle time must exceed lease");
        this.db=db;this.redis=redis;this.tx=tx;this.retryPolicy=retryPolicy;this.enabled=enabled;this.leaseSeconds=leaseSeconds;this.pendingIdleSeconds=pendingIdleSeconds;
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
        try {
            try { redis.opsForStream().createGroup(STREAM,ReadOffset.from("0-0"),GROUP); } catch(Exception ignored) { /* existing group or absent stream */ }
            var messages=redis.opsForStream().read(Consumer.from(GROUP,owner),StreamReadOptions.empty().count(10),StreamOffset.create(STREAM,ReadOffset.lastConsumed()));
            if(messages==null) return;
            for(var message:messages) {
                try { if(deliver(UUID.fromString((String)message.getValue().get("jobId")))) redis.opsForStream().acknowledge(STREAM,GROUP,message.getId()); }
                catch(Exception ignored) { /* keep pending for recovery in later phase */ }
            }
        } catch(Exception ignored) { }
    }

    @Scheduled(fixedDelay=5000)
    public void reclaimPending() {
        if(!enabled) return;
        try {
            ClaimedMessages<byte[],byte[]> claimed=redis.execute((RedisCallback<ClaimedMessages<byte[],byte[]>>) connection->{
                @SuppressWarnings("unchecked")
                RedisAsyncCommands<byte[],byte[]> nativeConnection=(RedisAsyncCommands<byte[],byte[]>)connection.getNativeConnection();
                var args=new XAutoClaimArgs<byte[]>().consumer(io.lettuce.core.Consumer.from(bytes(GROUP),bytes(owner)))
                        .minIdleTime(pendingIdleSeconds*1000L).startId(pendingCursor).count(20);
                try { return nativeConnection.xautoclaim(bytes(STREAM),args).get(5,java.util.concurrent.TimeUnit.SECONDS); }
                catch(Exception e) { throw new IllegalStateException("XAUTOCLAIM failed",e); }
            });
            if(claimed==null) return;
            pendingCursor=claimed.getId();
            for(var message:claimed.getMessages()) {
                for(var field:message.getBody().entrySet()) {
                    if(!"jobId".equals(decode(field.getKey()))) continue;
                    try {
                        if(deliver(UUID.fromString(decode(field.getValue())))) redis.opsForStream().acknowledge(STREAM,GROUP,RecordId.of(message.getId()));
                    } catch(Exception ignored) { /* Leave pending for another recovery pass. */ }
                    break;
                }
            }
        } catch(Exception ignored) { /* Redis may be unavailable. */ }
    }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String decode(Object value) { return value instanceof byte[] b?new String(b,StandardCharsets.UTF_8):String.valueOf(value); }
    public boolean deliver(UUID id) throws Exception {
        Claimed claimed=tx.execute(s->{
            int changed=db.update("UPDATE delivery_jobs SET status='IN_PROGRESS',attempt_count=attempt_count+1,cycle_attempt_count=cycle_attempt_count+1,lease_owner=?,lease_expires_at=now()+(? * interval '1 second'),updated_at=now() WHERE id=? AND status='QUEUED'",owner,leaseSeconds,id);
            if(changed==0) return null;
            var job=db.queryForMap("SELECT j.event_id,j.attempt_count,j.cycle_attempt_count,j.max_attempts,e.payload,p.url FROM delivery_jobs j JOIN events e ON e.id=j.event_id JOIN webhook_endpoints p ON p.id=j.endpoint_id WHERE j.id=?",id);
            UUID attemptId=UUID.randomUUID();
            int number=((Number)job.get("attempt_count")).intValue();
            db.update("INSERT INTO delivery_attempts(id,delivery_job_id,attempt_number,started_at) VALUES (?,?,?,now())",attemptId,id,number);
            return new Claimed(job,attemptId,number);
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
            var request=HttpRequest.newBuilder(URI.create((String)job.get("url"))).timeout(Duration.ofSeconds(5)).header("Content-Type","application/json").header("X-EventFlow-Delivery-Id",id.toString()).header("X-EventFlow-Event-Id",job.get("event_id").toString()).header("X-EventFlow-Attempt",Integer.toString(attempt)).POST(HttpRequest.BodyPublishers.ofString(job.get("payload").toString())).build();
            status=http.send(request,HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch(java.net.http.HttpTimeoutException e) { errorCode="TIMEOUT"; }
        catch(Exception e) { errorCode="NETWORK_ERROR"; }
        RetryPolicy.Outcome outcome=retryPolicy.classify(status);
        int cycleAttempts=((Number)job.get("cycle_attempt_count")).intValue();
        int maxAttempts=((Number)job.get("max_attempts")).intValue();
        boolean schedule=retryPolicy.shouldRetry(outcome,cycleAttempts,maxAttempts);
        Instant due=schedule ? Instant.now().plus(retryPolicy.delay(cycleAttempts)) : null;
        int finalStatus=status; String finalError=errorCode;
        boolean committed=Boolean.TRUE.equals(tx.execute(s->{
            int changed=db.update("UPDATE delivery_jobs SET status=?,next_attempt_at=?,last_http_status=?,last_error_code=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=? AND status='IN_PROGRESS' AND lease_owner=?",outcome==RetryPolicy.Outcome.SUCCESS?"SUCCEEDED":schedule?"RETRY_SCHEDULED":"DEAD",due==null?null:java.sql.Timestamp.from(due),finalStatus==0?null:finalStatus,finalError,id,owner);
            if(changed==0) return false;
            db.update("UPDATE delivery_attempts SET finished_at=now(),outcome=?,http_status=?,duration_ms=?,error_code=? WHERE id=?",outcome.name(),finalStatus==0?null:finalStatus,Duration.between(started,Instant.now()).toMillis(),finalError,attemptId);
            return true;
        }));
        if(committed && schedule) {
            try { redis.opsForZSet().add("eventflow:retry-due",id.toString(),due.toEpochMilli()); }
            catch(Exception ignored) { /* PostgreSQL due time is authoritative. */ }
        }
        return committed;
    }
    private record Claimed(Map<String,Object> job,UUID attemptId,int attemptNumber) {}
}
