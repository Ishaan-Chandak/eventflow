package dev.eventflow.worker;

import jakarta.annotation.PreDestroy;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class DistributedAdmissionController {
    static final String GLOBAL_KEY = "eventflow:{eventflow:admission}:global";
    private static final String ENDPOINT_PREFIX = "eventflow:{eventflow:admission}:endpoint:";

    private static final DefaultRedisScript<List> ACQUIRE = script("""
            local nowParts = redis.call('TIME')
            local now = nowParts[1] * 1000 + math.floor(nowParts[2] / 1000)
            local globalRecovered = redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
            local endpointRecovered = redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
            local globalUsed = redis.call('ZCARD', KEYS[1])
            local endpointUsed = redis.call('ZCARD', KEYS[2])
            if globalUsed >= tonumber(ARGV[3]) then
              return {2, globalUsed, endpointUsed, globalRecovered, endpointRecovered}
            end
            if endpointUsed >= tonumber(ARGV[4]) then
              return {3, globalUsed, endpointUsed, globalRecovered, endpointRecovered}
            end
            local expires = now + tonumber(ARGV[2])
            redis.call('ZADD', KEYS[1], expires, ARGV[1])
            redis.call('ZADD', KEYS[2], expires, ARGV[1])
            redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[2]) * 2)
            return {1, globalUsed + 1, endpointUsed + 1, globalRecovered, endpointRecovered}
            """);

    private static final DefaultRedisScript<Long> RENEW = new DefaultRedisScript<>("""
            local nowParts = redis.call('TIME')
            local now = nowParts[1] * 1000 + math.floor(nowParts[2] / 1000)
            local globalScore = redis.call('ZSCORE', KEYS[1], ARGV[1])
            local endpointScore = redis.call('ZSCORE', KEYS[2], ARGV[1])
            if not globalScore or not endpointScore or tonumber(globalScore) <= now or tonumber(endpointScore) <= now then
              redis.call('ZREM', KEYS[1], ARGV[1])
              redis.call('ZREM', KEYS[2], ARGV[1])
              return 0
            end
            local expires = now + tonumber(ARGV[2])
            redis.call('ZADD', KEYS[1], 'XX', expires, ARGV[1])
            redis.call('ZADD', KEYS[2], 'XX', expires, ARGV[1])
            redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[2]) * 2)
            return 1
            """, Long.class);

    private static final DefaultRedisScript<List> RELEASE = script("""
            local nowParts = redis.call('TIME')
            local now = nowParts[1] * 1000 + math.floor(nowParts[2] / 1000)
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
            redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
            local globalRemoved = redis.call('ZREM', KEYS[1], ARGV[1])
            local endpointRemoved = redis.call('ZREM', KEYS[2], ARGV[1])
            local paired = 0
            if globalRemoved == 1 and endpointRemoved == 1 then paired = 1 end
            return {paired, redis.call('ZCARD', KEYS[1]), redis.call('ZCARD', KEYS[2])}
            """);

    private final StringRedisTemplate redis;
    private final DeliveryMetrics metrics;
    private final int globalLimit;
    private final int endpointLimit;
    private final long leaseMillis;
    private final long renewalMillis;
    private final ScheduledExecutorService renewals;
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicInteger activeLocal = new AtomicInteger();

    public DistributedAdmissionController(StringRedisTemplate redis, DeliveryProperties properties, DeliveryMetrics metrics) {
        this.redis = redis;
        this.metrics = metrics;
        globalLimit = properties.getGlobalConcurrency();
        endpointLimit = properties.getEndpointConcurrency();
        leaseMillis = properties.getPermitLease().toMillis();
        renewalMillis = properties.getPermitRenewal().toMillis();
        ThreadFactory factory = task -> {
            var thread = new Thread(task, "eventflow-permit-renewal");
            thread.setDaemon(true);
            return thread;
        };
        renewals = Executors.newSingleThreadScheduledExecutor(factory);
        metrics.bindActivePermits(activeLocal);
    }

    public Admission tryAcquire(UUID endpointId) {
        Instant started = Instant.now();
        if (!accepting.get()) return measured(started, Outcome.REDIS_UNAVAILABLE, null);
        String endpointKey = endpointKey(endpointId);
        String token = UUID.randomUUID().toString();
        try {
            List<?> result = redis.execute(ACQUIRE, List.of(GLOBAL_KEY, endpointKey), token,
                    Long.toString(leaseMillis), Integer.toString(globalLimit), Integer.toString(endpointLimit));
            if (result == null || result.size() < 5) return measured(started, Outcome.REDIS_UNAVAILABLE, null);
            long code = number(result.get(0));
            metrics.recordPermitRecovery(number(result.get(3)), number(result.get(4)));
            metrics.recordUtilization(number(result.get(1)), globalLimit, number(result.get(2)), endpointLimit);
            if (code == 2) return measured(started, Outcome.GLOBAL_SATURATED, null);
            if (code == 3) return measured(started, Outcome.ENDPOINT_SATURATED, null);
            if (code != 1) return measured(started, Outcome.REDIS_UNAVAILABLE, null);
            var permit = new Permit(token, endpointKey);
            activeLocal.incrementAndGet();
            try {
                permit.renewal = renewals.scheduleAtFixedRate(permit::renew, renewalMillis, renewalMillis, TimeUnit.MILLISECONDS);
            } catch (RuntimeException e) {
                permit.close();
                return measured(started, Outcome.REDIS_UNAVAILABLE, null);
            }
            return measured(started, Outcome.ACQUIRED, permit);
        } catch (RuntimeException e) {
            return measured(started, Outcome.REDIS_UNAVAILABLE, null);
        }
    }

    public int activeLocalPermits() { return activeLocal.get(); }

    @PreDestroy
    public void stop() {
        if (accepting.compareAndSet(true, false)) renewals.shutdownNow();
    }

    private Admission measured(Instant started, Outcome outcome, Permit permit) {
        var metric = switch (outcome) {
            case ACQUIRED -> DeliveryMetrics.AdmissionResult.ACQUIRED;
            case GLOBAL_SATURATED -> DeliveryMetrics.AdmissionResult.GLOBAL_SATURATED;
            case ENDPOINT_SATURATED -> DeliveryMetrics.AdmissionResult.ENDPOINT_SATURATED;
            case REDIS_UNAVAILABLE -> DeliveryMetrics.AdmissionResult.REDIS_UNAVAILABLE;
        };
        metrics.recordAdmission(Duration.between(started, Instant.now()), metric);
        if (outcome != Outcome.ACQUIRED) metrics.recordDeferral(metric);
        return new Admission(outcome, permit);
    }

    private static String endpointKey(UUID endpointId) { return ENDPOINT_PREFIX + endpointId; }
    private static long number(Object value) { return value instanceof Number n ? n.longValue() : Long.parseLong(value.toString()); }
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static DefaultRedisScript<List> script(String source) { return new DefaultRedisScript(source, List.class); }

    public enum Outcome { ACQUIRED, GLOBAL_SATURATED, ENDPOINT_SATURATED, REDIS_UNAVAILABLE }
    public record Admission(Outcome outcome, Permit permit) { }

    public final class Permit implements AutoCloseable {
        private final String token;
        private final String endpointKey;
        private final AtomicBoolean owned = new AtomicBoolean(true);
        private volatile ScheduledFuture<?> renewal;

        private Permit(String token, String endpointKey) {
            this.token = token;
            this.endpointKey = endpointKey;
        }

        private void renew() {
            if (!owned.get()) return;
            try {
                Long renewed = redis.execute(RENEW, List.of(GLOBAL_KEY, endpointKey), token, Long.toString(leaseMillis));
                if (renewed == null || renewed != 1L) {
                    metrics.recordRenewalFailure(DeliveryMetrics.RenewalFailure.OWNERSHIP_LOST);
                    loseOwnership();
                }
            } catch (RuntimeException e) {
                metrics.recordRenewalFailure(DeliveryMetrics.RenewalFailure.REDIS_UNAVAILABLE);
            }
        }

        public boolean isOwned() { return owned.get(); }

        private void loseOwnership() {
            if (owned.compareAndSet(true, false)) {
                activeLocal.decrementAndGet();
                ScheduledFuture<?> scheduled = renewal;
                if (scheduled != null) scheduled.cancel(false);
            }
        }

        @Override
        public void close() {
            if (!owned.compareAndSet(true, false)) return;
            activeLocal.decrementAndGet();
            ScheduledFuture<?> scheduled = renewal;
            if (scheduled != null) scheduled.cancel(false);
            try {
                List<?> result = redis.execute(RELEASE, List.of(GLOBAL_KEY, endpointKey), token);
                if (result != null && result.size() >= 3)
                    metrics.recordUtilization(number(result.get(1)), globalLimit, number(result.get(2)), endpointLimit);
            }
            catch (RuntimeException ignored) { /* Expiry recovers an unreachable release. */ }
        }
    }
}
