package dev.eventflow.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

@Component
public class LeaseRecovery {
    private final JdbcTemplate db;
    private final TransactionTemplate tx;
    private final StringRedisTemplate redis;
    private final RetryPolicy policy;
    private final boolean enabled;
    public LeaseRecovery(JdbcTemplate db,TransactionTemplate tx,StringRedisTemplate redis,RetryPolicy policy,@Value("${eventflow.worker-enabled:true}") boolean enabled) {
        this.db=db;this.tx=tx;this.redis=redis;this.policy=policy;this.enabled=enabled;
    }

    @Scheduled(fixedDelay=1000)
    public void recover() {
        if(!enabled) return;
        List<Due> scheduled=tx.execute(s->{
            var expired=db.queryForList("SELECT id,cycle_attempt_count,max_attempts FROM delivery_jobs WHERE status='IN_PROGRESS' AND lease_expires_at < now() ORDER BY lease_expires_at FOR UPDATE SKIP LOCKED LIMIT 100");
            List<Due> due=new ArrayList<>();
            for(var row:expired) {
                UUID id=(UUID)row.get("id");
                int cycle=((Number)row.get("cycle_attempt_count")).intValue();
                int max=((Number)row.get("max_attempts")).intValue();
                boolean retry=cycle<max;
                Instant when=retry?Instant.now().plus(policy.delay(cycle)):null;
                db.update("UPDATE delivery_attempts SET finished_at=now(),outcome='UNKNOWN',error_code='WORKER_LOST' WHERE delivery_job_id=? AND finished_at IS NULL",id);
                db.update("UPDATE delivery_jobs SET status=?,next_attempt_at=?,lease_owner=NULL,lease_expires_at=NULL,last_error_code='WORKER_LOST',updated_at=now() WHERE id=?",retry?"RETRY_SCHEDULED":"DEAD",when==null?null:Timestamp.from(when),id);
                if(retry) due.add(new Due(id,when));
            }
            return due;
        });
        if(scheduled!=null) for(Due due:scheduled) {
            try { redis.opsForZSet().add("eventflow:retry-due",due.id().toString(),due.when().toEpochMilli()); }
            catch(Exception ignored) { /* Database scan recovers missing index entries. */ }
        }
    }
    private record Due(UUID id,Instant when) {}
}
