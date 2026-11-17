package dev.eventflow.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;

@Component
public class RetryScheduler {
    private static final String INDEX="eventflow:retry-due";
    private final JdbcTemplate db;
    private final StringRedisTemplate redis;
    private final TransactionTemplate tx;
    private final boolean enabled;

    public RetryScheduler(JdbcTemplate db,StringRedisTemplate redis,TransactionTemplate tx,@Value("${eventflow.worker-enabled:true}") boolean enabled) {
        this.db=db;this.redis=redis;this.tx=tx;this.enabled=enabled;
    }

    @Scheduled(fixedDelay=1000)
    public void schedule() {
        if(!enabled) return;
        Set<UUID> candidates=new LinkedHashSet<>();
        try {
            var due=redis.opsForZSet().rangeByScore(INDEX,Double.NEGATIVE_INFINITY,System.currentTimeMillis(),0,100);
            if(due!=null) for(String id:due) {
                try { candidates.add(UUID.fromString(id)); } catch(IllegalArgumentException ignored) { }
            }
        } catch(Exception ignored) { /* Database scan still finds due jobs. */ }
        candidates.addAll(db.query("SELECT id FROM delivery_jobs WHERE status='RETRY_SCHEDULED' AND next_attempt_at <= now() ORDER BY next_attempt_at LIMIT 100",(rs,n)->rs.getObject(1,UUID.class)));
        for(UUID id:candidates) {
            tx.executeWithoutResult(s->{
                int updated=db.update("UPDATE delivery_jobs SET status='QUEUED',next_attempt_at=NULL,updated_at=now() WHERE id=? AND status='RETRY_SCHEDULED' AND next_attempt_at <= now()",id);
                if(updated==1) db.update("INSERT INTO outbox_messages(id,delivery_job_id) VALUES (?,?)",UUID.randomUUID(),id);
            });
            try { redis.opsForZSet().remove(INDEX,id.toString()); } catch(Exception ignored) { }
        }
    }
}
