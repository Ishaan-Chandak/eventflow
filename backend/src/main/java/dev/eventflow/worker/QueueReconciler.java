package dev.eventflow.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

@Component
public class QueueReconciler {
    private final JdbcTemplate db;
    private final TransactionTemplate tx;
    private final boolean enabled;
    public QueueReconciler(JdbcTemplate db,TransactionTemplate tx,@Value("${eventflow.worker-enabled:true}") boolean enabled) {
        this.db=db;this.tx=tx;this.enabled=enabled;
    }

    @Scheduled(fixedDelay=10000)
    public void reconcile() {
        if(!enabled) return;
        tx.executeWithoutResult(s->{
            var ids=db.query("SELECT j.id FROM delivery_jobs j WHERE j.status='QUEUED' AND (j.capacity_deferred_until IS NULL OR j.capacity_deferred_until <= now()) AND j.updated_at < now()-interval '10 seconds' AND NOT EXISTS (SELECT 1 FROM outbox_messages o WHERE o.delivery_job_id=j.id AND o.published_at IS NULL) ORDER BY j.updated_at FOR UPDATE OF j SKIP LOCKED LIMIT 100",(rs,n)->rs.getObject(1,UUID.class));
            for(UUID id:ids) {
                db.update("UPDATE delivery_jobs SET updated_at=now() WHERE id=?",id);
                db.update("INSERT INTO outbox_messages(id,delivery_job_id) VALUES (?,?)",UUID.randomUUID(),id);
            }
        });
    }
}
