package dev.eventflow.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

@RestController
@RequestMapping("/api/v1")
public class ApiController {
    private final JdbcTemplate db;
    private final ObjectMapper json;
    private final Set<String> allowedHosts;

    public ApiController(JdbcTemplate db, ObjectMapper json, @Value("${eventflow.allowed-hosts}") String hosts) {
        this.db = db; this.json = json; this.allowedHosts = Set.of(hosts.split(","));
    }
    public record EndpointInput(@NotBlank String name, @NotBlank String url, @NotNull List<@NotBlank String> eventTypes, boolean enabled) {}
    public record EndpointPatch(String name, String url, List<String> eventTypes, Boolean enabled) {}
    public record EventInput(@NotBlank String eventType, @NotNull JsonNode payload) {}

    private void checkUrl(String url) {
        URI uri;
        try { uri = URI.create(url); } catch (Exception e) { throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid URL"); }
        if (!"http".equals(uri.getScheme()) || !allowedHosts.contains(uri.getHost()) || uri.getUserInfo() != null || uri.getFragment() != null)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "URL host is not allowed");
    }

    @PostMapping("/endpoints")
    public ResponseEntity<Map<String,Object>> endpoint(@Valid @RequestBody EndpointInput input) {
        checkUrl(input.url());
        UUID id = UUID.randomUUID();
        db.update("INSERT INTO webhook_endpoints(id,name,url,enabled,event_types) VALUES (?,?,?,?,?)", id, input.name(), input.url(), input.enabled(), input.eventTypes().toArray(String[]::new));
        return ResponseEntity.status(201).body(Map.of("id", id, "name", input.name(), "url", input.url(), "eventTypes", input.eventTypes(), "enabled", input.enabled()));
    }

    @GetMapping("/endpoints")
    public List<Map<String,Object>> endpoints(@RequestParam(defaultValue="0") int page, @RequestParam(defaultValue="20") int size) {
        int limit=Math.min(Math.max(size,1),100);
        return db.query("SELECT id,name,url,enabled,event_types,created_at FROM webhook_endpoints ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?",(rs,n)->endpointRow(rs.getObject("id",UUID.class),rs.getString("name"),rs.getString("url"),rs.getBoolean("enabled"),(String[])rs.getArray("event_types").getArray(),rs.getTimestamp("created_at").toInstant()),limit,Math.max(page,0)*limit);
    }
    private Map<String,Object> endpointRow(UUID id,String name,String url,boolean enabled,String[] eventTypes,java.time.Instant createdAt) {
        return Map.of("id",id,"name",name,"url",url,"enabled",enabled,"eventTypes",List.of(eventTypes),"createdAt",createdAt);
    }

    @PatchMapping("/endpoints/{id}")
    public Map<String,Object> patchEndpoint(@PathVariable UUID id,@RequestBody EndpointPatch patch) {
        if(patch.name()!=null && patch.name().isBlank()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Name cannot be blank");
        if(patch.url()!=null) checkUrl(patch.url());
        if(patch.eventTypes()!=null && patch.eventTypes().stream().anyMatch(x->x==null || x.isBlank())) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid event type");
        int changed=db.update("UPDATE webhook_endpoints SET name=COALESCE(?,name),url=COALESCE(?,url),enabled=COALESCE(?,enabled),event_types=COALESCE(?::text[],event_types),updated_at=now() WHERE id=?",patch.name(),patch.url(),patch.enabled(),patch.eventTypes()==null?null:patch.eventTypes().toArray(String[]::new),id);
        if(changed==0) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Endpoint not found");
        return db.query("SELECT id,name,url,enabled,event_types,created_at FROM webhook_endpoints WHERE id=?",(rs,n)->endpointRow(rs.getObject("id",UUID.class),rs.getString("name"),rs.getString("url"),rs.getBoolean("enabled"),(String[])rs.getArray("event_types").getArray(),rs.getTimestamp("created_at").toInstant()),id).getFirst();
    }

    @PostMapping("/events")
    @Transactional
    public ResponseEntity<Map<String,Object>> event(@Valid @RequestBody EventInput input, @RequestHeader(value="Idempotency-Key",required=false) String key) throws Exception {
        String payload = json.writeValueAsString(input.payload());
        String fingerprint = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest((input.eventType()+"\n"+payload).getBytes(StandardCharsets.UTF_8)));
        if (key != null && !key.isBlank()) {
            var old = db.queryForList("SELECT id,payload_sha256 FROM events WHERE idempotency_key=?", key);
            if (!old.isEmpty()) return replay(old.getFirst(), fingerprint);
        }
        UUID id = UUID.randomUUID(); List<UUID> jobs = new ArrayList<>();
        int inserted=db.update("INSERT INTO events(id,event_type,payload,idempotency_key,payload_sha256) VALUES (?,?,?::jsonb,?,?) ON CONFLICT (idempotency_key) DO NOTHING", id,input.eventType(),payload,key == null || key.isBlank() ? null : key,fingerprint);
        if(inserted==0) return replay(db.queryForMap("SELECT id,payload_sha256 FROM events WHERE idempotency_key=?",key),fingerprint);
        var endpoints = db.queryForList("SELECT id FROM webhook_endpoints WHERE enabled=true AND ? = ANY(event_types)",input.eventType());
        for (var endpoint : endpoints) {
            UUID job = UUID.randomUUID(); jobs.add(job);
            db.update("INSERT INTO delivery_jobs(id,event_id,endpoint_id,status) VALUES (?,?,?,'QUEUED')",job,id,endpoint.get("id"));
            db.update("INSERT INTO outbox_messages(id,delivery_job_id) VALUES (?,?)",UUID.randomUUID(),job);
        }
        return ResponseEntity.accepted().body(Map.of("eventId",id,"jobIds",jobs));
    }
    private ResponseEntity<Map<String,Object>> replay(Map<String,Object> old,String fingerprint) {
        if (!fingerprint.equals(old.get("payload_sha256"))) throw new ResponseStatusException(HttpStatus.CONFLICT,"Idempotency key has different content");
        UUID id=(UUID)old.get("id");
        List<UUID> jobs=db.query("SELECT id FROM delivery_jobs WHERE event_id=? ORDER BY created_at",(rs,n)->rs.getObject(1,UUID.class),id);
        return ResponseEntity.accepted().body(Map.of("eventId",id,"jobIds",jobs));
    }

    @GetMapping("/events")
    public Map<String,Object> events(@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size) {
        int limit=Math.min(Math.max(size,1),100), offset=Math.max(page,0)*limit;
        var rows=db.queryForList("SELECT e.id AS \"id\",e.event_type AS \"eventType\",e.created_at AS \"createdAt\",count(j.id) AS \"jobCount\" FROM events e LEFT JOIN delivery_jobs j ON j.event_id=e.id GROUP BY e.id ORDER BY e.created_at DESC,e.id DESC LIMIT ? OFFSET ?",limit,offset);
        Long total=db.queryForObject("SELECT count(*) FROM events",Long.class);
        return Map.of("items",rows,"total",total,"page",Math.max(page,0),"size",limit);
    }

    @GetMapping("/deliveries")
    public Map<String,Object> deliveries(@RequestParam(required=false) String status,@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size) {
        if(status!=null && !Set.of("QUEUED","IN_PROGRESS","RETRY_SCHEDULED","SUCCEEDED","DEAD").contains(status)) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid status");
        int limit=Math.min(Math.max(size,1),100), offset=Math.max(page,0)*limit;
        String where=status==null?"":" WHERE j.status=?";
        Object[] args=status==null?new Object[]{limit,offset}:new Object[]{status,limit,offset};
        var rows=db.queryForList("SELECT j.id AS \"id\",j.event_id AS \"eventId\",j.endpoint_id AS \"endpointId\",j.status,j.attempt_count AS \"attemptCount\",j.next_attempt_at AS \"nextAttemptAt\",j.last_http_status AS \"lastHttpStatus\",j.created_at AS \"createdAt\",e.event_type AS \"eventType\",p.name AS \"endpointName\" FROM delivery_jobs j JOIN events e ON e.id=j.event_id JOIN webhook_endpoints p ON p.id=j.endpoint_id"+where+" ORDER BY j.created_at DESC,j.id DESC LIMIT ? OFFSET ?",args);
        Long total=status==null?db.queryForObject("SELECT count(*) FROM delivery_jobs",Long.class):db.queryForObject("SELECT count(*) FROM delivery_jobs WHERE status=?",Long.class,status);
        Map<String,Long> summary=new LinkedHashMap<>();
        for(String value:List.of("QUEUED","IN_PROGRESS","RETRY_SCHEDULED","SUCCEEDED","DEAD")) summary.put(value,0L);
        db.query("SELECT status,count(*) AS count FROM delivery_jobs GROUP BY status",rs->{summary.put(rs.getString("status"),rs.getLong("count"));});
        return Map.of("items",rows,"total",total,"page",Math.max(page,0),"size",limit,"summary",summary);
    }

    @GetMapping("/deliveries/{id}")
    public Map<String,Object> delivery(@PathVariable UUID id) {
        var rows=db.queryForList("SELECT j.id,j.event_id AS \"eventId\",j.endpoint_id AS \"endpointId\",j.status,j.attempt_count AS \"attemptCount\",j.cycle_attempt_count AS \"cycleAttemptCount\",j.max_attempts AS \"maxAttempts\",j.next_attempt_at AS \"nextAttemptAt\",j.last_http_status AS \"lastHttpStatus\",j.last_error_code AS \"lastErrorCode\",j.created_at AS \"createdAt\",j.updated_at AS \"updatedAt\",e.event_type AS \"eventType\",p.name AS \"endpointName\",p.url AS \"endpointUrl\" FROM delivery_jobs j JOIN events e ON e.id=j.event_id JOIN webhook_endpoints p ON p.id=j.endpoint_id WHERE j.id=?",id);
        if(rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Delivery not found");
        Map<String,Object> detail=new LinkedHashMap<>(rows.getFirst());
        detail.put("attempts",db.queryForList("SELECT id,attempt_number AS \"attemptNumber\",started_at AS \"startedAt\",finished_at AS \"finishedAt\",outcome,http_status AS \"httpStatus\",duration_ms AS \"durationMs\",error_code AS \"errorCode\" FROM delivery_attempts WHERE delivery_job_id=? ORDER BY attempt_number",id));
        return detail;
    }

    @PostMapping("/deliveries/{id}/retry")
    @Transactional
    public ResponseEntity<Map<String,Object>> retry(@PathVariable UUID id) {
        int updated=db.update("UPDATE delivery_jobs SET status='QUEUED',cycle_attempt_count=0,next_attempt_at=NULL,last_error_code=NULL,updated_at=now() WHERE id=? AND status='DEAD'",id);
        if(updated==0) {
            Integer exists=db.queryForObject("SELECT count(*) FROM delivery_jobs WHERE id=?",Integer.class,id);
            throw new ResponseStatusException(exists!=null && exists>0?HttpStatus.CONFLICT:HttpStatus.NOT_FOUND,exists!=null && exists>0?"Only DEAD deliveries can be retried":"Delivery not found");
        }
        db.update("INSERT INTO outbox_messages(id,delivery_job_id) VALUES (?,?)",UUID.randomUUID(),id);
        return ResponseEntity.accepted().body(Map.of("id",id,"status","QUEUED"));
    }
}
