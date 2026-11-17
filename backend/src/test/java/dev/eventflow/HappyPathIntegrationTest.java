package dev.eventflow;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.stream.*;
import dev.eventflow.worker.DeliveryPipeline;
import dev.eventflow.worker.LeaseRecovery;
import dev.eventflow.worker.RetryScheduler;
import dev.eventflow.worker.QueueReconciler;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.DockerClientFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import com.sun.net.httpserver.HttpServer;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT)
class HappyPathIntegrationTest {
    @Container static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:16.9-alpine");
    @Container static GenericContainer<?> redis=new GenericContainer<>(DockerImageName.parse("redis:7.4.2-alpine")).withExposedPorts(6379);
    @DynamicPropertySource static void properties(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",postgres::getJdbcUrl);
        r.add("spring.datasource.username",postgres::getUsername);
        r.add("spring.datasource.password",postgres::getPassword);
        r.add("spring.data.redis.host",redis::getHost);
        r.add("spring.data.redis.port",()->redis.getMappedPort(6379));
        r.add("eventflow.allowed-hosts",()->"localhost");
        r.add("eventflow.retry-base-seconds",()->"1");
        r.add("eventflow.lease-seconds",()->"6");
        r.add("eventflow.pending-idle-seconds",()->"7");
    }
    @Autowired TestRestTemplate client;
    @Autowired JdbcTemplate db;
    @Autowired StringRedisTemplate redisTemplate;
    @Autowired DeliveryPipeline pipeline;
    @Autowired LeaseRecovery leaseRecovery;
    @Autowired RetryScheduler retryScheduler;
    @Autowired QueueReconciler queueReconciler;

    @Test void eventTravelsThroughOutboxStreamAndWorker() throws Exception {
        HttpServer receiver=HttpServer.create(new InetSocketAddress("localhost",0),0);
        receiver.createContext("/webhook", exchange->{exchange.getRequestBody().readAllBytes();exchange.sendResponseHeaders(200,-1);exchange.close();});
        receiver.start();
        try {
            var endpoint=client.postForEntity("/api/v1/endpoints",Map.of("name","test","url","http://localhost:"+receiver.getAddress().getPort()+"/webhook","eventTypes",new String[]{"order.created"},"enabled",true),Map.class);
            assertThat(endpoint.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            HttpHeaders headers=new HttpHeaders(); headers.setContentType(MediaType.APPLICATION_JSON);headers.set("Idempotency-Key","test-event-1");
            var response=client.postForEntity("/api/v1/events",new HttpEntity<>(Map.of("eventType","order.created","payload",Map.of("orderId","o-123")),headers),Map.class);
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            UUID id=UUID.fromString((String)((java.util.List<?>)response.getBody().get("jobIds")).getFirst());
            long until=System.currentTimeMillis()+15000;
            String status="";
            while(System.currentTimeMillis()<until) {
                status=db.queryForObject("SELECT status FROM delivery_jobs WHERE id=?",String.class,id);
                if("SUCCEEDED".equals(status)) break;
                Thread.sleep(200);
            }
            assertThat(status).isEqualTo("SUCCEEDED");
            assertThat(db.queryForObject("SELECT count(*) FROM delivery_attempts WHERE delivery_job_id=? AND outcome='SUCCESS'",Integer.class,id)).isEqualTo(1);
            assertThat(db.queryForObject("SELECT count(*) FROM outbox_messages WHERE delivery_job_id=? AND published_at IS NOT NULL",Integer.class,id)).isEqualTo(1);
        } finally { receiver.stop(0); }
    }

    @Test void serverFailureSchedulesRetryThenSucceeds() throws Exception {
        AtomicInteger status=new AtomicInteger(500);
        HttpServer receiver=receiver(status);
        try {
            UUID id=publish(receiver,"retry.created");
            awaitStatus(id,"RETRY_SCHEDULED",15000);
            assertThat(db.queryForObject("SELECT outcome FROM delivery_attempts WHERE delivery_job_id=?",String.class,id)).isEqualTo("TRANSIENT_FAILURE");
            redisTemplate.delete("eventflow:retry-due");
            status.set(200);
            awaitStatus(id,"SUCCEEDED",15000);
            assertThat(db.queryForObject("SELECT attempt_count FROM delivery_jobs WHERE id=?",Integer.class,id)).isGreaterThanOrEqualTo(2);
        } finally { receiver.stop(0); }
    }

    @Test void activeLeaseRejectsDuplicateDelivery() throws Exception {
        CountDownLatch entered=new CountDownLatch(1), release=new CountDownLatch(1);
        AtomicInteger requests=new AtomicInteger();
        HttpServer receiver=HttpServer.create(new InetSocketAddress("localhost",0),0);
        receiver.createContext("/webhook",exchange->{
            requests.incrementAndGet();entered.countDown();
            try { release.await(3,TimeUnit.SECONDS); } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
            exchange.getRequestBody().readAllBytes();exchange.sendResponseHeaders(200,-1);exchange.close();
        });
        receiver.start();
        try {
            UUID id=publish(receiver,"lease.created");
            assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();
            assertThat(db.queryForObject("SELECT status FROM delivery_jobs WHERE id=?",String.class,id)).isEqualTo("IN_PROGRESS");
            assertThat(pipeline.deliver(id)).isFalse();
            assertThat(requests.get()).isEqualTo(1);
            redisTemplate.opsForStream().add("eventflow:deliveries",Map.of("jobId",id.toString()));
            var abandoned=redisTemplate.opsForStream().read(Consumer.from("delivery-workers","abandoned-worker"),StreamReadOptions.empty().count(1),StreamOffset.create("eventflow:deliveries",ReadOffset.lastConsumed()));
            assertThat(abandoned).hasSize(1);
            release.countDown();
            awaitStatus(id,"SUCCEEDED",15000);
            assertThat(requests.get()).isEqualTo(1);
            Thread.sleep(7500);
            pipeline.reclaimPending();
            assertThat(redisTemplate.opsForStream().pending("eventflow:deliveries","delivery-workers").getTotalPendingMessages()).isZero();
        } finally { release.countDown();receiver.stop(0); }
    }

    @Test void expiredLeaseMarksAttemptUnknownAndRetries() throws Exception {
        AtomicInteger status=new AtomicInteger(200);
        HttpServer receiver=receiver(status);
        try {
            UUID id=publish(receiver,"expired.created");
            awaitStatus(id,"SUCCEEDED",15000);
            UUID orphan=UUID.randomUUID();
            db.update("UPDATE delivery_jobs SET status='IN_PROGRESS',attempt_count=2,cycle_attempt_count=2,lease_owner='crashed-worker',lease_expires_at=now()-interval '1 second' WHERE id=?",id);
            db.update("INSERT INTO delivery_attempts(id,delivery_job_id,attempt_number,started_at) VALUES (?,?,2,now()-interval '20 seconds')",orphan,id);
            leaseRecovery.recover();
            assertThat(db.queryForObject("SELECT outcome FROM delivery_attempts WHERE id=?",String.class,orphan)).isEqualTo("UNKNOWN");
            awaitStatus(id,"SUCCEEDED",15000);
            assertThat(db.queryForObject("SELECT attempt_count FROM delivery_jobs WHERE id=?",Integer.class,id)).isEqualTo(3);
        } finally { receiver.stop(0); }
    }

    @Test void competingSchedulersCreateOneOutboxTransition() throws Exception {
        AtomicInteger status=new AtomicInteger(200);
        HttpServer receiver=receiver(status);
        try {
            UUID id=publish(receiver,"scheduler.created");
            awaitStatus(id,"SUCCEEDED",15000);
            int before=db.queryForObject("SELECT count(*) FROM outbox_messages WHERE delivery_job_id=?",Integer.class,id);
            db.update("UPDATE delivery_jobs SET status='RETRY_SCHEDULED',next_attempt_at=now()-interval '1 second' WHERE id=?",id);
            try(var pool=Executors.newFixedThreadPool(2)) {
                var first=pool.submit(retryScheduler::schedule);
                var second=pool.submit(retryScheduler::schedule);
                first.get(5,TimeUnit.SECONDS);second.get(5,TimeUnit.SECONDS);
            }
            int after=db.queryForObject("SELECT count(*) FROM outbox_messages WHERE delivery_job_id=?",Integer.class,id);
            assertThat(after-before).isEqualTo(1);
        } finally { receiver.stop(0); }
    }

    @Test void queuedJobWithoutStreamMessageGetsRepublished() throws Exception {
        AtomicInteger status=new AtomicInteger(200);
        HttpServer receiver=receiver(status);
        try {
            UUID id=publish(receiver,"reconcile.created");
            awaitStatus(id,"SUCCEEDED",15000);
            int before=db.queryForObject("SELECT count(*) FROM outbox_messages WHERE delivery_job_id=?",Integer.class,id);
            db.update("UPDATE delivery_jobs SET status='QUEUED',updated_at=now()-interval '20 seconds' WHERE id=?",id);
            queueReconciler.reconcile();
            int after=db.queryForObject("SELECT count(*) FROM outbox_messages WHERE delivery_job_id=?",Integer.class,id);
            assertThat(after-before).isEqualTo(1);
            awaitStatus(id,"SUCCEEDED",15000);
        } finally { receiver.stop(0); }
    }

    @Test void redisOutageDoesNotLoseAcceptedEvent() throws Exception {
        AtomicInteger status=new AtomicInteger(200);
        HttpServer receiver=receiver(status);
        var docker=DockerClientFactory.instance().client();
        docker.pauseContainerCmd(redis.getContainerId()).exec();
        UUID id;
        try {
            id=publish(receiver,"outage.created");
            assertThat(db.queryForObject("SELECT status FROM delivery_jobs WHERE id=?",String.class,id)).isEqualTo("QUEUED");
            assertThat(db.queryForObject("SELECT count(*) FROM outbox_messages WHERE delivery_job_id=? AND published_at IS NULL",Integer.class,id)).isEqualTo(1);
        } finally {
            docker.unpauseContainerCmd(redis.getContainerId()).exec();
        }
        try { awaitStatus(id,"SUCCEEDED",45000); }
        finally { receiver.stop(0); }
    }

    @Test void dashboardApisListAndEditEndpoints() {
        String type="dashboard."+UUID.randomUUID();
        var created=client.postForEntity("/api/v1/endpoints",Map.of("name","Before","url","http://localhost:8082/webhook","eventTypes",new String[]{type},"enabled",true),Map.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String endpointId=(String)created.getBody().get("id");
        client.exchange("/api/v1/endpoints/"+endpointId,HttpMethod.PATCH,new HttpEntity<>(Map.of("name","After","enabled",false)),Map.class);
        var endpoints=client.getForEntity("/api/v1/endpoints",java.util.List.class);
        assertThat(endpoints.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(endpoints.getBody().toString()).contains("After").contains(type);
        var event=client.postForEntity("/api/v1/events",Map.of("eventType",type,"payload",Map.of("id","disabled")),Map.class);
        assertThat(event.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat((java.util.List<?>)event.getBody().get("jobIds")).isEmpty();
        var events=client.getForEntity("/api/v1/events",Map.class);
        assertThat(events.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(events.getBody()).containsKeys("items","total","page","size");
        var deliveries=client.getForEntity("/api/v1/deliveries?status=SUCCEEDED",Map.class);
        assertThat(deliveries.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(deliveries.getBody()).containsKeys("items","summary","total");
    }

    @Test void clientFailureIsDeadAndManualRetryStartsNewCycle() throws Exception {
        AtomicInteger status=new AtomicInteger(400);
        HttpServer receiver=receiver(status);
        try {
            UUID id=publish(receiver,"manual.created");
            awaitStatus(id,"DEAD",15000);
            assertThat(db.queryForObject("SELECT outcome FROM delivery_attempts WHERE delivery_job_id=?",String.class,id)).isEqualTo("PERMANENT_FAILURE");
            status.set(200);
            var retry=client.postForEntity("/api/v1/deliveries/"+id+"/retry",null,Map.class);
            assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            awaitStatus(id,"SUCCEEDED",15000);
            assertThat(db.queryForObject("SELECT attempt_count FROM delivery_jobs WHERE id=?",Integer.class,id)).isEqualTo(2);
            assertThat(db.queryForObject("SELECT cycle_attempt_count FROM delivery_jobs WHERE id=?",Integer.class,id)).isEqualTo(1);
            assertThat(client.postForEntity("/api/v1/deliveries/"+id+"/retry",null,Map.class).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        } finally { receiver.stop(0); }
    }

    private HttpServer receiver(AtomicInteger status) throws Exception {
        HttpServer server=HttpServer.create(new InetSocketAddress("localhost",0),0);
        server.createContext("/webhook", exchange->{exchange.getRequestBody().readAllBytes();exchange.sendResponseHeaders(status.get(),-1);exchange.close();});
        server.start();
        return server;
    }

    private UUID publish(HttpServer receiver,String type) {
        var endpoint=client.postForEntity("/api/v1/endpoints",Map.of("name",type,"url","http://localhost:"+receiver.getAddress().getPort()+"/webhook","eventTypes",new String[]{type},"enabled",true),Map.class);
        assertThat(endpoint.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        var event=client.postForEntity("/api/v1/events",Map.of("eventType",type,"payload",Map.of("id",UUID.randomUUID().toString())),Map.class);
        assertThat(event.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return UUID.fromString((String)((java.util.List<?>)event.getBody().get("jobIds")).getFirst());
    }

    private void awaitStatus(UUID id,String expected,long timeoutMs) throws Exception {
        long until=System.currentTimeMillis()+timeoutMs;
        String actual="";
        while(System.currentTimeMillis()<until) {
            actual=db.queryForObject("SELECT status FROM delivery_jobs WHERE id=?",String.class,id);
            if(expected.equals(actual)) return;
            Thread.sleep(100);
        }
        assertThat(actual).isEqualTo(expected);
    }
}
