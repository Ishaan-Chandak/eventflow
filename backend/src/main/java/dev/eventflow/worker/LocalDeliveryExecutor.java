package dev.eventflow.worker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class LocalDeliveryExecutor {
    private final int capacity;
    private final Duration shutdownGrace;
    private final Semaphore available;
    private final ThreadPoolExecutor executor;
    private final Counter rejections;
    private final Counter saturation;
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    public LocalDeliveryExecutor(DeliveryProperties properties, MeterRegistry registry) {
        capacity = properties.getLocalConcurrency();
        shutdownGrace = properties.getShutdownGrace();
        available = new Semaphore(capacity);
        var threadNumber = new AtomicInteger();
        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(task, "eventflow-delivery-" + threadNumber.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
        executor = new ThreadPoolExecutor(capacity, capacity, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(capacity), threadFactory, new ThreadPoolExecutor.AbortPolicy());
        executor.prestartAllCoreThreads();
        rejections = rejectionCounter(registry);
        saturation = counter(registry, "eventflow.delivery.executor.saturation");
        try {
            registry.gauge("eventflow.delivery.tasks.active", executor, ThreadPoolExecutor::getActiveCount);
            registry.gauge("eventflow.delivery.capacity.utilization", this,
                    value -> (double) (value.capacity - value.available.availablePermits()) / value.capacity);
        } catch (RuntimeException ignored) { }
    }

    public List<Permit> reserve(int requested) {
        if (requested <= 0 || !accepting.get()) return List.of();
        int target = Math.min(requested, capacity);
        List<Permit> permits = new ArrayList<>(target);
        while (permits.size() < target && accepting.get() && available.tryAcquire()) {
            permits.add(new Permit());
        }
        if (permits.size() < target && accepting.get()) {
            try { saturation.increment(); } catch (RuntimeException ignored) { }
        }
        boolean stillAccepting = accepting.get();
        if (!stillAccepting) permits.forEach(Permit::close);
        return stillAccepting ? permits : List.of();
    }

    public int availableCapacity() { return available.availablePermits(); }
    public boolean isAccepting() { return accepting.get(); }

    @PreDestroy
    public void stop() {
        if (!accepting.compareAndSet(true, false)) return;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(shutdownGrace.toMillis(), TimeUnit.MILLISECONDS)) {
                cancel(executor.shutdownNow());
            }
        } catch (InterruptedException e) {
            cancel(executor.shutdownNow());
            Thread.currentThread().interrupt();
        }
    }

    private void cancel(List<Runnable> tasks) {
        tasks.forEach(task -> {
            if (task instanceof DeliveryTask deliveryTask) deliveryTask.cancel();
        });
    }

    private static Counter rejectionCounter(MeterRegistry registry) {
        return counter(registry, "eventflow.delivery.executor.rejections");
    }

    private static Counter counter(MeterRegistry registry, String name) {
        try { return registry.counter(name); }
        catch (RuntimeException ignored) {
            return new SimpleMeterRegistry().counter(name);
        }
    }

    public final class Permit implements AutoCloseable {
        private final AtomicBoolean owned = new AtomicBoolean(true);

        public boolean submit(Runnable task) {
            return submit(task, () -> { });
        }

        public boolean submit(Runnable task, Runnable afterCapacityRelease) {
            if (!owned.compareAndSet(true, false)) throw new IllegalStateException("permit already used");
            try {
                executor.execute(new DeliveryTask(task, afterCapacityRelease));
                return true;
            } catch (RejectedExecutionException e) {
                available.release();
                try { rejections.increment(); } catch (RuntimeException ignored) { }
                return false;
            }
        }

        @Override
        public void close() {
            if (owned.compareAndSet(true, false)) available.release();
        }
    }

    private final class DeliveryTask implements Runnable {
        private final Runnable delegate;
        private final Runnable afterCapacityRelease;
        private final AtomicBoolean capacityOwned = new AtomicBoolean(true);

        private DeliveryTask(Runnable delegate, Runnable afterCapacityRelease) {
            this.delegate = delegate;
            this.afterCapacityRelease = afterCapacityRelease;
        }

        @Override
        public void run() {
            try { delegate.run(); }
            finally {
                releaseCapacity();
                try { afterCapacityRelease.run(); } catch (RuntimeException ignored) { }
            }
        }

        private void cancel() { releaseCapacity(); }
        private void releaseCapacity() {
            if (capacityOwned.compareAndSet(true, false)) available.release();
        }
    }
}
