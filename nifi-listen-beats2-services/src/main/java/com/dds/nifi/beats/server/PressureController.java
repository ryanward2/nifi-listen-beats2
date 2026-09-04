/*
 * Copyright 2026 DDS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dds.nifi.beats.server;

import com.dds.nifi.beats.batch.BatchCoordinator;

import java.lang.ref.WeakReference;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Edge-triggered pressure propagation with bounded FIFO retry cohorts.
 *
 * <p>Global pressure transitions scan all channels once, but capacity recovery never wakes every
 * channel at once. Processing-capacity retries are checked against each connection's exact pending
 * byte/task requirement, preventing a thundering herd when only a small amount of capacity returns.</p>
 */
public final class PressureController implements AutoCloseable {
    private static final int RETRY_COHORT_SIZE = 64;
    private static final int MAX_QUEUE_EXAMINATIONS_PER_COHORT = RETRY_COHORT_SIZE * 4;
    private static final long RETRY_DELAY_MILLIS = 1L;
    private static final Set<PressureReason> COHORT_RETRY_REASONS = Set.of(
            PressureReason.ACCEPTED_MEMORY,
            PressureReason.READY_BATCH_CAPACITY,
            PressureReason.PROCESSING_CAPACITY,
            PressureReason.GLOBAL_PRESSURE);

    private final ConnectionRegistry registry;
    private final MemoryTracker memory;
    private final BatchCoordinator batches;
    private final ProcessorMetrics metrics;
    private final EnumMap<PressureReason, Set<ConnectionState>> suspended = new EnumMap<>(PressureReason.class);
    private final EnumMap<PressureReason, ConcurrentLinkedQueue<WeakReference<ConnectionState>>> retryQueues =
            new EnumMap<>(PressureReason.class);
    private final EnumMap<PressureReason, AtomicBoolean> retryScheduled = new EnumMap<>(PressureReason.class);
    private final AtomicBoolean global = new AtomicBoolean();
    private final AtomicBoolean cleanupPressure = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ScheduledExecutorService retryExecutor;
    private volatile BeatsServer server;

    public PressureController(
            final ConnectionRegistry registry,
            final MemoryTracker memory,
            final BatchCoordinator batches,
            final ProcessorMetrics metrics) {
        this.registry = registry;
        this.memory = memory;
        this.batches = batches;
        this.metrics = metrics;
        for (PressureReason reason : PressureReason.values()) {
            suspended.put(reason, ConcurrentHashMap.newKeySet());
            retryQueues.put(reason, new ConcurrentLinkedQueue<>());
            retryScheduled.put(reason, new AtomicBoolean());
        }
        retryExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "ListenBeats2-pressure-retry");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void attachServer(final BeatsServer server) {
        this.server = server;
    }

    public void suspend(final ConnectionState state, final PressureReason reason) {
        if (closed.get()) {
            return;
        }
        final Set<ConnectionState> states = suspended.get(reason);
        if (states.add(state) && COHORT_RETRY_REASONS.contains(reason)) {
            retryQueues.get(reason).offer(new WeakReference<>(state));
        }
        state.suspend(reason, metrics);
    }

    public void clear(final ConnectionState state, final PressureReason reason) {
        suspended.get(reason).remove(state);
        state.clear(reason);
    }

    /** Called on edge-triggered capacity changes; it does not scan all channels during steady state. */
    public void signal() {
        if (closed.get()) {
            return;
        }
        final BeatsServer currentServer = server;
        if (currentServer == null) {
            return;
        }

        final boolean cleanupNow = !currentServer.cleanupHasCapacity();
        if (cleanupNow && cleanupPressure.compareAndSet(false, true)) {
            // Preserve established clients. Cleanup saturation only stops new admissions.
            currentServer.pauseAccepts();
        } else if (!cleanupNow && cleanupPressure.compareAndSet(true, false) && !global.get()) {
            currentServer.resumeAccepts();
        }

        final boolean pressureNow = memory.highWaterReached()
                || !batches.hasReadyCapacity()
                || !currentServer.processingHasCapacity();
        if (pressureNow && global.compareAndSet(false, true)) {
            memory.enterGlobalPressure();
            currentServer.pauseAccepts();
            for (ConnectionState state : registry.states()) {
                suspend(state, PressureReason.GLOBAL_PRESSURE);
            }
            metrics.globalPressureTransitions.increment();
        }

        final boolean recovered = memory.belowLowWater()
                && batches.hasReadyCapacity()
                && currentServer.processingHasCapacity();
        if (recovered && global.compareAndSet(true, false)) {
            memory.clearGlobalPressure();
            if (!cleanupPressure.get()) {
                currentServer.resumeAccepts();
            }
            metrics.globalPressureRecoveries.increment();
        }

        if (memory.belowLowWater()) {
            scheduleRetries(PressureReason.ACCEPTED_MEMORY);
        }
        if (batches.hasReadyCapacity()) {
            scheduleRetries(PressureReason.READY_BATCH_CAPACITY);
        }
        if (currentServer.processingHasCapacity()) {
            scheduleRetries(PressureReason.PROCESSING_CAPACITY);
        }
        if (!global.get()) {
            scheduleRetries(PressureReason.GLOBAL_PRESSURE);
        }
    }

    public void outstandingCapacityReleased(final ConnectionState state) {
        final Set<ConnectionState> states = suspended.get(PressureReason.PER_CONNECTION_OUTSTANDING);
        if (states.remove(state)) {
            state.clear(PressureReason.PER_CONNECTION_OUTSTANDING);
            if (state.channel().isActive()) {
                metrics.pressureRetryAttempts.increment();
                // PENDING_WORK can still be set. Retry it first; that handler clears the final reason.
                state.requestRetry();
                state.resumeIfClear(metrics);
            }
        }
    }

    public void connectionClosed(final ConnectionState state) {
        for (Set<ConnectionState> states : suspended.values()) {
            states.remove(state);
        }
    }

    private void scheduleRetries(final PressureReason reason) {
        if (closed.get() || !COHORT_RETRY_REASONS.contains(reason)) {
            return;
        }
        if (retryQueues.get(reason).isEmpty() || !coarselyEligible(reason)) {
            return;
        }
        final AtomicBoolean scheduled = retryScheduled.get(reason);
        if (scheduled.compareAndSet(false, true)) {
            try {
                retryExecutor.execute(() -> drainRetryCohort(reason));
            } catch (RuntimeException rejected) {
                scheduled.set(false);
                if (!closed.get()) {
                    throw rejected;
                }
            }
        }
    }

    private void drainRetryCohort(final PressureReason reason) {
        if (closed.get()) {
            retryScheduled.get(reason).set(false);
            return;
        }
        final AtomicBoolean scheduled = retryScheduled.get(reason);
        boolean madeProgress = false;
        try {
            if (!coarselyEligible(reason)) {
                return;
            }

            final ConcurrentLinkedQueue<WeakReference<ConnectionState>> queue = retryQueues.get(reason);
            final Set<ConnectionState> states = suspended.get(reason);
            int resumed = 0;
            int examined = 0;

            while (resumed < RETRY_COHORT_SIZE
                    && examined < MAX_QUEUE_EXAMINATIONS_PER_COHORT
                    && coarselyEligible(reason)) {
                final WeakReference<ConnectionState> reference = queue.poll();
                if (reference == null) {
                    break;
                }
                examined++;
                final ConnectionState state = reference.get();
                if (state == null) {
                    continue;
                }

                if (!states.contains(state)) {
                    continue; // stale queue entry
                }
                if (!state.channel().isActive()) {
                    states.remove(state);
                    continue;
                }
                if (!eligible(state, reason)) {
                    // Preserve FIFO fairness without repeatedly spinning on one large blocked frame.
                    queue.offer(reference);
                    continue;
                }
                if (!states.remove(state)) {
                    continue;
                }

                state.clear(reason);
                resumed++;
                madeProgress = true;
                metrics.pressureRetryAttempts.increment();

                // Request work even when PENDING_WORK is still set. That handler is responsible for
                // clearing PENDING_WORK after the retained event/frame has transferred successfully.
                state.requestRetry();
                state.resumeIfClear(metrics);
            }

            if (resumed > 0) {
                metrics.pressureRetryCohorts.increment();
            }
        } finally {
            scheduled.set(false);
            // Self-schedule only after actual progress. If every queued connection still needs more
            // capacity, the next ProcessingTracker/MemoryTracker/BatchCoordinator release signal
            // will schedule another cohort and avoids a 1 ms busy loop.
            if (!closed.get() && madeProgress && !retryQueues.get(reason).isEmpty() && coarselyEligible(reason)
                    && scheduled.compareAndSet(false, true)) {
                try {
                    retryExecutor.schedule(
                            () -> drainRetryCohort(reason), RETRY_DELAY_MILLIS, TimeUnit.MILLISECONDS);
                } catch (RuntimeException rejected) {
                    scheduled.set(false);
                    if (!closed.get()) {
                        throw rejected;
                    }
                }
            }
        }
    }

    private boolean coarselyEligible(final PressureReason reason) {
        final BeatsServer currentServer = server;
        return switch (reason) {
            case ACCEPTED_MEMORY -> memory.belowLowWater();
            case READY_BATCH_CAPACITY -> batches.hasReadyCapacity();
            case PROCESSING_CAPACITY -> currentServer != null && currentServer.processingHasCapacity();
            case GLOBAL_PRESSURE -> !global.get();
            default -> true;
        };
    }

    private boolean eligible(final ConnectionState state, final PressureReason reason) {
        final BeatsServer currentServer = server;
        if (reason == PressureReason.PROCESSING_CAPACITY) {
            return currentServer != null && currentServer.processingCanReserve(
                    state.processingBytesNeeded(), state.processingTaskNeeded());
        }
        return coarselyEligible(reason);
    }

    int suspendedCount(final PressureReason reason) {
        return suspended.get(reason).size();
    }

    int retryQueueSize(final PressureReason reason) {
        return retryQueues.get(reason).size();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        retryExecutor.shutdownNow();
        try {
            retryExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (AtomicBoolean scheduled : retryScheduled.values()) {
            scheduled.set(false);
        }
        for (ConcurrentLinkedQueue<WeakReference<ConnectionState>> queue : retryQueues.values()) {
            queue.clear();
        }
        final Set<ConnectionState> affected = new HashSet<>();
        for (Set<ConnectionState> states : suspended.values()) {
            affected.addAll(states);
            states.clear();
        }
        for (ConnectionState state : affected) {
            if (state.clearAllPressure()) {
                metrics.readSuspendedChannels.updateAndGet(value -> Math.max(0L, value - 1L));
            }
        }
        server = null;
    }
}
