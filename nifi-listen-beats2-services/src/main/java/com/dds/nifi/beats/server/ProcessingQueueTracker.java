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

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.SingleThreadEventExecutor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Submission-aware per-worker processing queue pressure with periodic reconciliation. */
public final class ProcessingQueueTracker {
    private final long workerHigh;
    private final long workerLow;
    private final long globalHigh;
    private final long globalLow;
    private final ProcessorMetrics metrics;
    private final Runnable pressureChanged;
    private final Map<EventExecutor, AtomicLong> workers = new ConcurrentHashMap<>();
    private final AtomicLong globalOutstanding = new AtomicLong();
    private final AtomicBoolean pressured = new AtomicBoolean();

    public ProcessingQueueTracker(
            final EventExecutorGroup group,
            final int queueCapacityPerWorker,
            final int highWatermarkPercent,
            final int lowWatermarkPercent,
            final ProcessorMetrics metrics,
            final Runnable pressureChanged) {
        if (queueCapacityPerWorker <= 0) {
            throw new IllegalArgumentException("Processing queue capacity must be positive");
        }
        this.metrics = metrics;
        this.pressureChanged = pressureChanged == null ? () -> { } : pressureChanged;
        int workerCount = 0;
        for (EventExecutor executor : group) {
            workers.put(executor, new AtomicLong());
            workerCount++;
        }
        final long perWorkerCapacity = Math.addExact((long) queueCapacityPerWorker, 1L);
        final long globalCapacity = Math.multiplyExact(Math.max(1L, workerCount), perWorkerCapacity);
        workerHigh = threshold(perWorkerCapacity, highWatermarkPercent);
        workerLow = threshold(perWorkerCapacity, lowWatermarkPercent);
        globalHigh = threshold(globalCapacity, highWatermarkPercent);
        globalLow = threshold(globalCapacity, lowWatermarkPercent);
    }

    public void beforeSubmission(final EventExecutor executor) {
        final AtomicLong worker = workers.computeIfAbsent(executor, ignored -> new AtomicLong());
        final long workerCount = worker.incrementAndGet();
        final long globalCount = globalOutstanding.incrementAndGet();
        publish(globalCount, maximumWorkerOutstanding());
        if (globalCount >= globalHigh || workerCount >= workerHigh) {
            enterPressure();
        }
    }

    public void submissionRejected(final EventExecutor executor) {
        completed(executor);
    }

    public void taskCompleted(final EventExecutor executor) {
        completed(executor);
    }

    /** Reconciles tracker counters with actual Netty executor queue depth. */
    public void reconcile(final EventExecutorGroup group) {
        long totalPending = 0L;
        long hottest = 0L;
        for (EventExecutor executor : group) {
            if (executor instanceof SingleThreadEventExecutor single) {
                final long pending = single.pendingTasks();
                totalPending += pending;
                hottest = Math.max(hottest, pending);
            }
        }
        metrics.processingExecutorPendingTasks.set(totalPending);
        metrics.processingExecutorHottestQueue.set(hottest);
        metrics.processingExecutorHottestQueueMaximum.accumulateAndGet(hottest, Math::max);
        if (totalPending >= globalHigh || hottest >= workerHigh) {
            enterPressure();
        } else if (totalPending <= globalLow && hottest <= workerLow) {
            leavePressure();
        }
    }

    public boolean pressured() {
        return pressured.get();
    }

    private void completed(final EventExecutor executor) {
        final AtomicLong worker = workers.get(executor);
        if (worker == null) {
            return;
        }
        final long workerCount = worker.decrementAndGet();
        final long globalCount = globalOutstanding.decrementAndGet();
        if (workerCount < 0L || globalCount < 0L) {
            worker.incrementAndGet();
            globalOutstanding.incrementAndGet();
            throw new IllegalStateException("Processing queue accounting underflow");
        }
        final long hottest = maximumWorkerOutstanding();
        publish(globalCount, hottest);
        if (globalCount <= globalLow && hottest <= workerLow) {
            leavePressure();
        }
    }

    private void publish(final long total, final long hottest) {
        metrics.processingExecutorPendingTasks.set(total);
        metrics.processingExecutorHottestQueue.set(hottest);
        metrics.processingExecutorHottestQueueMaximum.accumulateAndGet(hottest, Math::max);
    }

    private long maximumWorkerOutstanding() {
        long maximum = 0L;
        for (AtomicLong worker : workers.values()) {
            maximum = Math.max(maximum, worker.get());
        }
        return maximum;
    }

    private void enterPressure() {
        if (pressured.compareAndSet(false, true)) {
            metrics.processingExecutorPressureTransitions.increment();
            pressureChanged.run();
        }
    }

    private void leavePressure() {
        if (pressured.compareAndSet(true, false)) {
            metrics.processingExecutorPressureRecoveries.increment();
            pressureChanged.run();
        }
    }

    private static long threshold(final long maximum, final int percent) {
        return Math.max(1L, (maximum * percent + 99L) / 100L);
    }
}
