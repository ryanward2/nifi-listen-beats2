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

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

/** One lightweight periodic lag probe per Netty socket event loop. */
public final class EventLoopLagMonitor implements AutoCloseable {
    private final long intervalNanos;
    private final ProcessorMetrics metrics;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<Probe> probes;
    private final AtomicLongArray latestLagNanos;

    public EventLoopLagMonitor(
            final EventLoopGroup eventLoopGroup,
            final Duration interval,
            final ProcessorMetrics metrics) {
        this.intervalNanos = interval.toNanos();
        if (intervalNanos <= 0L) {
            throw new IllegalArgumentException("Event-loop lag interval must be positive");
        }
        this.metrics = metrics;
        final List<EventExecutor> executors = new ArrayList<>();
        for (EventExecutor executor : eventLoopGroup) {
            executors.add(executor);
        }
        latestLagNanos = new AtomicLongArray(Math.max(1, executors.size()));
        probes = new ArrayList<>(executors.size());
        for (int index = 0; index < executors.size(); index++) {
            final Probe probe = new Probe(executors.get(index), index);
            probes.add(probe);
            probe.schedule();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (Probe probe : probes) {
            probe.cancel();
        }
    }

    private final class Probe implements Runnable {
        private final EventExecutor executor;
        private final int slot;
        private volatile ScheduledFuture<?> scheduled;
        private long expectedNanos;

        private Probe(final EventExecutor executor, final int slot) {
            this.executor = executor;
            this.slot = slot;
        }

        private void schedule() {
            if (closed.get()) {
                return;
            }
            expectedNanos = System.nanoTime() + intervalNanos;
            scheduled = executor.schedule(this, intervalNanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public void run() {
            if (closed.get()) {
                return;
            }
            final long observed = Math.max(0L, System.nanoTime() - expectedNanos);
            latestLagNanos.set(slot, observed);
            long currentMaximum = 0L;
            for (int index = 0; index < latestLagNanos.length(); index++) {
                currentMaximum = Math.max(currentMaximum, latestLagNanos.get(index));
            }
            metrics.recordEventLoopLag(currentMaximum, observed);
            schedule();
        }

        private void cancel() {
            final ScheduledFuture<?> current = scheduled;
            if (current != null) {
                current.cancel(false);
            }
        }
    }
}
