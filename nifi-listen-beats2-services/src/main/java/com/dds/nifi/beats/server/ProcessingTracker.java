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

import com.dds.nifi.beats.protocol.ProcessingLease;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Global bound for partial-frame bytes and frames queued or executing on event-processing workers.
 * Byte capacity can be reserved as soon as a frame header arrives, before a worker task is needed.
 */
public final class ProcessingTracker {
    private final long maximumTasks;
    private final long maximumBytes;
    private final AtomicLong tasks = new AtomicLong();
    private final AtomicLong bytes = new AtomicLong();
    private volatile Runnable capacityListener = () -> { };

    public ProcessingTracker(final long maximumTasks, final long maximumBytes) {
        if (maximumTasks <= 0 || maximumBytes <= 0) {
            throw new IllegalArgumentException("Processing task and byte limits must be positive");
        }
        this.maximumTasks = maximumTasks;
        this.maximumBytes = maximumBytes;
    }

    public void capacityListener(final Runnable listener) {
        capacityListener = listener == null ? () -> { } : listener;
    }

    /** Reserves bytes only. The task slot is acquired when the complete frame is ready for offload. */
    public Reservation tryReserveBytes(final long initialBytes) {
        if (initialBytes < 0 || initialBytes > maximumBytes || !tryAddBytes(initialBytes)) {
            return null;
        }
        return new Reservation(this, initialBytes, false);
    }

    /** Convenience for complete work that needs both bytes and a worker-task slot immediately. */
    public Reservation tryReserve(final long initialBytes) {
        final Reservation reservation = tryReserveBytes(initialBytes);
        if (reservation == null) {
            return null;
        }
        if (!reservation.tryAcquireTask()) {
            reservation.release();
            return null;
        }
        return reservation;
    }

    private boolean tryAcquireTask() {
        while (true) {
            final long current = tasks.get();
            if (current >= maximumTasks) {
                return false;
            }
            if (tasks.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private boolean tryAddBytes(final long additional) {
        while (true) {
            final long current = bytes.get();
            if (additional > maximumBytes - current) {
                return false;
            }
            if (bytes.compareAndSet(current, current + additional)) {
                return true;
            }
        }
    }

    private void remove(final long reservedBytes, final boolean taskAcquired) {
        final long updatedBytes = bytes.addAndGet(-reservedBytes);
        if (updatedBytes < 0) {
            bytes.addAndGet(reservedBytes);
            throw new IllegalStateException("Processing byte reservation underflow");
        }
        if (taskAcquired) {
            final long updatedTasks = tasks.decrementAndGet();
            if (updatedTasks < 0) {
                tasks.incrementAndGet();
                throw new IllegalStateException("Processing task reservation underflow");
            }
        }
        capacityListener.run();
    }

    public boolean hasCapacity() {
        return tasks.get() < maximumTasks && bytes.get() < maximumBytes;
    }

    public boolean hasTaskCapacity() {
        return tasks.get() < maximumTasks;
    }

    public long availableBytes() {
        return Math.max(0L, maximumBytes - bytes.get());
    }

    public boolean canReserve(final long requestedBytes, final boolean taskRequired) {
        return requestedBytes <= availableBytes() && (!taskRequired || hasTaskCapacity());
    }

    public long tasks() { return tasks.get(); }
    public long bytes() { return bytes.get(); }
    public long maximumTasks() { return maximumTasks; }
    public long maximumBytes() { return maximumBytes; }

    public static final class Reservation implements ProcessingLease {
        private final ProcessingTracker tracker;
        private long reservedBytes;
        private boolean taskAcquired;
        private boolean released;

        private Reservation(
                final ProcessingTracker tracker,
                final long reservedBytes,
                final boolean taskAcquired) {
            this.tracker = tracker;
            this.reservedBytes = reservedBytes;
            this.taskAcquired = taskAcquired;
        }

        /** Acquires the bounded worker-task slot without changing the existing byte reservation. */
        public synchronized boolean tryAcquireTask() {
            if (released) {
                return false;
            }
            if (taskAcquired) {
                return true;
            }
            if (!tracker.tryAcquireTask()) {
                return false;
            }
            taskAcquired = true;
            return true;
        }

        public synchronized boolean taskAcquired() {
            return taskAcquired;
        }

        public synchronized boolean grow(final long additionalBytes) {
            if (released) {
                return false;
            }
            if (additionalBytes <= 0) {
                return true;
            }
            if (!tracker.tryAddBytes(additionalBytes)) {
                return false;
            }
            reservedBytes += additionalBytes;
            return true;
        }

        public synchronized void shrinkTo(final long targetBytes) {
            if (released) {
                return;
            }
            if (targetBytes < 0 || targetBytes > reservedBytes) {
                throw new IllegalArgumentException("Target reservation must be between 0 and current reservation");
            }
            final long releasedBytes = reservedBytes - targetBytes;
            if (releasedBytes > 0) {
                tracker.bytes.addAndGet(-releasedBytes);
                reservedBytes = targetBytes;
                tracker.capacityListener.run();
            }
        }

        public synchronized long reservedBytes() {
            return reservedBytes;
        }

        @Override
        public synchronized void release() {
            if (!released) {
                released = true;
                tracker.remove(reservedBytes, taskAcquired);
            }
        }
    }
}
