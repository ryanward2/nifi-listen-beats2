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

package com.dds.nifi.beats.batch;

import com.dds.nifi.beats.model.BeatsEvent;
import com.dds.nifi.beats.server.MemoryTracker;
import com.dds.nifi.beats.server.ProcessorMetrics;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/** Fixed-partition batch coordinator with per-partition locks, deadline heaps, and pooled NDJSON chunks. */
public final class BatchCoordinator {
    private final BatchConfig config;
    private final BatchKeyStrategy keyStrategy;
    private final List<Shard> shards;
    private final LinkedBlockingDeque<ReadyBatch> ready;
    private final Semaphore readySlots;
    private final AtomicInteger activeCount = new AtomicInteger();
    private final AtomicInteger evictionCursor = new AtomicInteger();
    private final ProcessorMetrics metrics;
    private final ByteBufAllocator allocator;
    private volatile Runnable capacityListener = () -> { };

    public BatchCoordinator(final BatchConfig config, final ProcessorMetrics metrics) {
        this(config, metrics, PooledByteBufAllocator.DEFAULT);
    }

    public BatchCoordinator(
            final BatchConfig config,
            final ProcessorMetrics metrics,
            final ByteBufAllocator allocator) {
        this.config = config;
        this.metrics = metrics;
        this.allocator = allocator;
        this.keyStrategy = Strategies.create(
                config.strategy(), config.jsonPointers(), config.missingKeyPolicy(), config.defaultBucket(),
                config.maximumKeyLength(), config.hashKeys());
        final int partitionCount = normalizePartitions(config.partitions());
        final List<Shard> created = new ArrayList<>(partitionCount);
        for (int index = 0; index < partitionCount; index++) {
            created.add(new Shard());
        }
        this.shards = List.copyOf(created);
        this.ready = new LinkedBlockingDeque<>(config.maximumReadyBatches());
        this.readySlots = new Semaphore(config.maximumReadyBatches());
    }

    public void capacityListener(final Runnable listener) {
        capacityListener = listener == null ? () -> { } : listener;
    }

    /**
     * Appends and consumes the event only after a batch/key slot is available. On success, the
     * transient event payload is copied into pooled NDJSON chunks and released immediately.
     */
    public AppendResult append(final BeatsEvent event) throws Exception {
        final BatchKey key = keyStrategy.extract(event);
        final Shard shard = shard(key);
        final long now = System.nanoTime();

        for (;;) {
            shard.lock.lock();
            try {
                InternalBatch batch = shard.active.get(key);
                if (batch == null) {
                    if (!reserveActiveSlot()) {
                        if (!flushOneOldest("active-key-limit")) {
                            return AppendResult.DEFERRED_CAPACITY;
                        }
                        continue;
                    }
                    try {
                        batch = new InternalBatch(key, now, allocator, config.maximumBytes());
                        shard.active.put(key, batch);
                    } catch (RuntimeException | Error failure) {
                        activeCount.decrementAndGet();
                        throw failure;
                    }
                }

                final int eventBytes = event.payload().length();
                if (batch.eventCount() > 0
                        && eventBytes > config.maximumBytes() - batch.payloadBytes()) {
                    if (!flushLocked(shard, key, batch, "byte-count")) {
                        return AppendResult.DEFERRED_CAPACITY;
                    }
                    // The event remains owned by the caller. Retry into a new empty batch so the
                    // configured byte limit is hard except for one individually oversized event.
                    continue;
                }

                batch.append(event, now);
                scheduleLocked(shard, batch);

                final boolean flushRequired = config.strategy() == BatchingStrategy.NONE
                        || (config.strategy() == BatchingStrategy.WINDOW && event.windowComplete())
                        || batch.eventCount() >= config.maximumEvents()
                        || batch.payloadBytes() >= config.maximumBytes()
                        || now - batch.createdNanos() >= config.maximumAge().toNanos();
                if (!flushRequired) {
                    return AppendResult.ACCEPTED;
                }

                final String reason = config.strategy() == BatchingStrategy.NONE ? "none"
                        : config.strategy() == BatchingStrategy.WINDOW && event.windowComplete() ? "window-boundary"
                        : batch.eventCount() >= config.maximumEvents() ? "event-count"
                        : batch.payloadBytes() >= config.maximumBytes() ? "byte-count" : "maximum-age";
                return flushLocked(shard, key, batch, reason)
                        ? AppendResult.ACCEPTED
                        : AppendResult.ACCEPTED_PRESSURE;
            } finally {
                shard.lock.unlock();
            }
        }
    }

    /** Processes only due heap entries; it never scans all active keys. */
    public void flushExpired() {
        final long now = System.nanoTime();
        for (Shard shard : shards) {
            shard.lock.lock();
            try {
                while (true) {
                    final Deadline deadline = shard.deadlines.isEmpty() ? null : shard.deadlines.first();
                    if (deadline == null || deadline.deadlineNanos() > now) {
                        break;
                    }
                    shard.deadlines.pollFirst();
                    shard.scheduled.remove(deadline.key(), deadline);
                    final InternalBatch batch = shard.active.get(deadline.key());
                    if (batch == null || batch.generation() != deadline.generation()) {
                        continue;
                    }
                    final boolean age = now - batch.createdNanos() >= config.maximumAge().toNanos();
                    final boolean idle = now - batch.lastAppendNanos() >= config.maximumIdle().toNanos();
                    if (!age && !idle) {
                        scheduleLocked(shard, batch);
                        continue;
                    }
                    if (!flushLocked(shard, batch.key(), batch, age ? "maximum-age" : "idle")) {
                        scheduleLocked(shard, batch, now + 1_000_000L);
                        break;
                    }
                }
            } finally {
                shard.lock.unlock();
            }
        }
    }

    public void flushAll(final String reason) {
        for (Shard shard : shards) {
            shard.lock.lock();
            try {
                final List<Map.Entry<BatchKey, InternalBatch>> entries = new ArrayList<>(shard.active.entrySet());
                for (Map.Entry<BatchKey, InternalBatch> entry : entries) {
                    if (!flushLocked(shard, entry.getKey(), entry.getValue(), reason)) {
                        break;
                    }
                }
            } finally {
                shard.lock.unlock();
            }
        }
    }

    private boolean flushOneOldest(final String reason) {
        final int start = Math.floorMod(evictionCursor.getAndIncrement(), shards.size());
        final List<Shard> locked = new ArrayList<>(shards.size());
        Shard selectedShard = null;
        Map.Entry<BatchKey, InternalBatch> selected = null;
        try {
            for (int offset = 0; offset < shards.size(); offset++) {
                final Shard shard = shards.get((start + offset) & (shards.size() - 1));
                if (!shard.lock.tryLock()) {
                    continue;
                }
                locked.add(shard);
                final var iterator = shard.active.entrySet().iterator();
                if (!iterator.hasNext()) {
                    continue;
                }
                final Map.Entry<BatchKey, InternalBatch> candidate = iterator.next();
                if (selected == null
                        || candidate.getValue().createdNanos() < selected.getValue().createdNanos()) {
                    selectedShard = shard;
                    selected = candidate;
                }
            }
            if (selected == null) {
                return false;
            }
            final boolean flushed = flushLocked(
                    selectedShard, selected.getKey(), selected.getValue(), reason);
            if (flushed) {
                metrics.batchFlushActiveKeyLimit.increment();
            }
            return flushed;
        } finally {
            for (int index = locked.size() - 1; index >= 0; index--) {
                locked.get(index).lock.unlock();
            }
        }
    }

    private boolean flushLocked(
            final Shard shard,
            final BatchKey key,
            final InternalBatch batch,
            final String reason) {
        if (!readySlots.tryAcquire()) {
            capacityListener.run();
            return false;
        }
        final ReadyBatch readyBatch;
        try {
            readyBatch = toReady(batch, reason);
        } catch (RuntimeException e) {
            readySlots.release();
            throw e;
        }
        if (!ready.offerLast(readyBatch)) {
            readyBatch.content().release();
            readySlots.release();
            throw new IllegalStateException("Ready queue rejected a batch with a reserved slot");
        }
        shard.active.remove(key);
        unscheduleLocked(shard, key);
        activeCount.decrementAndGet();
        metrics.batchFlushes.increment();
        metrics.recordBatchFlush(reason);
        return true;
    }

    private ReadyBatch toReady(final InternalBatch batch, final String reason) {
        return new ReadyBatch(
                batch.key(),
                batch.receipts().build(),
                batch.content().build(),
                batch.payloadBytes(),
                batch.encodedBytes(),
                reason);
    }

    public ReadyBatch poll() {
        return ready.pollFirst();
    }

    /** A claimed batch retains its ready-slot permit, guaranteeing rollback capacity. */
    public void requeueFirst(final ReadyBatch batch) {
        if (!ready.offerFirst(batch)) {
            throw new IllegalStateException("Reserved ready-batch slot was unavailable during rollback");
        }
        capacityListener.run();
    }

    /** Releases the slot after a committed batch has been permanently finalized. */
    public void claimCommitted() {
        readySlots.release();
        capacityListener.run();
    }

    private void scheduleLocked(final Shard shard, final InternalBatch batch) {
        scheduleLocked(shard, batch, batch.expiryNanos(config));
    }

    private void scheduleLocked(final Shard shard, final InternalBatch batch, final long deadlineNanos) {
        unscheduleLocked(shard, batch.key());
        final Deadline deadline = new Deadline(batch.key(), deadlineNanos, batch.generation());
        shard.scheduled.put(batch.key(), deadline);
        shard.deadlines.add(deadline);
    }

    private void unscheduleLocked(final Shard shard, final BatchKey key) {
        final Deadline previous = shard.scheduled.remove(key);
        if (previous != null) {
            shard.deadlines.remove(previous);
        }
    }

    private boolean reserveActiveSlot() {
        while (true) {
            final int current = activeCount.get();
            if (current >= config.maximumActiveKeys()) {
                return false;
            }
            if (activeCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private Shard shard(final BatchKey key) {
        final int hash = spread(key.hashCode());
        return shards.get(hash & (shards.size() - 1));
    }

    public void discardAll(final MemoryTracker memory) {
        for (Shard shard : shards) {
            shard.lock.lock();
            try {
                for (InternalBatch batch : shard.active.values()) {
                    batch.release();
                    memory.release(batch.eventCount(), batch.payloadBytes());
                }
                activeCount.addAndGet(-shard.active.size());
                shard.active.clear();
                shard.deadlines.clear();
                shard.scheduled.clear();
            } finally {
                shard.lock.unlock();
            }
        }

        ReadyBatch batch;
        while ((batch = ready.pollFirst()) != null) {
            batch.content().release();
            memory.release(batch.eventCount(), batch.payloadBytes());
            readySlots.release();
        }
        capacityListener.run();
    }

    public int activeSize() { return activeCount.get(); }
    public int readySize() { return ready.size(); }
    public boolean hasReadyCapacity() { return readySlots.availablePermits() > 0; }

    private static int normalizePartitions(final int configured) {
        int value = 1;
        while (value < configured && value < 1024) {
            value <<= 1;
        }
        return value;
    }

    private static int spread(final int hash) {
        return hash ^ (hash >>> 16);
    }

    private static final class Shard {
        private final ReentrantLock lock = new ReentrantLock();
        private final Map<BatchKey, InternalBatch> active = new LinkedHashMap<>();
        private final Map<BatchKey, Deadline> scheduled = new LinkedHashMap<>();
        private final NavigableSet<Deadline> deadlines = new TreeSet<>(
                Comparator.comparingLong(Deadline::deadlineNanos)
                        .thenComparing(deadline -> deadline.key().value())
                        .thenComparingLong(Deadline::generation));
    }

    private record Deadline(BatchKey key, long deadlineNanos, long generation) {
    }
}
