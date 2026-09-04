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

package com.dds.nifi.beats.service;

import com.dds.nifi.beats.batch.ReadyBatch;
import com.dds.nifi.beats.server.ProcessorMetrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Explicit claim state machine separating processor ownership from NiFi commit confirmation. */
final class ClaimManager {
    enum State { CLAIMED, COMMIT_CONFIRMED, ROLLBACK_CONFIRMED, ABANDONED }

    private final Map<UUID, ClaimRecord> claims = new LinkedHashMap<>();
    private final ProcessorMetrics metrics;
    private boolean accepting = true;
    private int activeResolutions;

    ClaimManager(final ProcessorMetrics metrics) {
        this.metrics = metrics;
    }

    synchronized UUID claim(final ReadyBatch batch) {
        if (!accepting) {
            return null;
        }
        final UUID id = UUID.randomUUID();
        claims.put(id, new ClaimRecord(batch, State.CLAIMED, System.nanoTime()));
        return id;
    }

    synchronized List<ReadyBatch> confirmCommitted(final Collection<UUID> ids) {
        final List<ReadyBatch> committed = new ArrayList<>(ids.size());
        final long now = System.nanoTime();
        for (UUID id : ids) {
            final ClaimRecord record = claims.get(id);
            if (record == null || record.state != State.CLAIMED) {
                continue;
            }
            record.state = State.COMMIT_CONFIRMED;
            claims.remove(id);
            metrics.recordCommitLatency(now - record.claimedNanos);
            committed.add(record.batch);
        }
        if (!committed.isEmpty()) {
            activeResolutions++;
        }
        notifyAll();
        return committed;
    }

    synchronized List<ReadyBatch> confirmRolledBack(final Collection<UUID> ids) {
        final List<ReadyBatch> rolledBack = new ArrayList<>(ids.size());
        for (UUID id : ids) {
            final ClaimRecord record = claims.get(id);
            if (record == null || record.state != State.CLAIMED) {
                continue;
            }
            record.state = State.ROLLBACK_CONFIRMED;
            claims.remove(id);
            rolledBack.add(record.batch);
        }
        if (!rolledBack.isEmpty()) {
            activeResolutions++;
        }
        notifyAll();
        return rolledBack;
    }

    synchronized void resolutionCompleted() {
        if (activeResolutions <= 0) {
            throw new IllegalStateException("No active claim resolution to complete");
        }
        activeResolutions--;
        notifyAll();
    }

    synchronized void quiesce() {
        accepting = false;
    }

    synchronized boolean accepting() {
        return accepting;
    }

    synchronized boolean awaitEmpty(final Duration timeout) {
        final long timeoutNanos = Math.max(0L, timeout.toNanos());
        final long start = System.nanoTime();
        while (!claims.isEmpty() || activeResolutions > 0) {
            final long elapsed = System.nanoTime() - start;
            final long remaining = timeoutNanos - elapsed;
            if (remaining <= 0) {
                return false;
            }
            final long millis = Math.max(1L, Math.min(100L, remaining / 1_000_000L));
            try {
                wait(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    synchronized List<ReadyBatch> abandonOutstanding() {
        final List<ReadyBatch> abandoned = new ArrayList<>(claims.size());
        for (ClaimRecord record : claims.values()) {
            if (record.state == State.CLAIMED) {
                record.state = State.ABANDONED;
                abandoned.add(record.batch);
            }
        }
        claims.clear();
        notifyAll();
        return abandoned;
    }

    synchronized int size() {
        return claims.size();
    }

    synchronized int activeResolutionCount() {
        return activeResolutions;
    }

    private static final class ClaimRecord {
        private final ReadyBatch batch;
        private final long claimedNanos;
        private State state;

        private ClaimRecord(final ReadyBatch batch, final State state, final long claimedNanos) {
            this.batch = batch;
            this.state = state;
            this.claimedNanos = claimedNanos;
        }
    }
}
