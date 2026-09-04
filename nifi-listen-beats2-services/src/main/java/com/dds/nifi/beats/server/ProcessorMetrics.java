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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/** Low-contention listener-wide counters and bounded latency samples. No high-cardinality labels. */
public final class ProcessorMetrics {
    private final long startedNanos = System.nanoTime();

    public final AtomicLong currentConnections = new AtomicLong();
    public final LongAdder acceptedConnections = new LongAdder();
    public final LongAdder rejectedConnections = new LongAdder();
    public final LongAdder connectionRateLimited = new LongAdder();
    public final LongAdder protocolConnectionCloses = new LongAdder();
    public final LongAdder idleConnectionCloses = new LongAdder();
    public final LongAdder overloadConnectionCloses = new LongAdder();
    public final LongAdder disconnectedBeforeCommitEvents = new LongAdder();
    public final LongAdder disconnectedAfterCommitEvents = new LongAdder();

    public final LongAdder tlsHandshakeStarted = new LongAdder();
    public final LongAdder tlsHandshakeSucceeded = new LongAdder();
    public final LongAdder tlsHandshakeFailed = new LongAdder();
    public final LongAdder tlsHandshakeRejected = new LongAdder();
    public final LongAdder tlsHandshakeTimeouts = new LongAdder();
    public final AtomicLong tlsConcurrentHandshakes = new AtomicLong();
    public final AtomicLong tlsPeakConcurrentHandshakes = new AtomicLong();

    public final LongAdder framesDecoded = new LongAdder();
    public final LongAdder windowsReceived = new LongAdder();
    public final LongAdder jsonFrames = new LongAdder();
    public final LongAdder protocolErrors = new LongAdder();
    public final LongAdder sequenceErrors = new LongAdder();
    public final LongAdder compressedFrames = new LongAdder();
    public final LongAdder compressedBytes = new LongAdder();
    public final LongAdder decompressedBytes = new LongAdder();
    public final LongAdder partialFrameReservations = new LongAdder();
    public final AtomicLong partialFrameReservedBytes = new AtomicLong();
    public final LongAdder frameAssemblyTimeouts = new LongAdder();
    public final LongAdder firstByteTimeouts = new LongAdder();

    public final LongAdder processingOffloadSubmitted = new LongAdder();
    public final LongAdder processingOffloadCancelled = new LongAdder();
    public final LongAdder processingExecutorRejections = new LongAdder();
    public final AtomicLong processingExecutorPendingTasks = new AtomicLong();
    public final AtomicLong processingExecutorHottestQueue = new AtomicLong();
    public final AtomicLong processingExecutorHottestQueueMaximum = new AtomicLong();
    public final LongAdder processingExecutorPressureTransitions = new LongAdder();
    public final LongAdder processingExecutorPressureRecoveries = new LongAdder();

    public final LongAdder filterEventsInput = new LongAdder();
    public final LongAdder filterEventsKept = new LongAdder();
    public final LongAdder filterEventsDropped = new LongAdder();
    public final LongAdder filterBytesDropped = new LongAdder();
    public final LongAdder filterEvaluationErrors = new LongAdder();

    public final LongAdder eventsAccepted = new LongAdder();
    public final LongAdder payloadBytesAccepted = new LongAdder();
    public final LongAdder eventsCommitted = new LongAdder();
    public final LongAdder payloadBytesCommitted = new LongAdder();
    public final LongAdder acknowledgementsSent = new LongAdder();
    public final LongAdder acknowledgementFailures = new LongAdder();
    public final LongAdder acknowledgementWriteTimeouts = new LongAdder();
    public final LongAdder acknowledgementUnwritableWrites = new LongAdder();
    public final AtomicLong pendingAcknowledgementWrites = new AtomicLong();
    public final LongAdder keepAlivesSent = new LongAdder();
    public final LongAdder keepAliveFailures = new LongAdder();

    public final AtomicLong readSuspendedChannels = new AtomicLong();
    public final LongAdder processingPressureEvents = new LongAdder();
    public final LongAdder acceptedMemoryPressureEvents = new LongAdder();
    public final LongAdder readyBatchPressureEvents = new LongAdder();
    public final LongAdder perConnectionPressureEvents = new LongAdder();
    public final LongAdder globalPressureTransitions = new LongAdder();
    public final LongAdder globalPressureRecoveries = new LongAdder();
    public final LongAdder pressureRetryAttempts = new LongAdder();
    public final LongAdder pressureRetryCohorts = new LongAdder();
    public final AtomicLong deferredFrames = new AtomicLong();
    public final AtomicLong acceptSuspended = new AtomicLong();

    public final AtomicLong cleanupPendingTasks = new AtomicLong();
    public final AtomicLong cleanupPeakPendingTasks = new AtomicLong();
    public final AtomicLong cleanupPressureActive = new AtomicLong();
    public final LongAdder cleanupSubmitted = new LongAdder();
    public final LongAdder cleanupCompleted = new LongAdder();
    public final LongAdder cleanupEmergencySubmitted = new LongAdder();
    public final LongAdder cleanupRejections = new LongAdder();
    public final LongAdder cleanupDrainFailures = new LongAdder();
    public final LongAdder cleanupPressureTransitions = new LongAdder();
    public final LongAdder cleanupPressureRecoveries = new LongAdder();

    public final AtomicLong eventLoopLagCurrentNanos = new AtomicLong();
    public final AtomicLong eventLoopLagMaximumNanos = new AtomicLong();
    public final LongAdder eventLoopLagOver50Millis = new LongAdder();
    public final LongAdder eventLoopLagOver100Millis = new LongAdder();
    public final LongAdder eventLoopLagOver1000Millis = new LongAdder();

    public final LongAdder batchFlushes = new LongAdder();
    public final LongAdder batchFlushEventCount = new LongAdder();
    public final LongAdder batchFlushByteCount = new LongAdder();
    public final LongAdder batchFlushMaximumAge = new LongAdder();
    public final LongAdder batchFlushIdle = new LongAdder();
    public final LongAdder batchFlushWindow = new LongAdder();
    public final LongAdder batchFlushNone = new LongAdder();
    public final LongAdder batchFlushActiveKeyLimit = new LongAdder();
    public final LongAdder batchFlushShutdown = new LongAdder();

    public final LongAdder claimsCreated = new LongAdder();
    public final LongAdder claimsCommitted = new LongAdder();
    public final LongAdder claimsRolledBack = new LongAdder();
    public final LongAdder claimsAbandoned = new LongAdder();
    public final LongAdder claimFinalizationFailures = new LongAdder();

    private final Latency queueAge = new Latency();
    private final Latency commitLatency = new Latency();
    private final Latency acknowledgementLatency = new Latency();
    private final Latency tlsHandshakeLatency = new Latency();
    private final LongAdder[] connectionCloseReasons = initializeCloseReasons();

    public void recordQueueAge(final long nanos) { queueAge.record(nanos); }
    public void recordCommitLatency(final long nanos) { commitLatency.record(nanos); }
    public void recordAcknowledgementLatency(final long nanos) { acknowledgementLatency.record(nanos); }
    public void recordTlsHandshakeLatency(final long nanos) { tlsHandshakeLatency.record(nanos); }
    public long queueAgeAverageMillis() { return queueAge.averageMillis(); }
    public long queueAgeMaximumMillis() { return queueAge.maximumMillis(); }
    public long queueAgeP50Millis() { return queueAge.percentileMillis(0.50); }
    public long queueAgeP95Millis() { return queueAge.percentileMillis(0.95); }
    public long queueAgeP99Millis() { return queueAge.percentileMillis(0.99); }
    public long commitLatencyAverageMillis() { return commitLatency.averageMillis(); }
    public long commitLatencyMaximumMillis() { return commitLatency.maximumMillis(); }
    public long commitLatencyP50Millis() { return commitLatency.percentileMillis(0.50); }
    public long commitLatencyP95Millis() { return commitLatency.percentileMillis(0.95); }
    public long commitLatencyP99Millis() { return commitLatency.percentileMillis(0.99); }
    public long acknowledgementLatencyAverageMillis() { return acknowledgementLatency.averageMillis(); }
    public long acknowledgementLatencyMaximumMillis() { return acknowledgementLatency.maximumMillis(); }
    public long acknowledgementLatencyP50Millis() { return acknowledgementLatency.percentileMillis(0.50); }
    public long acknowledgementLatencyP95Millis() { return acknowledgementLatency.percentileMillis(0.95); }
    public long acknowledgementLatencyP99Millis() { return acknowledgementLatency.percentileMillis(0.99); }
    public long tlsHandshakeLatencyAverageMillis() { return tlsHandshakeLatency.averageMillis(); }
    public long tlsHandshakeLatencyMaximumMillis() { return tlsHandshakeLatency.maximumMillis(); }
    public long tlsHandshakeLatencyP50Millis() { return tlsHandshakeLatency.percentileMillis(0.50); }
    public long tlsHandshakeLatencyP95Millis() { return tlsHandshakeLatency.percentileMillis(0.95); }
    public long tlsHandshakeLatencyP99Millis() { return tlsHandshakeLatency.percentileMillis(0.99); }

    public void connectionClosed(final ConnectionCloseReason reason) {
        connectionCloseReasons[reason.ordinal()].increment();
    }

    public Map<String, Long> connectionCloseReasonSnapshot() {
        final EnumMap<ConnectionCloseReason, Long> snapshot = new EnumMap<>(ConnectionCloseReason.class);
        for (ConnectionCloseReason reason : ConnectionCloseReason.values()) {
            snapshot.put(reason, connectionCloseReasons[reason.ordinal()].sum());
        }
        final java.util.LinkedHashMap<String, Long> names = new java.util.LinkedHashMap<>();
        snapshot.forEach((reason, value) -> names.put(reason.name().toLowerCase(java.util.Locale.ROOT), value));
        return Map.copyOf(names);
    }

    public void recordEventLoopLag(final long currentMaximumNanos, final long observedNanos) {
        final long current = Math.max(0L, currentMaximumNanos);
        eventLoopLagCurrentNanos.set(current);
        eventLoopLagMaximumNanos.accumulateAndGet(current, Math::max);
        if (observedNanos >= 50_000_000L) eventLoopLagOver50Millis.increment();
        if (observedNanos >= 100_000_000L) eventLoopLagOver100Millis.increment();
        if (observedNanos >= 1_000_000_000L) eventLoopLagOver1000Millis.increment();
    }

    public long eventLoopLagCurrentMillis() { return eventLoopLagCurrentNanos.get() / 1_000_000L; }
    public long eventLoopLagMaximumMillis() { return eventLoopLagMaximumNanos.get() / 1_000_000L; }

    public long eventsAcceptedPerSecond() { return rate(eventsAccepted.sum()); }
    public long eventsCommittedPerSecond() { return rate(eventsCommitted.sum()); }
    public long payloadBytesAcceptedPerSecond() { return rate(payloadBytesAccepted.sum()); }
    public long payloadBytesCommittedPerSecond() { return rate(payloadBytesCommitted.sum()); }

    private long rate(final long total) {
        final long elapsed = Math.max(1L, System.nanoTime() - startedNanos);
        return (long) ((double) total * 1_000_000_000D / elapsed);
    }

    public void recordBatchFlush(final String reason) {
        switch (reason) {
            case "event-count" -> batchFlushEventCount.increment();
            case "byte-count" -> batchFlushByteCount.increment();
            case "maximum-age" -> batchFlushMaximumAge.increment();
            case "idle" -> batchFlushIdle.increment();
            case "window-boundary" -> batchFlushWindow.increment();
            case "none" -> batchFlushNone.increment();
            case "shutdown", "service-disabled" -> batchFlushShutdown.increment();
            default -> { }
        }
    }


    private static LongAdder[] initializeCloseReasons() {
        final LongAdder[] counters = new LongAdder[ConnectionCloseReason.values().length];
        for (int index = 0; index < counters.length; index++) {
            counters[index] = new LongAdder();
        }
        return counters;
    }

    /** Lock-free fixed-size rolling sample; percentile extraction is cached for one second. */
    private static final class Latency {
        private static final int SAMPLE_SIZE = 2048;
        private final LongAdder count = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();
        private final AtomicLong maximumNanos = new AtomicLong();
        private final AtomicLong cursor = new AtomicLong();
        private final AtomicLongArray samples = new AtomicLongArray(SAMPLE_SIZE);
        private volatile long cachedAtNanos;
        private volatile long cachedP50;
        private volatile long cachedP95;
        private volatile long cachedP99;

        private void record(final long nanos) {
            if (nanos < 0) {
                return;
            }
            count.increment();
            totalNanos.add(nanos);
            maximumNanos.accumulateAndGet(nanos, Math::max);
            samples.set((int) Math.floorMod(cursor.getAndIncrement(), SAMPLE_SIZE), nanos);
        }

        private long averageMillis() {
            final long observations = count.sum();
            return observations == 0 ? 0L : (totalNanos.sum() / observations) / 1_000_000L;
        }

        private long maximumMillis() {
            return maximumNanos.get() / 1_000_000L;
        }

        private long percentileMillis(final double percentile) {
            refreshPercentiles();
            if (percentile <= 0.50D) {
                return cachedP50;
            }
            return percentile <= 0.95D ? cachedP95 : cachedP99;
        }

        private void refreshPercentiles() {
            final long now = System.nanoTime();
            if (now - cachedAtNanos < 1_000_000_000L) {
                return;
            }
            synchronized (this) {
                if (now - cachedAtNanos < 1_000_000_000L) {
                    return;
                }
                final int size = (int) Math.min(cursor.get(), SAMPLE_SIZE);
                if (size == 0) {
                    cachedP50 = cachedP95 = cachedP99 = 0L;
                } else {
                    final long[] copy = new long[size];
                    for (int index = 0; index < size; index++) {
                        copy[index] = samples.get(index);
                    }
                    Arrays.sort(copy);
                    cachedP50 = percentile(copy, 0.50D);
                    cachedP95 = percentile(copy, 0.95D);
                    cachedP99 = percentile(copy, 0.99D);
                }
                cachedAtNanos = now;
            }
        }

        private static long percentile(final long[] values, final double percentile) {
            final int index = Math.min(values.length - 1,
                    Math.max(0, (int) Math.ceil(percentile * values.length) - 1));
            return values[index] / 1_000_000L;
        }
    }

}
