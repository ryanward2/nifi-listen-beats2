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

package com.dds.nifi.beats.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * Compact payload-free receipt ledger retained until NiFi commit.
 *
 * <p>Contiguous sequences from the same connection are coalesced into primitive ranges. This
 * removes the per-event receipt object and boxed sequence allocations from raw batching modes
 * while retaining enough information for gap-safe cumulative ACK processing and FlowFile
 * attributes.</p>
 */
public final class BatchReceipts {
    private final ConnectionToken[] runConnections;
    private final long[] runWindowIds;
    private final long[] runFirstSequences;
    private final int[] runEventCounts;
    private final int eventCount;
    private final long oldestReceivedNanos;
    private final ConnectionToken commonConnection;
    private final String commonRemoteAddress;
    private final boolean commonSender;
    private final int commonRemotePort;
    private final boolean commonWindow;
    private final long commonWindowId;
    private final long firstSequence;
    private final long lastSequence;
    private final String commonTlsSubject;
    private final String commonTlsIssuer;

    private BatchReceipts(
            final ConnectionToken[] runConnections,
            final long[] runWindowIds,
            final long[] runFirstSequences,
            final int[] runEventCounts,
            final int eventCount,
            final long oldestReceivedNanos,
            final ConnectionToken commonConnection,
            final String commonRemoteAddress,
            final boolean commonSender,
            final int commonRemotePort,
            final boolean commonWindow,
            final long commonWindowId,
            final long firstSequence,
            final long lastSequence,
            final String commonTlsSubject,
            final String commonTlsIssuer) {
        this.runConnections = runConnections;
        this.runWindowIds = runWindowIds;
        this.runFirstSequences = runFirstSequences;
        this.runEventCounts = runEventCounts;
        this.eventCount = eventCount;
        this.oldestReceivedNanos = oldestReceivedNanos;
        this.commonConnection = commonConnection;
        this.commonRemoteAddress = commonRemoteAddress;
        this.commonSender = commonSender;
        this.commonRemotePort = commonRemotePort;
        this.commonWindow = commonWindow;
        this.commonWindowId = commonWindowId;
        this.firstSequence = firstSequence;
        this.lastSequence = lastSequence;
        this.commonTlsSubject = commonTlsSubject;
        this.commonTlsIssuer = commonTlsIssuer;
    }

    public int runCount() { return runConnections.length; }
    public ConnectionToken runConnection(final int index) { return runConnections[index]; }
    public long runWindowId(final int index) { return runWindowIds[index]; }
    public long runFirstSequence(final int index) { return runFirstSequences[index]; }
    public int runEventCount(final int index) { return runEventCounts[index]; }
    public int eventCount() { return eventCount; }
    public long oldestReceivedNanos() { return oldestReceivedNanos; }
    public boolean singleConnection() { return commonConnection != null; }
    public ConnectionToken commonConnection() { return commonConnection; }
    public boolean singleSender() { return commonSender; }
    public String commonRemoteAddress() { return commonRemoteAddress; }
    public int commonRemotePort() { return commonRemotePort; }
    public boolean singleWindow() { return commonConnection != null && commonWindow; }
    public long commonWindowId() { return commonWindowId; }
    public long firstSequence() { return firstSequence; }
    public long lastSequence() { return lastSequence; }
    public String commonTlsSubject() { return commonTlsSubject; }
    public String commonTlsIssuer() { return commonTlsIssuer; }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private ConnectionToken[] runConnections = new ConnectionToken[16];
        private long[] runWindowIds = new long[16];
        private long[] runFirstSequences = new long[16];
        private int[] runEventCounts = new int[16];
        private int runCount;
        private int eventCount;
        private long oldestReceivedNanos = Long.MAX_VALUE;

        private ConnectionToken firstConnection;
        private String firstRemoteAddress;
        private int firstRemotePort;
        private long firstWindowId;
        private long firstSequence;
        private long lastSequence;
        private String firstTlsSubject;
        private String firstTlsIssuer;
        private boolean commonConnection = true;
        private boolean commonSender = true;
        private boolean commonWindow = true;
        private boolean built;

        /** Ensures append capacity before payload bytes are copied into the batch. */
        public void prepareForAppend(final BeatsEvent event) {
            if (built) {
                throw new IllegalStateException("Receipt ledger has already been built");
            }
            Objects.requireNonNull(event, "event");
            ensureRunCapacity(runCount + 1);
        }

        public void append(final BeatsEvent event) {
            prepareForAppend(event);
            if (eventCount == 0) {
                firstConnection = event.connection();
                firstRemoteAddress = event.remoteAddress();
                firstRemotePort = event.remotePort();
                firstWindowId = event.windowId();
                firstSequence = event.sequence();
                firstTlsSubject = event.tlsSubject();
                firstTlsIssuer = event.tlsIssuer();
            } else {
                commonConnection &= firstConnection.equals(event.connection());
                commonSender &= Objects.equals(firstRemoteAddress, event.remoteAddress());
                commonWindow &= commonConnection && firstWindowId == event.windowId();
            }
            lastSequence = event.sequence();
            oldestReceivedNanos = Math.min(oldestReceivedNanos, event.receivedNanos());
            appendRun(event.connection(), event.windowId(), event.sequence());
            eventCount++;
        }

        private void appendRun(final ConnectionToken connection, final long windowId, final long sequence) {
            if (runCount > 0) {
                final int last = runCount - 1;
                final long expected = next(runFirstSequences[last], runEventCounts[last]);
                if (runConnections[last].equals(connection)
                        && runWindowIds[last] == windowId
                        && expected == sequence
                        && runEventCounts[last] < Integer.MAX_VALUE) {
                    runEventCounts[last]++;
                    return;
                }
            }
            ensureRunCapacity(runCount + 1);
            runConnections[runCount] = connection;
            runWindowIds[runCount] = windowId;
            runFirstSequences[runCount] = sequence;
            runEventCounts[runCount] = 1;
            runCount++;
        }

        public int eventCount() {
            return eventCount;
        }

        public BatchReceipts build() {
            if (built) {
                throw new IllegalStateException("Receipt ledger has already been built");
            }
            if (eventCount == 0) {
                throw new IllegalStateException("Cannot build an empty receipt ledger");
            }
            built = true;
            return new BatchReceipts(
                    Arrays.copyOf(runConnections, runCount),
                    Arrays.copyOf(runWindowIds, runCount),
                    Arrays.copyOf(runFirstSequences, runCount),
                    Arrays.copyOf(runEventCounts, runCount),
                    eventCount,
                    oldestReceivedNanos,
                    commonConnection ? firstConnection : null,
                    firstRemoteAddress,
                    commonSender,
                    firstRemotePort,
                    commonWindow,
                    firstWindowId,
                    firstSequence,
                    lastSequence,
                    commonConnection ? firstTlsSubject : null,
                    commonConnection ? firstTlsIssuer : null);
        }

        private void ensureRunCapacity(final int required) {
            if (required <= runConnections.length) {
                return;
            }
            final int next = Math.max(required, runConnections.length << 1);
            runConnections = Arrays.copyOf(runConnections, next);
            runWindowIds = Arrays.copyOf(runWindowIds, next);
            runFirstSequences = Arrays.copyOf(runFirstSequences, next);
            runEventCounts = Arrays.copyOf(runEventCounts, next);
        }

        private static long next(final long first, final int count) {
            return (first + Integer.toUnsignedLong(count)) & 0xFFFF_FFFFL;
        }
    }
}
