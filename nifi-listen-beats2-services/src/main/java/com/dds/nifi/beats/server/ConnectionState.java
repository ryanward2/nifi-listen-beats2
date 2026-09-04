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

import com.dds.nifi.beats.model.ConnectionToken;
import com.dds.nifi.beats.protocol.ProtocolException;
import io.netty.channel.Channel;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Per-connection Lumberjack state.
 *
 * <p>Sequence continuity is validated within each advertised window. This is required because
 * current go-lumber v2 senders reset sequences for each batch, while older senders can continue
 * from an arbitrary sequence. ACK frames have no window identifier, so cumulative ACKs are emitted
 * strictly in advertised-window order even when NiFi commits later windows first.</p>
 */
public final class ConnectionState {
    private static final long UNSIGNED_MASK = 0xFFFF_FFFFL;
    private static final int MAX_PENDING_WINDOWS = 1024;

    private final ConnectionToken token;
    private final Channel channel;
    private final String remoteAddress;
    private final int remotePort;
    private final Deque<WindowState> windows = new ArrayDeque<>();
    private final Map<Long, WindowState> windowsById = new HashMap<>();
    private final EnumSet<PressureReason> pressureReasons = EnumSet.noneOf(PressureReason.class);

    private long currentWindowId;
    private WindowState receivingWindow;
    private long outstanding;
    private long outstandingBytes;

    private volatile Runnable decoderRetryAction;
    private volatile Runnable pendingRetryAction;
    private volatile Runnable streamRetryAction;
    private volatile boolean pendingWorkPresent;
    private volatile boolean streamWorkPresent;
    private volatile boolean decoderWorkPresent;
    private volatile long processingBytesNeeded;
    private volatile boolean processingTaskNeeded;
    private volatile long lastAcknowledgementWriteNanos = System.nanoTime();
    private long lastAcknowledgedWindowId = -1L;
    private long lastAcknowledgedSequence = -1L;
    private String tlsSubject;
    private String tlsIssuer;

    public ConnectionState(
            final ConnectionToken token,
            final Channel channel,
            final String remoteAddress,
            final int remotePort) {
        this.token = token;
        this.channel = channel;
        this.remoteAddress = remoteAddress;
        this.remotePort = remotePort;
    }

    /**
     * Opens the next advertised window. Zero-length windows are legal and ACKed in window order.
     * The first event sequence is intentionally not constrained to one.
     */
    public synchronized List<CommitAdvance> window(
            final long window,
            final long configuredMaximumWindow) throws ProtocolException {
        if (window < 0 || window > configuredMaximumWindow || window > Integer.MAX_VALUE) {
            throw new ProtocolException("Advertised window is outside configured bounds: " + window);
        }
        if (receivingWindow != null) {
            throw new ProtocolException("New window received before the current window completed: remaining="
                    + receivingWindow.remaining());
        }
        if (windows.size() >= MAX_PENDING_WINDOWS) {
            throw new ProtocolException("Outstanding protocol windows exceed the hard per-connection maximum: "
                    + MAX_PENDING_WINDOWS);
        }

        currentWindowId = next(currentWindowId);
        final WindowState opened = new WindowState(currentWindowId, (int) window);
        windows.addLast(opened);
        windowsById.put(opened.id, opened);
        if (opened.advertisedCount > 0) {
            receivingWindow = opened;
        }
        return advanceAcknowledgements();
    }

    public synchronized boolean hasOutstandingCapacity(
            final int eventBytes,
            final long configuredMaximumOutstanding,
            final long configuredMaximumOutstandingBytes) {
        return outstanding < configuredMaximumOutstanding
                && eventBytes >= 0
                && eventBytes <= configuredMaximumOutstandingBytes - outstandingBytes;
    }

    public ReceiveReceipt receive(
            final long sequence,
            final int eventBytes,
            final long configuredMaximumOutstanding,
            final long configuredMaximumOutstandingBytes) throws ProtocolException {
        return receive(sequence, eventBytes, configuredMaximumOutstanding,
                configuredMaximumOutstandingBytes, System.nanoTime());
    }

    public synchronized ReceiveReceipt receive(
            final long sequence,
            final int eventBytes,
            final long configuredMaximumOutstanding,
            final long configuredMaximumOutstandingBytes,
            final long eventReceivedNanos) throws ProtocolException {
        final WindowState window = receivingWindow;
        if (window == null) {
            throw new ProtocolException("JSON frame received without an active window");
        }
        if (eventBytes < 0) {
            throw new ProtocolException("Negative event size");
        }
        if (outstanding >= configuredMaximumOutstanding) {
            throw new ProtocolException("Outstanding unacknowledged events exceed the configured per-connection maximum: "
                    + configuredMaximumOutstanding);
        }
        if (eventBytes > configuredMaximumOutstandingBytes - outstandingBytes) {
            throw new ProtocolException("Outstanding unacknowledged bytes exceed the configured per-connection maximum: "
                    + configuredMaximumOutstandingBytes);
        }

        final long normalizedSequence = sequence & UNSIGNED_MASK;
        window.receive(normalizedSequence, eventBytes, eventReceivedNanos);
        outstanding++;
        outstandingBytes += eventBytes;

        final boolean complete = window.receivedCount == window.advertisedCount;
        if (complete) {
            receivingWindow = null;
        }
        return new ReceiveReceipt(window.id, complete);
    }

    /**
     * Marks a contiguous receipt range committed. Unknown, stale, or duplicate post-commit
     * receipts are ignored because the NiFi transaction has already completed.
     */
    public synchronized List<CommitAdvance> commitRange(
            final long windowId,
            final long firstSequence,
            final int eventCount) {
        if (eventCount <= 0) {
            return List.of();
        }
        final WindowState window = windowsById.get(windowId);
        if (window == null || window.receivedCount == 0) {
            return List.of();
        }

        final long distance = unsignedDistance(window.firstSequence, firstSequence & UNSIGNED_MASK);
        if (distance >= window.receivedCount || distance > Integer.MAX_VALUE) {
            return List.of();
        }
        final int start = (int) distance;
        final int count = Math.min(eventCount, window.receivedCount - start);
        if (count <= 0) {
            return List.of();
        }
        window.committed.set(start, start + count);
        return advanceAcknowledgements();
    }

    /**
     * Advances only the head window. Once it is completely ACK-eligible, later already-committed
     * windows can produce additional ordered ACK frames in the same write cycle.
     */
    private List<CommitAdvance> advanceAcknowledgements() {
        List<CommitAdvance> advances = null;
        while (true) {
            final WindowState head = windows.peekFirst();
            if (head == null) {
                break;
            }

            if (head.advertisedCount == 0) {
                if (advances == null) {
                    advances = new ArrayList<>();
                }
                advances.add(new CommitAdvance(head.id, 0L, 0L, -1L));
                removeHead(head);
                continue;
            }

            final int start = head.acknowledgedCount;
            int end = head.committed.nextClearBit(start);
            end = Math.min(end, head.receivedCount);
            if (end <= start) {
                break;
            }

            long releasedBytes = 0L;
            long oldestReceivedNanos = Long.MAX_VALUE;
            for (int offset = start; offset < end; offset++) {
                releasedBytes += head.receivedBytes[offset];
                head.receivedBytes[offset] = 0;
                final long receivedNanos = head.receivedNanos[offset];
                if (receivedNanos > 0) {
                    oldestReceivedNanos = Math.min(oldestReceivedNanos, receivedNanos);
                }
                head.receivedNanos[offset] = 0L;
            }

            final int advancedEvents = end - start;
            head.acknowledgedCount = end;
            outstanding -= advancedEvents;
            outstandingBytes -= releasedBytes;
            if (outstanding < 0 || outstandingBytes < 0) {
                throw new IllegalStateException("Connection outstanding accounting underflow");
            }

            if (advances == null) {
                advances = new ArrayList<>();
            }
            advances.add(new CommitAdvance(
                    head.id,
                    sequenceAt(head.firstSequence, end - 1),
                    advancedEvents,
                    oldestReceivedNanos == Long.MAX_VALUE ? -1L : oldestReceivedNanos));

            if (head.acknowledgedCount == head.advertisedCount
                    && head.receivedCount == head.advertisedCount) {
                removeHead(head);
                continue;
            }
            break;
        }
        return advances == null ? List.of() : List.copyOf(advances);
    }

    private void removeHead(final WindowState expected) {
        final WindowState removed = windows.pollFirst();
        if (removed != expected) {
            throw new IllegalStateException("Protocol window ordering corrupted");
        }
        windowsById.remove(expected.id);
    }

    public boolean suspend(final PressureReason reason, final ProcessorMetrics metrics) {
        final boolean firstReason;
        synchronized (this) {
            firstReason = pressureReasons.isEmpty();
            pressureReasons.add(reason);
        }
        if (firstReason) {
            metrics.readSuspendedChannels.incrementAndGet();
            channel.eventLoop().execute(() -> channel.config().setAutoRead(false));
        }
        return firstReason;
    }

    public synchronized void clear(final PressureReason reason) {
        pressureReasons.remove(reason);
    }

    public synchronized boolean isSuspendedFor(final PressureReason reason) {
        return pressureReasons.contains(reason);
    }

    public synchronized boolean readSuspended() {
        return !pressureReasons.isEmpty();
    }

    public synchronized Set<PressureReason> pressureReasons() {
        return Set.copyOf(pressureReasons);
    }

    public void resumeIfClear(final ProcessorMetrics metrics) {
        final boolean clear;
        synchronized (this) {
            clear = pressureReasons.isEmpty();
        }
        if (!clear || !channel.isActive()) {
            return;
        }
        channel.eventLoop().execute(() -> {
            synchronized (ConnectionState.this) {
                if (!pressureReasons.isEmpty() || !channel.isActive()) {
                    return;
                }
            }
            if (!channel.config().isAutoRead()) {
                channel.config().setAutoRead(true);
                metrics.readSuspendedChannels.updateAndGet(value -> Math.max(0, value - 1));
            }
            channel.read();
        });
    }

    public synchronized boolean clearAllPressure() {
        final boolean wasSuspended = !pressureReasons.isEmpty();
        pressureReasons.clear();
        return wasSuspended;
    }

    public void decoderRetryAction(final Runnable action) { decoderRetryAction = action; }
    public void pendingRetryAction(final Runnable action) { pendingRetryAction = action; }
    public void streamRetryAction(final Runnable action) { streamRetryAction = action; }

    public void requestRetry() {
        if (!channel.isActive()) {
            return;
        }
        final Runnable pendingAction = pendingRetryAction;
        if (pendingWorkPresent && pendingAction != null) {
            pendingAction.run();
            return;
        }
        final Runnable streamAction = streamRetryAction;
        if (streamWorkPresent && streamAction != null) {
            streamAction.run();
            return;
        }
        final Runnable decoderAction = decoderRetryAction;
        if (decoderWorkPresent && decoderAction != null) {
            decoderAction.run();
        }
    }

    public void clearDecoderRetryAction() { decoderRetryAction = null; }
    public void clearPendingRetryAction() { pendingRetryAction = null; }
    public void clearStreamRetryAction() { streamRetryAction = null; }
    public void pendingWorkPresent(final boolean present) { pendingWorkPresent = present; }
    public void streamWorkPresent(final boolean present) { streamWorkPresent = present; }
    public void decoderWorkPresent(final boolean present) { decoderWorkPresent = present; }

    public void processingRetryRequirement(final long bytes, final boolean taskRequired) {
        processingBytesNeeded = Math.max(0L, bytes);
        processingTaskNeeded = taskRequired;
    }

    public void clearProcessingRetryRequirement() {
        processingBytesNeeded = 0L;
        processingTaskNeeded = false;
    }

    public long processingBytesNeeded() { return processingBytesNeeded; }
    public boolean processingTaskNeeded() { return processingTaskNeeded; }

    public static long next(final long value) { return (value + 1L) & UNSIGNED_MASK; }
    public static long previous(final long value) { return (value - 1L) & UNSIGNED_MASK; }

    private static long unsignedDistance(final long first, final long value) {
        return (value - first) & UNSIGNED_MASK;
    }

    private static long sequenceAt(final long first, final int zeroBasedOffset) {
        return (first + Integer.toUnsignedLong(zeroBasedOffset)) & UNSIGNED_MASK;
    }

    public void markAcknowledgementWrite(final long nanos) { lastAcknowledgementWriteNanos = nanos; }

    public synchronized void markAcknowledgementWrite(
            final long nanos,
            final long windowId,
            final long sequence) {
        lastAcknowledgementWriteNanos = nanos;
        lastAcknowledgedWindowId = windowId;
        lastAcknowledgedSequence = sequence & UNSIGNED_MASK;
    }

    public long lastAcknowledgementWriteNanos() { return lastAcknowledgementWriteNanos; }

    /**
     * Returns an ACK that is safe to repeat only while its partially acknowledged window remains
     * the current protocol head. ACKs from completed/removed windows must never be replayed into a
     * later window because Beats senders can reset sequence numbers between windows.
     */
    public synchronized KeepAliveAcknowledgement repeatableKeepAliveAcknowledgement() {
        final WindowState head = windows.peekFirst();
        if (head == null
                || head.id != lastAcknowledgedWindowId
                || head.acknowledgedCount <= 0
                || head.acknowledgedCount >= head.advertisedCount
                || lastAcknowledgedSequence < 0L) {
            return null;
        }
        return new KeepAliveAcknowledgement(lastAcknowledgedWindowId, lastAcknowledgedSequence);
    }

    public synchronized boolean isRepeatableKeepAliveAcknowledgement(
            final long windowId,
            final long sequence) {
        final KeepAliveAcknowledgement current = repeatableKeepAliveAcknowledgement();
        return current != null
                && current.windowId() == windowId
                && current.sequence() == (sequence & UNSIGNED_MASK);
    }

    public ConnectionToken token() { return token; }
    public Channel channel() { return channel; }
    public String remoteAddress() { return remoteAddress; }
    public int remotePort() { return remotePort; }
    public String tlsSubject() { return tlsSubject; }
    public void tlsSubject(final String value) { tlsSubject = value; }
    public String tlsIssuer() { return tlsIssuer; }
    public void tlsIssuer(final String value) { tlsIssuer = value; }
    public synchronized long outstanding() { return outstanding; }
    public synchronized long outstandingBytes() { return outstandingBytes; }
    public synchronized long currentWindowRemaining() {
        return receivingWindow == null ? 0L : receivingWindow.remaining();
    }
    public synchronized int pendingWindowCount() { return windows.size(); }
    public synchronized long committedPendingCount() {
        long count = 0L;
        for (WindowState window : windows) {
            count += window.committed.cardinality();
        }
        return count;
    }

    public record ReceiveReceipt(long windowId, boolean windowComplete) { }

    public record KeepAliveAcknowledgement(long windowId, long sequence) { }

    /** One wire ACK frontier. Multiple advances can be produced in strict window order. */
    public record CommitAdvance(long windowId, long ackSequence, long eventsAdvanced, long oldestReceivedNanos) { }

    private static final class WindowState {
        private final long id;
        private final int advertisedCount;
        private final BitSet committed;
        private int[] receivedBytes = new int[16];
        private long[] receivedNanos = new long[16];
        private int receivedCount;
        private int acknowledgedCount;
        private boolean sequenceInitialized;
        private long firstSequence;
        private long expectedSequence;

        private WindowState(final long id, final int advertisedCount) {
            this.id = id;
            this.advertisedCount = advertisedCount;
            this.committed = new BitSet(advertisedCount);
        }

        private void receive(final long sequence, final int eventBytes, final long eventReceivedNanos)
                throws ProtocolException {
            if (receivedCount >= advertisedCount) {
                throw new ProtocolException("Window received more events than advertised: " + advertisedCount);
            }
            if (!sequenceInitialized) {
                sequenceInitialized = true;
                firstSequence = sequence;
                expectedSequence = next(sequence);
            } else {
                if (sequence != expectedSequence) {
                    throw new ProtocolException("Sequence gap within window " + id + ": expected "
                            + Long.toUnsignedString(expectedSequence) + " received "
                            + Long.toUnsignedString(sequence));
                }
                expectedSequence = next(expectedSequence);
            }
            ensureCapacity(receivedCount + 1);
            receivedBytes[receivedCount] = eventBytes;
            receivedNanos[receivedCount] = eventReceivedNanos;
            receivedCount++;
        }

        private long remaining() {
            return advertisedCount - (long) receivedCount;
        }

        private void ensureCapacity(final int required) {
            if (required <= receivedBytes.length) {
                return;
            }
            int capacity = Math.min(advertisedCount, Math.max(required, receivedBytes.length << 1));
            receivedBytes = java.util.Arrays.copyOf(receivedBytes, capacity);
            receivedNanos = java.util.Arrays.copyOf(receivedNanos, capacity);
        }
    }
}
