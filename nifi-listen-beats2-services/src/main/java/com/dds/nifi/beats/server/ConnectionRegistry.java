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
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Bounded global/per-source connection registry with exact rejection classification. */
public final class ConnectionRegistry {
    private final int maximumConnections;
    private final int maximumPerSource;
    private final int maximumAttemptsPerSecond;
    private final int maximumAttemptsPerSourcePerSecond;
    private final int maximumTrackedAttemptSources;
    private final ProcessorMetrics metrics;
    private final LongSupplier attemptSecondClock;
    private final Map<ConnectionToken, ConnectionState> states = new ConcurrentHashMap<>();
    private final Map<String, Integer> sourceCounts = new HashMap<>();
    private final Map<String, AttemptWindow> sourceAttemptWindows = new HashMap<>();
    private final Object registrationLock = new Object();
    private final AtomicLong peakConnections = new AtomicLong();
    private final AtomicLong peakConnectionsPerSource = new AtomicLong();
    private int totalConnections;
    private long globalAttemptSecond = Long.MIN_VALUE;
    private int globalAttempts;
    private long lastAttemptPruneSecond = Long.MIN_VALUE;
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public ConnectionRegistry(final int maximumConnections, final int maximumPerSource, final ProcessorMetrics metrics) {
        this(maximumConnections, maximumPerSource, 0, 0, metrics);
    }

    public ConnectionRegistry(
            final int maximumConnections,
            final int maximumPerSource,
            final int maximumAttemptsPerSecond,
            final int maximumAttemptsPerSourcePerSecond,
            final ProcessorMetrics metrics) {
        this(maximumConnections, maximumPerSource, maximumAttemptsPerSecond,
                maximumAttemptsPerSourcePerSecond, metrics,
                () -> System.nanoTime() / 1_000_000_000L);
    }

    ConnectionRegistry(
            final int maximumConnections,
            final int maximumPerSource,
            final int maximumAttemptsPerSecond,
            final int maximumAttemptsPerSourcePerSecond,
            final ProcessorMetrics metrics,
            final LongSupplier attemptSecondClock) {
        this.maximumConnections = maximumConnections;
        this.maximumPerSource = maximumPerSource;
        this.maximumAttemptsPerSecond = maximumAttemptsPerSecond;
        this.maximumAttemptsPerSourcePerSecond = maximumAttemptsPerSourcePerSecond;
        final long trackingBasis = maximumAttemptsPerSecond > 0
                ? maximumAttemptsPerSecond : maximumConnections;
        this.maximumTrackedAttemptSources = (int) Math.max(
                1_024L, Math.min(1_000_000L, trackingBasis));
        this.metrics = metrics;
        this.attemptSecondClock = attemptSecondClock;
    }

    public Admission admit(final Channel channel) {
        final SocketAddress remoteSocketAddress = channel.remoteAddress();
        final String address;
        final int port;
        if (remoteSocketAddress instanceof InetSocketAddress remote) {
            address = remote.getAddress() == null ? remote.getHostString() : remote.getAddress().getHostAddress();
            port = remote.getPort();
        } else {
            address = remoteSocketAddress == null ? "unknown" : remoteSocketAddress.toString();
            port = 0;
        }

        final int sourceCountAfterAdmission;
        final int totalAfterAdmission;
        synchronized (registrationLock) {
            final ConnectionCloseReason rateRejection = recordConnectionAttemptLocked(address);
            if (rateRejection != null) {
                metrics.rejectedConnections.increment();
                return Admission.rejected(rateRejection);
            }
            final int sourceCount = sourceCounts.getOrDefault(address, 0);
            if (totalConnections >= maximumConnections) {
                metrics.rejectedConnections.increment();
                return Admission.rejected(ConnectionCloseReason.GLOBAL_CONNECTION_LIMIT);
            }
            if (maximumPerSource > 0 && sourceCount >= maximumPerSource) {
                metrics.rejectedConnections.increment();
                return Admission.rejected(ConnectionCloseReason.PER_SOURCE_CONNECTION_LIMIT);
            }
            totalConnections++;
            totalAfterAdmission = totalConnections;
            sourceCountAfterAdmission = sourceCount + 1;
            sourceCounts.put(address, sourceCountAfterAdmission);
        }

        boolean registered = false;
        try {
            final ConnectionToken token = ConnectionToken.create();
            final ConnectionState state = new ConnectionState(token, channel, address, port);
            states.put(token, state);
            channels.add(channel);
            metrics.currentConnections.incrementAndGet();
            metrics.acceptedConnections.increment();
            peakConnections.accumulateAndGet(totalAfterAdmission, Math::max);
            peakConnectionsPerSource.accumulateAndGet(sourceCountAfterAdmission, Math::max);
            registered = true;
            return Admission.accepted(state);
        } finally {
            if (!registered) {
                decrementCounts(address);
            }
        }
    }

    /** Compatibility helper retained for focused tests. */
    public ConnectionState register(final Channel channel) {
        return admit(channel).state();
    }

    public void remove(final ConnectionState state) {
        if (state != null && states.remove(state.token()) != null) {
            decrementCounts(state.remoteAddress());
            metrics.currentConnections.updateAndGet(value -> Math.max(0L, value - 1L));
            if (state.clearAllPressure()) {
                metrics.readSuspendedChannels.updateAndGet(value -> Math.max(0L, value - 1L));
            }
        }
    }

    private void decrementCounts(final String address) {
        synchronized (registrationLock) {
            totalConnections = Math.max(0, totalConnections - 1);
            final int updated = sourceCounts.getOrDefault(address, 1) - 1;
            if (updated <= 0) {
                sourceCounts.remove(address);
            } else {
                sourceCounts.put(address, updated);
            }
        }
    }

    ConnectionCloseReason recordConnectionAttempt(final String address) {
        synchronized (registrationLock) {
            return recordConnectionAttemptLocked(address);
        }
    }

    private ConnectionCloseReason recordConnectionAttemptLocked(final String address) {
        if (maximumAttemptsPerSecond <= 0 && maximumAttemptsPerSourcePerSecond <= 0) {
            return null;
        }
        final long second = attemptSecondClock.getAsLong();
        if (globalAttemptSecond != second) {
            globalAttemptSecond = second;
            globalAttempts = 0;
        }
        pruneAttemptWindows(second);
        if (maximumAttemptsPerSecond > 0 && globalAttempts >= maximumAttemptsPerSecond) {
            return ConnectionCloseReason.GLOBAL_CONNECTION_RATE_LIMIT;
        }

        AttemptWindow sourceWindow = null;
        if (maximumAttemptsPerSourcePerSecond > 0) {
            sourceWindow = sourceAttemptWindows.get(address);
            if (sourceWindow == null || sourceWindow.second != second) {
                if (sourceWindow == null && sourceAttemptWindows.size() >= maximumTrackedAttemptSources) {
                    return ConnectionCloseReason.GLOBAL_CONNECTION_RATE_LIMIT;
                }
                sourceWindow = new AttemptWindow(second, 0);
                sourceAttemptWindows.put(address, sourceWindow);
            }
            if (sourceWindow.attempts >= maximumAttemptsPerSourcePerSecond) {
                return ConnectionCloseReason.PER_SOURCE_CONNECTION_RATE_LIMIT;
            }
        }

        globalAttempts++;
        if (sourceWindow != null) {
            sourceWindow.attempts++;
        }
        return null;
    }

    private void pruneAttemptWindows(final long second) {
        if (lastAttemptPruneSecond == second) {
            return;
        }
        // Fixed-window counters from previous seconds are never reusable. Removing them at the
        // first attempt in each new second bounds per-source rate-state retention to one window.
        sourceAttemptWindows.entrySet().removeIf(entry -> entry.getValue().second != second);
        lastAttemptPruneSecond = second;
    }

    int trackedAttemptSources() {
        synchronized (registrationLock) {
            return sourceAttemptWindows.size();
        }
    }

    public ConnectionState get(final ConnectionToken token) {
        return states.get(token);
    }

    public Collection<ConnectionState> states() {
        return states.values();
    }

    public long peakConnections() {
        return peakConnections.get();
    }

    public long peakConnectionsPerSource() {
        return peakConnectionsPerSource.get();
    }

    public int trackedSources() {
        synchronized (registrationLock) {
            return sourceCounts.size();
        }
    }

    public void markShutdownAndCloseAll() {
        for (ConnectionState state : states.values()) {
            ConnectionCloseTracker.mark(state.channel(), ConnectionCloseReason.NORMAL_SHUTDOWN);
        }
        channels.close().awaitUninterruptibly();
    }

    public void closeAll() {
        channels.close().awaitUninterruptibly();
    }

    public record Admission(ConnectionState state, ConnectionCloseReason rejectionReason) {
        public static Admission accepted(final ConnectionState state) {
            return new Admission(state, null);
        }

        public static Admission rejected(final ConnectionCloseReason reason) {
            return new Admission(null, reason);
        }

        public boolean accepted() {
            return state != null;
        }
    }

    private static final class AttemptWindow {
        private final long second;
        private int attempts;

        private AttemptWindow(final long second, final int attempts) {
            this.second = second;
            this.attempts = attempts;
        }
    }
}
