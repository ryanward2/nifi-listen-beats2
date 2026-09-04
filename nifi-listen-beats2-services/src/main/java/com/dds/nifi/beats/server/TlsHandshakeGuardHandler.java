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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nonblocking listener-wide TLS handshake concurrency and timeout guard.
 *
 * <p>A timeout records the outcome and requests channel close, but the live-handshake lease remains
 * held until the SSL handshake future or {@code channelInactive()} confirms terminal completion.</p>
 */
public final class TlsHandshakeGuardHandler extends ChannelInboundHandlerAdapter {
    private final SslHandler sslHandler;
    private final AtomicLong concurrentHandshakes;
    private final long maximumConcurrentHandshakes;
    private final long handshakeTimeoutNanos;
    private final ScheduledExecutorService timeoutExecutor;
    private final ProcessorMetrics metrics;
    private final AtomicBoolean outcomeRecorded = new AtomicBoolean();
    private final AtomicBoolean slotReleased = new AtomicBoolean();
    private final AtomicBoolean slotAcquired = new AtomicBoolean();
    private volatile long startedNanos;
    private volatile ScheduledFuture<?> timeoutFuture;

    public TlsHandshakeGuardHandler(
            final SslHandler sslHandler,
            final AtomicLong concurrentHandshakes,
            final int maximumConcurrentHandshakes,
            final Duration handshakeTimeout,
            final ScheduledExecutorService timeoutExecutor,
            final ProcessorMetrics metrics) {
        this.sslHandler = Objects.requireNonNull(sslHandler, "SSL handler required");
        this.concurrentHandshakes = Objects.requireNonNull(concurrentHandshakes, "Handshake counter required");
        if (maximumConcurrentHandshakes <= 0) {
            throw new IllegalArgumentException("Maximum concurrent handshakes must be positive");
        }
        this.maximumConcurrentHandshakes = maximumConcurrentHandshakes;
        this.handshakeTimeoutNanos = Objects.requireNonNull(handshakeTimeout, "Handshake timeout required").toNanos();
        if (handshakeTimeoutNanos <= 0L) {
            throw new IllegalArgumentException("TLS handshake timeout must be positive");
        }
        this.timeoutExecutor = Objects.requireNonNull(timeoutExecutor, "TLS timeout executor required");
        this.metrics = Objects.requireNonNull(metrics, "Processor metrics required");
    }

    @Override
    public void channelActive(final ChannelHandlerContext context) throws Exception {
        final long current = tryAcquireSlot();
        if (current < 0L) {
            metrics.tlsHandshakeRejected.increment();
            ConnectionCloseTracker.mark(context.channel(), ConnectionCloseReason.TLS_LIMIT);
            context.close();
            return;
        }

        slotAcquired.set(true);
        startedNanos = System.nanoTime();
        metrics.tlsHandshakeStarted.increment();
        metrics.tlsConcurrentHandshakes.set(current);
        metrics.tlsPeakConcurrentHandshakes.accumulateAndGet(current, Math::max);

        try {
            timeoutFuture = timeoutExecutor.schedule(() -> {
                if (!outcomeRecorded.compareAndSet(false, true)) {
                    return;
                }
                metrics.tlsHandshakeTimeouts.increment();
                metrics.recordTlsHandshakeLatency(System.nanoTime() - startedNanos);
                ConnectionCloseTracker.mark(context.channel(), ConnectionCloseReason.TLS_TIMEOUT);
                // Do not release the slot here. The channel/SSL engine is still live until terminal.
                context.close();
            }, handshakeTimeoutNanos, TimeUnit.NANOSECONDS);
        } catch (RuntimeException e) {
            recordFailure(context, e);
            return;
        }

        sslHandler.handshakeFuture().addListener(future -> {
            cancelTimeout();
            final long latency = Math.max(0L, System.nanoTime() - startedNanos);
            if (outcomeRecorded.compareAndSet(false, true)) {
                metrics.recordTlsHandshakeLatency(latency);
                if (future.isSuccess()) {
                    metrics.tlsHandshakeSucceeded.increment();
                } else {
                    metrics.tlsHandshakeFailed.increment();
                    ConnectionCloseTracker.mark(context.channel(), ConnectionCloseReason.TLS_FAILURE);
                    context.close();
                }
            }
            releaseSlot();
        });

        super.channelActive(context);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext context) throws Exception {
        cancelTimeout();
        if (slotAcquired.get()) {
            if (outcomeRecorded.compareAndSet(false, true)) {
                metrics.tlsHandshakeFailed.increment();
                metrics.recordTlsHandshakeLatency(Math.max(0L, System.nanoTime() - startedNanos));
            }
            releaseSlot();
        }
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext context) throws Exception {
        cancelTimeout();
        if (!context.channel().isActive()) {
            releaseSlot();
        }
        super.handlerRemoved(context);
    }

    private long tryAcquireSlot() {
        while (true) {
            final long current = concurrentHandshakes.get();
            if (current >= maximumConcurrentHandshakes) {
                return -1L;
            }
            if (concurrentHandshakes.compareAndSet(current, current + 1L)) {
                return current + 1L;
            }
        }
    }

    private void recordFailure(final ChannelHandlerContext context, final RuntimeException failure) {
        if (outcomeRecorded.compareAndSet(false, true)) {
            metrics.tlsHandshakeFailed.increment();
            metrics.recordTlsHandshakeLatency(Math.max(0L, System.nanoTime() - startedNanos));
        }
        ConnectionCloseTracker.mark(context.channel(), ConnectionCloseReason.INTERNAL_ERROR);
        releaseSlot();
        context.close();
    }

    private void releaseSlot() {
        if (!slotAcquired.get() || !slotReleased.compareAndSet(false, true)) {
            return;
        }
        final long updated = concurrentHandshakes.updateAndGet(value -> Math.max(0L, value - 1L));
        metrics.tlsConcurrentHandshakes.set(updated);
    }

    private void cancelTimeout() {
        final ScheduledFuture<?> current = timeoutFuture;
        timeoutFuture = null;
        if (current != null) {
            current.cancel(false);
        }
    }
}
