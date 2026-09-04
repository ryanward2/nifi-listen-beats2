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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared-scheduler first-protocol-byte and established-idle timeout guard. */
public final class ConnectionIdleTimeoutHandler extends ChannelInboundHandlerAdapter {
    private final ConnectionState state;
    private final long firstByteTimeoutNanos;
    private final long idleTimeoutNanos;
    private final ScheduledExecutorService scheduler;
    private final ProcessorMetrics metrics;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean protocolDataSeen;
    private volatile boolean waitingForTlsHandshake;
    private volatile long lastActivityNanos;
    private volatile ScheduledFuture<?> timeoutFuture;

    public ConnectionIdleTimeoutHandler(
            final ConnectionState state,
            final Duration firstByteTimeout,
            final Duration idleTimeout,
            final ScheduledExecutorService scheduler,
            final ProcessorMetrics metrics) {
        this.state = Objects.requireNonNull(state, "Connection state required");
        this.firstByteTimeoutNanos = Objects.requireNonNull(firstByteTimeout, "First-byte timeout required").toNanos();
        this.idleTimeoutNanos = Objects.requireNonNull(idleTimeout, "Idle timeout required").toNanos();
        if (firstByteTimeoutNanos <= 0L || idleTimeoutNanos < 0L) {
            throw new IllegalArgumentException("First-byte timeout must be positive and idle timeout cannot be negative");
        }
        this.scheduler = Objects.requireNonNull(scheduler, "Idle timeout scheduler required");
        this.metrics = Objects.requireNonNull(metrics, "Processor metrics required");
    }

    @Override
    public void channelActive(final ChannelHandlerContext context) throws Exception {
        lastActivityNanos = System.nanoTime();
        waitingForTlsHandshake = context.piperoce().get(SslHandler.class) != null;
        if (!waitingForTlsHandshake) {
            schedule(context, initialDelay());
        }
        super.channelActive(context);
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext context, final Object event) throws Exception {
        if (event instanceof SslHandshakeCompletionEvent completion && waitingForTlsHandshake) {
            waitingForTlsHandshake = false;
            lastActivityNanos = System.nanoTime();
            if (completion.isSuccess() && context.channel().isActive() && !closed.get()) {
                schedule(context, initialDelay());
            }
        }
        super.userEventTriggered(context, event);
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
        if (!(message instanceof ByteBuf buffer) || buffer.isReadable()) {
            protocolDataSeen = true;
            lastActivityNanos = System.nanoTime();
        }
        super.channelRead(context, message);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext context) throws Exception {
        closed.set(true);
        cancelTimeout();
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext context) throws Exception {
        closed.set(true);
        cancelTimeout();
        super.handlerRemoved(context);
    }

    private long initialDelay() {
        if (idleTimeoutNanos == 0L) {
            return firstByteTimeoutNanos;
        }
        return Math.min(firstByteTimeoutNanos, idleTimeoutNanos);
    }

    private void schedule(final ChannelHandlerContext context, final long delayNanos) {
        try {
            timeoutFuture = scheduler.schedule(
                    () -> check(context), Math.max(1L, delayNanos), TimeUnit.NANOSECONDS);
        } catch (RuntimeException e) {
            ConnectionCloseTracker.mark(context.channel(), ConnectionCloseReason.INTERNAL_ERROR);
            context.channel().eventLoop().execute(context::close);
        }
    }

    private void check(final ChannelHandlerContext context) {
        if (closed.get() || waitingForTlsHandshake || !context.channel().isActive()) {
            return;
        }

        final long now = System.nanoTime();
        final long elapsed = Math.max(0L, now - lastActivityNanos);
        final long limit = protocolDataSeen ? idleTimeoutNanos : firstByteTimeoutNanos;
        if (limit == 0L) {
            return; // established-idle timeout disabled
        }

        // Never manufacture a reconnect while the listener intentionally holds reads or an ACK.
        if (state.readSuspended() || state.outstanding() > 0L) {
            lastActivityNanos = now;
            schedule(context, limit);
            return;
        }

        if (elapsed >= limit) {
            if (closed.compareAndSet(false, true)) {
                final ConnectionCloseReason reason = protocolDataSeen
                        ? ConnectionCloseReason.IDLE_TIMEOUT : ConnectionCloseReason.FIRST_BYTE_TIMEOUT;
                ConnectionCloseTracker.mark(context.channel(), reason);
                if (protocolDataSeen) {
                    metrics.idleConnectionCloses.increment();
                } else {
                    metrics.firstByteTimeouts.increment();
                }
                context.channel().eventLoop().execute(context::close);
            }
            return;
        }
        schedule(context, limit - elapsed);
    }

    private void cancelTimeout() {
        final ScheduledFuture<?> current = timeoutFuture;
        timeoutFuture = null;
        if (current != null) {
            current.cancel(false);
        }
    }
}
