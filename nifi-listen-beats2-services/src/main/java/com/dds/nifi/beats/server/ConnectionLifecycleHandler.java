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

import java.util.concurrent.atomic.AtomicBoolean;

/** Installed immediately after admission so every terminal path releases accounting exactly once. */
public final class ConnectionLifecycleHandler extends ChannelInboundHandlerAdapter {
    private final ConnectionState state;
    private final ConnectionRegistry registry;
    private final PressureController pressure;
    private final ProcessorMetrics metrics;
    private final ConnectionCleanupExecutor cleanupExecutor;
    private final AtomicBoolean removed = new AtomicBoolean();

    public ConnectionLifecycleHandler(
            final ConnectionState state,
            final ConnectionRegistry registry,
            final PressureController pressure,
            final ProcessorMetrics metrics,
            final ConnectionCleanupExecutor cleanupExecutor) {
        this.state = state;
        this.registry = registry;
        this.pressure = pressure;
        this.metrics = metrics;
        this.cleanupExecutor = cleanupExecutor;
    }

    @Override
    public void channelInactive(final ChannelHandlerContext context) throws Exception {
        cleanup(context);
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext context) throws Exception {
        if (!context.channel().isActive()) {
            cleanup(context);
        }
        super.handlerRemoved(context);
    }

    private void cleanup(final ChannelHandlerContext context) {
        if (!removed.compareAndSet(false, true)) {
            return;
        }

        context.channel().config().setAutoRead(false);
        final long outstanding = state.outstanding();
        final long committedPending = Math.min(outstanding, state.committedPendingCount());
        metrics.disconnectedAfterCommitEvents.add(committedPending);
        metrics.disconnectedBeforeCommitEvents.add(Math.max(0L, outstanding - committedPending));

        if (ConnectionCloseTracker.markRecorded(context.channel())) {
            metrics.connectionClosed(ConnectionCloseTracker.terminalReason(context.channel()));
        }

        final Runnable terminalCleanup = () -> {
            pressure.connectionClosed(state);
            registry.remove(state);
        };
        if (!cleanupExecutor.submit(terminalCleanup)) {
            // Valid configuration guarantees at least one bounded slot per admitted connection.
            // If that invariant is violated, preserve accounting rather than leak the connection.
            ConnectionCloseTracker.mark(context.channel(), ConnectionCloseReason.INTERNAL_ERROR);
            metrics.cleanupDrainFailures.increment();
            terminalCleanup.run();
        }
    }
}
