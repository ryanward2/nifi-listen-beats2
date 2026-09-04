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

import com.dds.nifi.beats.batch.BatchCoordinator;
import com.dds.nifi.beats.filter.JelDropFilter;
import com.dds.nifi.beats.protocol.BeatsAckEncoder;
import com.dds.nifi.beats.protocol.BeatsFrameDecoder;
import com.dds.nifi.beats.protocol.CompressedFrameExpander;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;

import javax.net.ssl.SSLEngine;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/** Exception-safe per-channel pipeline construction with pre-TLS admission controls. */
public final class BeatsChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ServerConfig config;
    private final ConnectionRegistry registry;
    private final BatchCoordinator batches;
    private final MemoryTracker memory;
    private final ProcessorMetrics metrics;
    private final PressureController pressure;
    private final AckCoordinator acknowledgements;
    private final JelDropFilter eventFilter;
    private final EventExecutorGroup eventProcessingGroup;
    private final ProcessingTracker processingTracker;
    private final ProcessingQueueTracker processingQueueTracker;
    private final ConnectionCleanupExecutor cleanupExecutor;
    private final ScheduledExecutorService tlsTimeoutExecutor;
    private final ScheduledExecutorService idleTimeoutExecutor;
    private final AtomicLong concurrentTlsHandshakes;
    private final BooleanSupplier acceptsPaused;
    private final BooleanSupplier quiescing;

    public BeatsChannelInitializer(
            final ServerConfig config,
            final ConnectionRegistry registry,
            final BatchCoordinator batches,
            final MemoryTracker memory,
            final ProcessorMetrics metrics,
            final PressureController pressure,
            final AckCoordinator acknowledgements,
            final JelDropFilter eventFilter,
            final EventExecutorGroup eventProcessingGroup,
            final ProcessingTracker processingTracker,
            final ProcessingQueueTracker processingQueueTracker,
            final ConnectionCleanupExecutor cleanupExecutor,
            final ScheduledExecutorService tlsTimeoutExecutor,
            final ScheduledExecutorService idleTimeoutExecutor,
            final AtomicLong concurrentTlsHandshakes,
            final BooleanSupplier acceptsPaused,
            final BooleanSupplier quiescing) {
        this.config = config;
        this.registry = registry;
        this.batches = batches;
        this.memory = memory;
        this.metrics = metrics;
        this.pressure = pressure;
        this.acknowledgements = acknowledgements;
        this.eventFilter = eventFilter == null ? JelDropFilter.disabled() : eventFilter;
        this.eventProcessingGroup = eventProcessingGroup;
        this.processingTracker = processingTracker;
        this.processingQueueTracker = processingQueueTracker;
        this.cleanupExecutor = cleanupExecutor;
        this.tlsTimeoutExecutor = tlsTimeoutExecutor;
        this.idleTimeoutExecutor = idleTimeoutExecutor;
        this.concurrentTlsHandshakes = concurrentTlsHandshakes;
        this.acceptsPaused = acceptsPaused;
        this.quiescing = quiescing;
    }

    @Override
    protected void initChannel(final SocketChannel channel) {
        if (quiescing.getAsBoolean()) {
            rejectBeforeAdmission(channel, ConnectionCloseReason.NORMAL_SHUTDOWN);
            return;
        }
        if (cleanupExecutor.isAdmissionPaused()) {
            rejectBeforeAdmission(channel, ConnectionCloseReason.CLEANUP_OVERLOAD);
            return;
        }
        if (acceptsPaused.getAsBoolean()) {
            rejectBeforeAdmission(channel, ConnectionCloseReason.GLOBAL_MEMORY_PRESSURE);
            return;
        }

        final ConnectionRegistry.Admission admission = registry.admit(channel);
        if (!admission.accepted()) {
            recordPreAdmissionClose(channel, admission.rejectionReason());
            channel.close();
            return;
        }
        final ConnectionState state = admission.state();

        boolean lifecycleInstalled = false;
        boolean initialized = false;
        try {
            channel.pipeline().addLast("connection-lifecycle",
                    new ConnectionLifecycleHandler(state, registry, pressure, metrics, cleanupExecutor));
            lifecycleInstalled = true;

            if (config.sslContext() != null) {
                final SSLEngine engine = config.sslContext().createSSLEngine();
                engine.setUseClientMode(false);
                switch (config.clientAuth()) {
                    case REQUIRED -> engine.setNeedClientAuth(true);
                    case WANT -> engine.setWantClientAuth(true);
                    default -> { }
                }
                final SslHandler sslHandler = new SslHandler(engine);
                // External guard owns timeout semantics so a timed-out live engine keeps its slot
                // until handshake completion or channelInactive confirms terminal release.
                sslHandler.setHandshakeTimeout(0L, TimeUnit.MILLISECONDS);
                channel.pipeline().addLast("tls-guard", new TlsHandshakeGuardHandler(
                        sslHandler,
                        concurrentTlsHandshakes,
                        config.maximumConcurrentHandshakes(),
                        config.tlsHandshakeTimeout(),
                        tlsTimeoutExecutor,
                        metrics));
                channel.pipeline().addLast("tls", sslHandler);
            }

            channel.pipeline().addLast("connection-idle", new ConnectionIdleTimeoutHandler(
                    state,
                    config.firstProtocolByteTimeout(),
                    config.protocolIdleTimeout(),
                    idleTimeoutExecutor,
                    metrics));
            channel.pipeline().addLast("beats-decoder", new BeatsFrameDecoder(
                    config.protocolLimits(),
                    processingTracker,
                    state,
                    metrics,
                    pressure,
                    config.frameAssemblyTimeout(),
                    (eventFilter.isEnabled() || config.batchConfig().usesJsonFields())
                            ? BeatsFrameDecoder.STREAMING_JSON_WORKING_SET_MULTIPLIER
                            : BeatsFrameDecoder.RAW_JSON_WORKING_SET_MULTIPLIER));
            channel.pipeline().addLast("beats-ack", new BeatsAckEncoder());

            final EventExecutor processingExecutor = eventProcessingGroup.next();
            channel.pipeline().addLast("beats-processing-offload",
                    new ProcessingOffloadBridge(
                            processingExecutor, state, metrics, pressure, processingQueueTracker));
            channel.pipeline().addLast(processingExecutor, "beats-expand",
                    new CompressedFrameExpander(config.protocolLimits(), metrics, state)hannel.pipeline().addLast(processingExecutor, "beats-expand",
       handler", new BeatsProtocolHandler(
                    state,
                    batches,
                    memory,
                    metrics,
                    pressure,
                    acknowledgements,
                    eventFilter,
                    config.maximumEventsPerWindow(),
                    config.maximumOutstandingEventsPerConnection(),
                    config.maximumOutstandingBytesPerConnection(),
                    config.maximumProtocolFramesPerSecond()));
            initialized = true;
        } finally {
            if (!initialized) {
                ConnectionCloseTracker.mark(channel, ConnectionCloseReason.INTERNAL_ERROR);
                if (!lifecycleInstalled) {
                    registry.remove(state);
                }
                channel.close();
            }
        }
    }
    private void rejectBeforeAdmission(final SocketChannel channel, final ConnectionCloseReason reason) {
        metrics.rejectedConnections.increment();
        recordPreAdmissionClose(channel, reason);
        channel.close();
    }

    private void recordPreAdmissionClose(final SocketChannel channel, final ConnectionCloseReason reason) {
        ConnectionCloseTracker.mark(channel, reason);
        // Pre-admission channels intentionally do not install the lifecycle/cleanup pipeline.
        // Record the stable close reason directly so rejection floods remain visible without
        // consuming general disconnect-cleanup capacity.
        if (ConnectionCloseTracker.markRecorded(channel)) {
            metrics.connectionClosed(reason);
        }
    }

}
