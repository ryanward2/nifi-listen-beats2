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

import com.dds.nifi.beats.batch.BatchConfig;
import com.dds.nifi.beats.protocol.ProtocolLimits;
import org.apache.nifi.security.util.ClientAuth;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.time.Duration;

public record ServerConfig(
        InetAddress bindAddress,
        int port,
        int workerThreads,
        int eventProcessingThreads,
        int eventProcessingQueueCapacity,
        int eventProcessingQueueHighWaterPercent,
        int eventProcessingQueueLowWaterPercent,
        long eventProcessingMaximumBytes,
        int cleanupWorkerThreads,
        int maximumCleanupPendingTasks,
        int cleanupQueueHighWaterPercent,
        int cleanupQueueLowWaterPercent,
        int socketReceiveBuffer,
        int receiveFrameBuffer,
        int backlog,
        boolean tcpKeepAlive,
        boolean pooledDirectBuffers,
        Duration firstProtocolByteTimeout,
        Duration protocolIdleTimeout,
        Duration frameAssemblyTimeout,
        Duration eventLoopLagProbeInterval,
        int maximumConnections,
        int maximumConnectionsPerSource,
        int maximumConnectionAttemptsPerSecond,
        int maximumConnectionAttemptsPerSourcePerSecond,
        int maximumProtocolFramesPerSecond,
        long maximumQueuedEvents,
        long maximumQueuedBytes,
        int highWaterPercent,
        int lowWaterPercent,
        long maximumEventsPerWindow,
        long maximumOutstandingEventsPerConnection,
        long maximumOutstandingBytesPerConnection,
        Duration acknowledgementWriteTimeout,
        Duration protocolKeepAliveInterval,
        ProtocolLimits protocolLimits,
        BatchConfig batchConfig,
        SSLContext sslContext,
        ClientAuth clientAuth,
        Duration tlsHandshakeTimeout,
        int maximumConcurrentHandshakes) {
}
