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

import com.dds.nifi.beats.protocol.FrameResources;
import com.dds.nifi.beats.protocol.ProcessingFrame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.EventExecutor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ownership-safe bridge from a socket event loop to one ordered processing executor.
 * Submission and completion update pressure synchronously instead of waiting for a poll.
 */
public final class ProcessingOffloadBridge extends ChannelInboundHandlerAdapter {
    private final EventExecutor processingExecutor;
    private final ConnectionState state;
    private final ProcessorMetrics metrics;
    private final PressureController pressure;
    private final ProcessingQueueTracker queueTracker;
    private final AtomicBoolean terminated = new AtomicBoolean();

    public ProcessingOffloadBridge(
            final EventExecutor processingExecutor,
            final ConnectionState state,
            final ProcessorMetrics metrics,
            final PressureController pressure,
            final ProcessingQueueTracker queueTracker) {
        this.processingExecutor = processingExecutor;
        this.state = state;
        this.metrics = metrics;
        this.pressure = pressure;
        this.queueTracker = queueTracker;
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        if (!(message instanceof ProcessingFrame frame)) {
            context.fireChannelRead(message);
            return;
        }

        metrics.processingOffloadSubmitted.increment();
        queueTracker.beforeSubmission(processingExecutor);
        try {
            processingExecutor.execute(() -> {
                try {
                    if (terminated.get() || !context.channel().isActive()) {
                        FrameResources.release(frame);
                        metrics.processingOffloadCancelled.increment();
                        return;
                    }
                    context.fireChannelRead(frame);
                } finally {
                    queueTracker.taskCompleted(processingExecutor);
                }
            });
        } catch (RuntimeException rejection) {
            queueTracker.submissionRejected(processingExecutor);
            FrameResources.release(frame);
            metrics.processingExecutorRejections.increment();
            metrics.overloadConnectionCloses.increment();
            pressure.suspend(state, PressureReason.PROCESSING_EXECUTOR_QUEUE);
            ConnectionCloseTracker.mark(context.channel(), ConnectionCloseReason.PROCESSING_OVERLOAD);
            context.close();
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext context) throws Exception {
        terminated.set(true);
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext context) throws Exception {
        terminated.set(true);
        super.handlerRemoved(context);
    }
}
