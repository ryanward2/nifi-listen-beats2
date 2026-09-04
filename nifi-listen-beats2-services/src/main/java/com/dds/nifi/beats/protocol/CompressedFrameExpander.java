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

package com.dds.nifi.beats.protocol;

import com.dds.nifi.beats.model.EventPayload;
import com.dds.nifi.beats.server.ConnectionState;
import com.dds.nifi.beats.server.ProcessorMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Resumable compressed-envelope expander.
 *
 * <p>Inflation uses a fixed 32 KiB scratch buffer and an event-sized cumulation. Each nested frame
 * is fired downstream immediately. The complete decompressed envelope and an envelope-wide list of
 * event payloads are never retained.</p>
 */
public final class CompressedFrameExpander extends ChannelInboundHandlerAdapter {
    private static final int INFLATE_CHUNK_BYTES = 32 * 1024;
    private static final int FRAME_HEADER_BYTES = 10;

    private final ProtocolLimits limits;
    private final ProcessorMetrics metrics;
    private final ConnectionState state;
    private final ArrayDeque<ProcessingFrame> pendingTopLevel = new ArrayDeque<>();

    private ChannelHandlerContext handlerContext;
    private EnvelopeCursor activeEnvelope;
    private boolean draining;

    public CompressedFrameExpander(
            final ProtocolLimits limits,
            final ProcessorMetrics metrics,
            final ConnectionState state) {
        this.limits = limits;
        this.metrics = metrics;
        this.state = state;
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext context) throws Exception {
        handlerContext = context;
        state.streamRetryAction(() -> context.executor().execute(this::drain));
        super.handlerAdded(context);
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        if (!(message instanceof ProcessingFrame processingFrame)) {
            context.fireChannelRead(message);
            return;
        }
        pendingTopLevel.addLast(processingFrame);
        state.streamWorkPresent(true);
        drain();
    }

    /** Runs only on the channel's ordered event-processing executor. */
    private void drain() {
        final ChannelHandlerContext context = handlerContext;
        if (draining || context == null || !context.channel().isActive()) {
            return;
        }
        draining = true;
        try {
            while (context.channel().isActive()) {
                if (state.readSuspended()) {
                    return;
                }

                if (activeEnvelope != null) {
                    final BeatsFrame nested = activeEnvelope.nextFrame();
                    if (nested == null) {
                        activeEnvelope.close();
                        activeEnvelope = null;
                        continue;
                    }
                    context.fireChannelRead(new ProcessingBatch(List.of(nested), ProcessingLease.NO_OP));
                    if (state.readSuspended()) {
                        return;
                    }
                    continue;
                }

                final ProcessingFrame next = pendingTopLevel.pollFirst();
                if (next == null) {
                    state.streamWorkPresent(false);
                    return;
                }
                final BeatsFrame frame = next.wireBatch().frames().getFirst();
                if (frame instanceof CompressedFrame compressed) {
                    activeEnvelope = new EnvelopeCursor(
                            context,
                            compressed,
                            next.reservation(),
                            limits,
                            metrics);
                    continue;
                }

                // Transfer both frame ownership and the processing reservation to the protocol handler.
                context.fireChannelRead(new ProcessingBatch(next.wireBatch().frames(), next.reservation()));
                if (state.readSuspended()) {
                    return;
                }
            }
        } catch (Throwable failure) {
            context.fireExceptionCaught(failure);
        } finally {
            draining = false;
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext context) throws Exception {
        state.clearStreamRetryAction();
        releaseAll();
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext context) throws Exception {
        state.clearStreamRetryAction();
        releaseAll();
        super.handlerRemoved(context);
    }

    private void releaseAll() {
        if (activeEnvelope != null) {
            activeEnvelope.close();
            activeEnvelope = null;
        }
        ProcessingFrame frame;
        while ((frame = pendingTopLevel.pollFirst()) != null) {
            FrameResources.release(frame.wireBatch().frames());
            frame.reservation().release();
        }
        state.streamWorkPresent(false);
    }

    /** One resumable compressed envelope; all methods run on one ordered executor. */
    private static final class EnvelopeCursor implements AutoCloseable {
        private final ChannelHandlerContext context;
        private final CompressedFrame compressedFrame;
        private final com.dds.nifi.beats.server.ProcessingTracker.Reservation reservation;
        private final ProtocolLimits limits;
        private final ProcessorMetrics metrics;
        private final Inflater inflater = new Inflater();
        private final byte[] inflateChunk = new byte[INFLATE_CHUNK_BYTES];
        private final ByteBuf cumulation;
        private final ByteBuffer[] compressedBuffers;

        private int compressedBufferIndex;
        private long totalInflated;
        private int frameCount;
        private boolean completed;
        private boolean closed;

        private EnvelopeCursor(
                final ChannelHandlerContext context,
                final CompressedFrame compressedFrame,
                final com.dds.nifi.beats.server.ProcessingTracker.Reservation reservation,
                final ProtocolLimits limits,
                final ProcessorMetrics metrics) {
            this.context = context;
            this.compressedFrame = compressedFrame;
            this.reservation = reservation;
            this.limits = limits;
            this.metrics = metrics;
            final int maximumCumulation = Math.min(
                    limits.maximumDecompressedBytes(),
                    Math.max(64 * 1024, safeAdd(limits.maximumFrameBytes(), INFLATE_CHUNK_BYTES + FRAME_HEADER_BYTES)));
            this.cumulation = context.alloc().buffer(Math.min(64 * 1024, maximumCumulation), maximumCumulation);
            this.compressedBuffers = compressedFrame.payload().nioBuffers(
                    compressedFrame.payload().readerIndex(), compressedFrame.payload().readableBytes());
            metrics.compressedFrames.increment();
            metrics.compressedBytes.add(compressedFrame.payload().readableBytes());
        }

        /** Returns one complete nested frame, or null when the envelope has completed. */
        private BeatsFrame nextFrame() throws ProtocolException {
            if (completed) {
                return null;
            }

            while (true) {
                final BeatsFrame decoded = decodeOne();
                if (decoded != null) {
                    frameCount++;
                    if (frameCount > limits.maximumFramesPerCompressedEnvelope()) {
                        FrameResources.release(decoded);
                        throw new ProtocolException("Compressed envelope exceeds maximum nested frame count: " + frameCount);
                    }
                    compact(cumulation);
                    return decoded;
                }

                if (inflater.finished()) {
                    validateCompletion();
                    completed = true;
                    metrics.decompressedBytes.add(totalInflated);
                    return null;
                }

                if (inflater.needsInput()) {
                    while (compressedBufferIndex < compressedBuffers.length
                            && !compressedBuffers[compressedBufferIndex].hasRemaining()) {
                        compressedBufferIndex++;
                    }
                    if (compressedBufferIndex >= compressedBuffers.length) {
                        throw new ProtocolException("Compressed frame ended before the deflate stream completed");
                    }
                    inflater.setInput(compressedBuffers[compressedBufferIndex++]);
                }

                final int count;
                try {
                    count = inflater.inflate(inflateChunk);
                } catch (DataFormatException e) {
                    throw new ProtocolException("Invalid compressed Beats frame", e);
                }

                if (count > 0) {
                    totalInflated += count;
                    enforceInflationLimits(compressedFrame.payload().readableBytes(), totalInflated, limits);
                    if (cumulation.writableBytes() < count) {
                        try {
                            cumulation.ensureWritable(count);
                        } catch (RuntimeException e) {
                            throw new ProtocolException("Inflated partial frame exceeds bounded event-sized cumulation", e);
                        }
                    }
                    cumulation.writeBytes(inflateChunk, 0, count);
                } else if (inflater.needsDictionary()) {
                    throw new ProtocolException("Compressed frame requires an unsupported dictionary");
                } else if (!inflater.needsInput() && !inflater.finished()) {
                    throw new ProtocolException("Compressed frame made no inflation progress");
                }
            }
        }

        private BeatsFrame decodeOne() throws ProtocolException {
            if (cumulation.readableBytes() < 2) {
                return null;
            }
            cumulation.markReaderIndex();
            final byte version = cumulation.readByte();
            final byte type = cumulation.readByte();
            if (version != BeatsFrameDecoder.VERSION_2) {
                throw new ProtocolException("Unsupported protocol version in compressed envelope: " + (char) version);
            }

            if (type == BeatsFrameDecoder.WINDOW) {
                if (cumulation.readableBytes() < Integer.BYTES) {
                    cumulation.resetReaderIndex();
                    return null;
                }
                final long window = cumulation.readUnsignedInt();
                return new WindowFrame(version, window);
            }

            if (type == BeatsFrameDecoder.JSON) {
                if (cumulation.readableBytes() < 2 * Integer.BYTES) {
                    cumulation.resetReaderIndex();
                    return null;
                }
                final long sequence = cumulation.readUnsignedInt();
                final long declaredLength = cumulation.readUnsignedInt();
                if (declaredLength > limits.maximumFrameBytes() || declaredLength > Integer.MAX_VALUE) {
                    throw new ProtocolException("Nested JSON frame exceeds configured maximum: " + declaredLength);
                }
                if (cumulation.readableBytes() < (int) declaredLength) {
                    cumulation.resetReaderIndex();
                    return null;
                }

                // The copy occurs on the bounded event-processing executor, never on a socket event loop.
                final ByteBuf payload = context.alloc().buffer((int) declaredLength, (int) declaredLength);
                boolean transferred = false;
                try {
                    payload.writeBytes(cumulation, (int) declaredLength);
                    final JsonFrame frame = new JsonFrame(version, sequence, new EventPayload(payload));
                    transferred = true;
                    return frame;
                } finally {
                    if (!transferred) {
                        payload.release();
                    }
                }
            }

            if (type == BeatsFrameDecoder.COMPRESSED) {
                throw new ProtocolException("Nested compressed frames are not supported");
            }
            throw new ProtocolException("Unsupported nested frame type: " + (char) type);
        }

        private void validateCompletion() throws ProtocolException {
            if (inflater.getRemaining() > 0 || hasTrailingInput(compressedBuffers, compressedBufferIndex)) {
                throw new ProtocolException("Compressed frame contains trailing bytes after the deflate stream");
            }
            if (cumulation.isReadable()) {
                throw new ProtocolException("Incomplete nested frame at end of compressed envelope");
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            inflater.end();
            cumulation.release();
            if (compressedFrame.payload().refCnt() > 0) {
                compressedFrame.payload().release();
            }
            reservation.release();
        }
    }

    private static void enforceInflationLimits(
            final long compressedBytes,
            final long totalInflated,
            final ProtocolLimits limits) throws ProtocolException {
        if (totalInflated > limits.maximumDecompressedBytes()) {
            throw new ProtocolException("Inflated frame exceeds configured limit: " + totalInflated);
        }
        final long ratioLimit = saturatedMultiply(compressedBytes, limits.maximumCompressionRatio());
        if (totalInflated > ratioLimit) {
            throw new ProtocolException("Compressed frame exceeds configured compression ratio");
        }
    }

    private static void compact(final ByteBuf buffer) {
        if (!buffer.isReadable()) {
            buffer.clear();
        } else if (buffer.readerIndex() >= 64 * 1024 && buffer.readerIndex() >= buffer.capacity() / 2) {
            buffer.discardReadBytes();
        }
    }

    private static boolean hasTrailingInput(final ByteBuffer[] buffers, final int start) {
        for (int index = start; index < buffers.length; index++) {
            if (buffers[index].hasRemaining()) {
                return true;
            }
        }
        return false;
    }

    private static int safeAdd(final int left, final int right) {
        return left > Integer.MAX_VALUE - right ? Integer.MAX_VALUE : left + right;
    }

    private static long saturatedMultiply(final long left, final long right) {
        if (left == 0 || right == 0) {
            return 0;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }
}
