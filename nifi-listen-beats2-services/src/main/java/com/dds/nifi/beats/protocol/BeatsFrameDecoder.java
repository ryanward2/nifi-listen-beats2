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
import com.dds.nifi.beats.server.ConnectionCloseReason;
import com.dds.nifi.beats.server.ConnectionCloseTracker;
import com.dds.nifi.beats.server.PressureController;
import com.dds.nifi.beats.server.PressureReason;
import com.dds.nifi.beats.server.ProcessingTracker;
import com.dds.nifi.beats.server.ProcessorMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Stateful Lumberjack v2 decoder.
 *
 * <p>The declared body size is reserved immediately after a complete frame header is available,
 * before the body is allowed to accumulate in Netty. The complete body is transferred using a
 * retained slice, so large copies never run on the socket event-loop thread.</p>
 */
public final class BeatsFrameDecoder extends ByteToMessageDecoder {
    public static final byte VERSION_2 = '2';
    public static final byte WINDOW = 'W';
    public static final byte JSON = 'J';
    public static final byte COMPRESSED = 'C';

    private static final long WINDOW_RESERVATION_BYTES = 16L;
    private static final long FRAME_ACCOUNTING_OVERHEAD = 64L * 1024L;
    private static final long COMPRESSED_WORKING_OVERHEAD = 128L * 1024L;
    public static final int RAW_JSON_WORKING_SET_MULTIPLIER = 2;
    public static final int STREAMING_JSON_WORKING_SET_MULTIPLIER = 4;

    private enum DecodeState { FRAME_HEADER, JSON_BODY, COMPRESSED_BODY }

    private final ProtocolLimits limits;
    private final ProcessingTracker processingTracker;
    private final ConnectionState connectionState;
    private final ProcessorMetrics metrics;
    private final PressureController pressure;
    private final Duration frameAssemblyTimeout;
    private final int jsonWorkingSetMultiplier;

    private DecodeState decodeState = DecodeState.FRAME_HEADER;
    private byte pendingVersion;
    private long pendingSequence;
    private int pendingLength;
    private ProcessingTracker.Reservation pendingReservation;
    private boolean deferredForProcessingCapacity;
    private ChannelHandlerContext handlerContext;
    private ScheduledFuture<?> assemblyTimeoutFuture;

    public BeatsFrameDecoder(
            final ProtocolLimits limits,
            final ProcessingTracker processingTracker,
            final ConnectionState connectionState,
            final ProcessorMetrics metrics,
            final PressureController pressure,
            final Duration frameAssemblyTimeout) {
        this(limits, processingTracker, connectionState, metrics, pressure, frameAssemblyTimeout,
                RAW_JSON_WORKING_SET_MULTIPLIER);
    }

    public BeatsFrameDecoder(
            final ProtocolLimits limits,
            final ProcessingTracker processingTracker,
            final ConnectionState connectionState,
            final ProcessorMetrics metrics,
            final PressureController pressure,
            final Duration frameAssemblyTimeout,
            final int jsonWorkingSetMultiplier) {
        if (jsonWorkingSetMultiplier < RAW_JSON_WORKING_SET_MULTIPLIER) {
            throw new IllegalArgumentException("JSON working-set multiplier must be at least "
                    + RAW_JSON_WORKING_SET_MULTIPLIER);
        }
        this.limits = limits;
        this.processingTracker = processingTracker;
        this.connectionState = connectionState;
        this.metrics = metrics;
        this.pressure = pressure;
        this.frameAssemblyTimeout = frameAssemblyTimeout;
        this.jsonWorkingSetMultiplier = jsonWorkingSetMultiplier;
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext context) throws Exception {
        super.handlerAdded(context);
        handlerContext = context;
        connectionState.decoderRetryAction(() -> context.executor().execute(() -> retryBufferedDecode(context)));
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext context) throws Exception {
        releasePendingReservation();
        connectionState.clearDecoderRetryAction();
        super.handlerRemoved(context);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext context) throws Exception {
        releasePendingReservation();
        connectionState.clearDecoderRetryAction();
        super.channelInactive(context);
    }

    @Override
    protected void decode(final ChannelHandlerContext context, final ByteBuf input, final List<Object> output) throws Exception {
        if (decodeState == DecodeState.JSON_BODY) {
            finishJson(context, input, output);
            return;
        }
        if (decodeState == DecodeState.COMPRESSED_BODY) {
            finishCompressed(input, output);
            return;
        }

        if (input.readableBytes() < 2) {
            return;
        }

        final int reader = input.readerIndex();
        final byte version = input.getByte(reader);
        final byte type = input.getByte(reader + 1);
        if (version != VERSION_2) {
            throw new ProtocolException("Unsupported Beats protocol version byte: " + (char) version);
        }

        if (type == WINDOW) {
            decodeWindow(input, output, version);
        } else if (type == JSON) {
            beginJson(context, input, output, version);
        } else if (type == COMPRESSED) {
            beginCompressed(context, input, output, version);
        } else {
            throw new ProtocolException("Unsupported Beats v2 frame type: " + (char) type);
        }
    }

    private void decodeWindow(final ByteBuf input, final List<Object> output, final byte version) throws ProtocolException {
        if (input.readableBytes() < 2 + Integer.BYTES) {
            return;
        }
        final ProcessingTracker.Reservation reservation = processingTracker.tryReserve(WINDOW_RESERVATION_BYTES);
        if (reservation == null) {
            deferUnreadFrame(WINDOW_RESERVATION_BYTES, true);
            return;
        }

        boolean transferred = false;
        try {
            input.skipBytes(2);
            final long window = input.readUnsignedInt();
            clearProcessingPressure();
            output.add(new ProcessingFrame(new WireFrameBatch(List.of(new WindowFrame(version, window))), reservation));
            transferred = true;
        } finally {
            if (!transferred) {
                reservation.release();
            }
        }
    }

    private void beginJson(
            final ChannelHandlerContext context,
            final ByteBuf input,
            final List<Object> output,
            final byte version) throws ProtocolException {
        final int headerBytes = 2 + (2 * Integer.BYTES);
        if (input.readableBytes() < headerBytes) {
            return;
        }

        final int reader = input.readerIndex();
        final long sequence = input.getUnsignedInt(reader + 2);
        final long declaredLength = input.getUnsignedInt(reader + 2 + Integer.BYTES);
        validateJsonLength(declaredLength);

        final long workingSet = jsonWorkingSet(declaredLength);
        if (workingSet > processingTracker.maximumBytes()) {
            throw new ProtocolException("JSON frame working set " + workingSet
                    + " exceeds Maximum Event Processing Bytes " + processingTracker.maximumBytes());
        }
        final ProcessingTracker.Reservation reservation = processingTracker.tryReserveBytes(workingSet);
        if (reservation == null) {
            deferUnreadFrame(workingSet, false);
            return;
        }

        input.skipBytes(headerBytes);
        pendingVersion = version;
        pendingSequence = sequence;
        pendingLength = (int) declaredLength;
        pendingReservation = reservation;
        decodeState = DecodeState.JSON_BODY;
        metrics.partialFrameReservations.increment();
        metrics.partialFrameReservedBytes.addAndGet(workingSet);
        scheduleAssemblyTimeout(context);
        finishJson(context, input, output);
    }

    private void finishJson(
            final ChannelHandlerContext context,
            final ByteBuf input,
            final List<Object> output) {
        if (input.readableBytes() < pendingLength) {
            return;
        }
        if (!pendingReservation.tryAcquireTask()) {
            deferUnreadFrame(0L, true);
            return;
        }

        final byte version = pendingVersion;
        final long sequence = pendingSequence;
        final ByteBuf payload = input.readRetainedSlice(pendingLength);
        final ProcessingTracker.Reservation reservation = detachPendingReservation();
        boolean transferred = false;
        try {
            clearProcessingPressure();
            final JsonFrame frame = new JsonFrame(version, sequence, new EventPayload(payload));
            output.add(new ProcessingFrame(new WireFrameBatch(List.of(frame)), reservation));
            transferred = true;
        } finally {
            if (!transferred) {
                payload.release();
                reservation.release();
            }
        }
    }

    private void beginCompressed(
            final ChannelHandlerContext context,
            final ByteBuf input,
            final List<Object> output,
            final byte version) throws ProtocolException {
        final int headerBytes = 2 + Integer.BYTES;
        if (input.readableBytes() < headerBytes) {
            return;
        }

        final int reader = input.readerIndex();
        final long declaredLength = input.getUnsignedInt(reader + 2);
        validateCompressedLength(declaredLength);
        final long workingSet = compressedWorkingSet(declaredLength);
        if (workingSet > processingTracker.maximumBytes()) {
            throw new ProtocolException("Compressed frame working set " + workingSet
                    + " exceeds Maximum Event Processing Bytes " + processingTracker.maximumBytes());
        }

        final ProcessingTracker.Reservation reservation = processingTracker.tryReserveBytes(workingSet);
        if (reservation == null) {
            deferUnreadFrame(workingSet, false);
            return;
        }

        input.skipBytes(headerBytes);
        pendingVersion = version;
        pendingLength = (int) declaredLength;
        pendingReservation = reservation;
        decodeState = DecodeState.COMPRESSED_BODY;
        metrics.partialFrameReservations.increment();
        metrics.partialFrameReservedBytes.addAndGet(workingSet);
        scheduleAssemblyTimeout(context);
        finishCompressed(input, output);
    }

    private void finishCompressed(final ByteBuf input, final List<Object> output) {
        if (input.readableBytes() < pendingLength) {
            return;
        }
        if (!pendingReservation.tryAcquireTask()) {
            deferUnreadFrame(0L, true);
            return;
        }

        final byte version = pendingVersion;
        final ByteBuf payload = input.readRetainedSlice(pendingLength);
        final ProcessingTracker.Reservation reservation = detachPendingReservation();
        boolean transferred = false;
        try {
            clearProcessingPressure();
            output.add(new ProcessingFrame(
                    new WireFrameBatch(List.of(new CompressedFrame(version, payload))), reservation));
            transferred = true;
        } finally {
            if (!transferred) {
                payload.release();
                reservation.release();
            }
        }
    }

    private ProcessingTracker.Reservation detachPendingReservation() {
        cancelAssemblyTimeout();
        final ProcessingTracker.Reservation reservation = pendingReservation;
        metrics.partialFrameReservedBytes.addAndGet(-reservation.reservedBytes());
        pendingReservation = null;
        pendingLength = 0;
        pendingSequence = 0;
        pendingVersion = 0;
        decodeState = DecodeState.FRAME_HEADER;
        return reservation;
    }

    private void releasePendingReservation() {
        cancelAssemblyTimeout();
        final ProcessingTracker.Reservation reservation = pendingReservation;
        if (reservation != null) {
            metrics.partialFrameReservedBytes.addAndGet(-reservation.reservedBytes());
            reservation.release();
            pendingReservation = null;
        }
        pendingLength = 0;
        pendingSequence = 0;
        pendingVersion = 0;
        decodeState = DecodeState.FRAME_HEADER;
        deferredForProcessingCapacity = false;
        connectionState.decoderWorkPresent(false);
        connectionState.clearProcessingRetryRequirement();
    }

    private void deferUnreadFrame(final long requiredBytes, final boolean taskRequired) {
        deferredForProcessingCapacity = true;
        connectionState.decoderWorkPresent(true);
        connectionState.processingRetryRequirement(requiredBytes, taskRequired);
        pressure.suspend(connectionState, PressureReason.PROCESSING_CAPACITY);
        metrics.processingPressureEvents.increment();
        pressure.signal();
    }

    private void clearProcessingPressure() {
        deferredForProcessingCapacity = false;
        connectionState.decoderWorkPresent(false);
        connectionState.clearProcessingRetryRequirement();
        pressure.clear(connectionState, PressureReason.PROCESSING_CAPACITY);
    }

    /** Re-enters the decoder when a complete frame is already buffered and the sender is awaiting ACK. */
    private void retryBufferedDecode(final ChannelHandlerContext context) {
        if (!deferredForProcessingCapacity || !context.channel().isActive()
                || connectionState.isSuspendedFor(PressureReason.PROCESSING_CAPACITY)) {
            return;
        }
        final ByteBuf trigger = context.alloc().buffer(0, 0);
        try {
            super.channelRead(context, trigger);
        } catch (Exception e) {
            context.fireExceptionCaught(e);
        }
    }

    private void scheduleAssemblyTimeout(final ChannelHandlerContext context) {
        cancelAssemblyTimeout();
        assemblyTimeoutFuture = context.executor().schedule(
                this::onAssemblyTimeout,
                frameAssemblyTimeout.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void onAssemblyTimeout() {
        assemblyTimeoutFuture = null;
        final ChannelHandlerContext context = handlerContext;
        if (pendingReservation == null || context == null || !context.channel().isActive()) {
            return;
        }
        // Listener-induced read suspension must not manufacture reconnects and duplicate retries.
        // The active assembly clock resumes in another timeout interval after pressure clears.
        if (connectionState.readSuspended()) {
            scheduleAssemblyTimeout(context);
            return;
        }
        metrics.frameAssemblyTimeouts.increment();
        ConnectionCloseTracker.mark(context.channel(), ConnectionCloseReason.PARTIAL_FRAME_TIMEOUT);
        context.fireExceptionCaught(new ProtocolException(
                "Declared " + decodeState + " frame did not complete within " + frameAssemblyTimeout));
    }

    private void cancelAssemblyTimeout() {
        final ScheduledFuture<?> future = assemblyTimeoutFuture;
        assemblyTimeoutFuture = null;
        if (future != null) {
            future.cancel(false);
        }
    }

    private long jsonWorkingSet(final long payloadBytes) {
        // ByteToMessageDecoder cumulation can round capacity above the declared length. Raw mode
        // reserves two body equivalents; JEL tree mode reserves a larger conservative multiplier
        // for parser/tree allocations, plus fixed allocator/header overhead.
        return saturatingAdd(
                saturatingMultiply(payloadBytes, jsonWorkingSetMultiplier),
                FRAME_ACCOUNTING_OVERHEAD);
    }

    private long compressedWorkingSet(final long compressedBytes) {
        // Account for top-level compressed cumulation, the event-sized inflated cumulation, and one
        // materialized nested event. Accepted batch content is governed separately by MemoryTracker.
        final long compressedCumulation = saturatingMultiply(compressedBytes, 2L);
        final long nestedCumulationAndEvent = saturatingMultiply(
                limits.maximumFrameBytes(), jsonWorkingSetMultiplier);
        final long boundedNested = Math.min(
                saturatingMultiply(limits.maximumDecompressedBytes(), 2L),
                saturatingAdd(nestedCumulationAndEvent, COMPRESSED_WORKING_OVERHEAD));
        return saturatingAdd(compressedCumulation, boundedNested);
    }

    private void validateJsonLength(final long declaredLength) throws ProtocolException {
        if (declaredLength > limits.maximumFrameBytes()) {
            throw new ProtocolException("JSON frame exceeds maximum frame size: " + declaredLength);
        }
        if (declaredLength > Integer.MAX_VALUE) {
            throw new ProtocolException("JSON frame length exceeds Java buffer limit");
        }
    }

    private void validateCompressedLength(final long declaredLength) throws ProtocolException {
        if (declaredLength > limits.maximumCompressedBytes()) {
            throw new ProtocolException("Compressed frame exceeds configured limit: " + declaredLength);
        }
        if (declaredLength > Integer.MAX_VALUE) {
            throw new ProtocolException("Compressed frame length exceeds Java buffer limit");
        }
    }

    private static long saturatingAdd(final long left, final long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static long saturatingMultiply(final long value, final long multiplier) {
        if (value == 0 || multiplier == 0) {
            return 0;
        }
        return value > Long.MAX_VALUE / multiplier ? Long.MAX_VALUE : value * multiplier;
    }
}
