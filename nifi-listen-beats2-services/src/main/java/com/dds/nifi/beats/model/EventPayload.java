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

package com.dds.nifi.beats.model;

import com.dds.nifi.beats.api.EventContent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reference-counted pooled event payload. Ownership transfers from the decoder to protocol and
 * batch processing. Raw bytes are copied into pooled NDJSON batch chunks, then this payload is
 * released; only the bounded batch chunks remain until the claim commits or is discarded.
 */
public final class EventPayload implements EventContent {
    private final ByteBuf buffer;
    private final AtomicBoolean released = new AtomicBoolean();

    public EventPayload(final ByteBuf buffer) {
        this.buffer = Objects.requireNonNull(buffer, "buffer");
    }

    @Override
    public int length() {
        return buffer.readableBytes();
    }

    @Override
    public void writeTo(final OutputStream output) throws IOException {
        ensureAccessible();
        buffer.getBytes(buffer.readerIndex(), output, buffer.readableBytes());
    }

    public InputStream openStream() {
        ensureAccessible();
        return new ByteBufInputStream(buffer.duplicate(), false);
    }

    public ByteBuf retainedDuplicate() {
        ensureAccessible();
        return buffer.retainedDuplicate();
    }

    public void release() {
        if (released.compareAndSet(false, true)) {
            buffer.release();
        }
    }

    public boolean isReleased() {
        return released.get();
    }

    private void ensureAccessible() {
        if (released.get() || buffer.refCnt() <= 0) {
            throw new IllegalStateException("Event payload has been released");
        }
    }
}
