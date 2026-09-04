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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Reference-counted sequence of pooled NDJSON chunks. Delimiters are already included. */
public final class BatchContent implements EventContent {
    private final List<ByteBuf> chunks;
    private final int length;
    private final AtomicBoolean released = new AtomicBoolean();

    public BatchContent(final List<ByteBuf> chunks, final int length) {
        this.chunks = List.copyOf(chunks);
        this.length = length;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public void writeTo(final OutputStream output) throws IOException {
        ensureAccessible();
        for (ByteBuf chunk : chunks) {
            chunk.getBytes(chunk.readerIndex(), output, chunk.readableBytes());
        }
    }

    public int chunkCount() {
        return chunks.size();
    }

    public void release() {
        if (released.compareAndSet(false, true)) {
            for (ByteBuf chunk : chunks) {
                chunk.release();
            }
        }
    }

    public boolean isReleased() {
        return released.get();
    }

    private void ensureAccessible() {
        if (released.get()) {
            throw new IllegalStateException("Batch content has been released");
        }
    }
}
