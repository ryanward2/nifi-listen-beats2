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

package com.dds.nifi.beats.batch;

import com.dds.nifi.beats.model.BatchContent;
import com.dds.nifi.beats.model.EventPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.ArrayList;
import java.util.List;

/** Builds NDJSON into bounded pooled chunks and avoids retaining one ByteBuf per event. */
final class BatchContentBuilder {
    private static final int CHUNK_BYTES = 64 * 1024;
    private static final int INITIAL_CHUNK_BYTES = 4 * 1024;

    private final ByteBufAllocator allocator;
    private final int chunkBytes;
    private final List<ByteBuf> chunks = new ArrayList<>();
    private int encodedBytes;
    private boolean built;

    BatchContentBuilder(final ByteBufAllocator allocator, final long maximumBatchBytes) {
        this.allocator = allocator;
        this.chunkBytes = (int) Math.max(1L, Math.min(CHUNK_BYTES, maximumBatchBytes));
    }

    void append(final EventPayload payload) {
        if (built) {
            throw new IllegalStateException("Batch content has already been built");
        }
        final int originalChunkCount = chunks.size();
        final int originalLastWriterIndex = originalChunkCount == 0 ? 0 : chunks.getLast().writerIndex();
        final int originalEncodedBytes = encodedBytes;
        final ByteBuf source = payload.retainedDuplicate();
        try {
            while (source.isReadable()) {
                final ByteBuf target = writableChunk(source.readableBytes());
                final int count = Math.min(source.readableBytes(), target.writableBytes());
                target.writeBytes(source, count);
                encodedBytes += count;
            }
            final ByteBuf delimiterTarget = writableChunk(1);
            delimiterTarget.writeByte('\n');
            encodedBytes++;
        } catch (RuntimeException | Error failure) {
            rollbackAppend(originalChunkCount, originalLastWriterIndex, originalEncodedBytes);
            throw failure;
        } finally {
            source.release();
        }
    }

    private void rollbackAppend(
            final int originalChunkCount,
            final int originalLastWriterIndex,
            final int originalEncodedBytes) {
        while (chunks.size() > originalChunkCount) {
            chunks.removeLast().release();
        }
        if (originalChunkCount > 0) {
            chunks.getLast().writerIndex(originalLastWriterIndex);
        }
        encodedBytes = originalEncodedBytes;
    }
    BatchContent build() {
        if (built) {
            throw new IllegalStateException("Batch content has already been built");
        }
        built = true;
        return new BatchContent(List.copyOf(chunks), encodedBytes);
    }

    void release() {
        if (!built) {
            built = true;
            for (ByteBuf chunk : chunks) {
                chunk.release();
            }
        }
    }

    int encodedBytes() {
        return encodedBytes;
    }

    private ByteBuf writableChunk(final int minimumWritable) {
        if (minimumWritable <= 0) {
            throw new IllegalArgumentException("Minimum writable bytes must be positive");
        }
        if (chunks.isEmpty() || !chunks.getLast().isWritable()) {
            final int initialCapacity = Math.min(
                    chunkBytes,
                    Math.max(INITIAL_CHUNK_BYTES, Math.min(minimumWritable, chunkBytes)));
            chunks.add(allocator.buffer(initialCapacity, chunkBytes));
        }
        return chunks.getLast();
    }
}
