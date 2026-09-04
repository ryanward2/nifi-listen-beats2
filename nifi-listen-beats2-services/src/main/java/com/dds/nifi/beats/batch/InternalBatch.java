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

import com.dds.nifi.beats.model.BatchReceipts;
import com.dds.nifi.beats.model.BeatsEvent;
import io.netty.buffer.ByteBufAllocator;

final class InternalBatch {
    private final BatchKey key;
    private final long createdNanos;
    private long lastAppendNanos;
    private long payloadBytes;
    private long generation;
    private final BatchReceipts.Builder receipts = BatchReceipts.builder();
    private final BatchContentBuilder content;

    InternalBatch(
            final BatchKey key,
            final long now,
            final ByteBufAllocator allocator,
            final long maximumBatchBytes) {
        this.key = key;
        this.createdNanos = now;
        this.lastAppendNanos = now;
        this.content = new BatchContentBuilder(allocator, maximumBatchBytes);
    }

    /** Copies raw JSON into pooled NDJSON chunks and releases the transient per-event payload. */
    void append(final BeatsEvent event, final long appendNanos) {
        final int bytes = event.payload().length();
        receipts.prepareForAppend(event);
        content.append(event.payload());
        receipts.append(event);
        payloadBytes += bytes;
        lastAppendNanos = appendNanos;
        generation++;
        event.payload().release();
    }

    long expiryNanos(final BatchConfig config) {
        final long ageDeadline = saturatingAdd(createdNanos, config.maximumAge().toNanos());
        final long idleDeadline = saturatingAdd(lastAppendNanos, config.maximumIdle().toNanos());
        return Math.min(ageDeadline, idleDeadline);
    }

    private static long saturatingAdd(final long left, final long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    void release() {
        content.release();
    }

    BatchKey key() { return key; }
    long createdNanos() { return createdNanos; }
    long lastAppendNanos() { return lastAppendNanos; }
    long payloadBytes() { return payloadBytes; }
    int encodedBytes() { return content.encodedBytes(); }
    long generation() { return generation; }
    int eventCount() { return receipts.eventCount(); }
    BatchReceipts.Builder receipts() { return receipts; }
    BatchContentBuilder content() { return content; }
}
