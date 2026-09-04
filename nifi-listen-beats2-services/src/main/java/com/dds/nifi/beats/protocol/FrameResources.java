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

/** Centralized idempotent payload release helpers for exceptional and shutdown paths. */
public final class FrameResources {
    private FrameResources() {
    }

    public static void release(final BeatsFrame frame) {
        if (frame instanceof JsonFrame json) {
            json.payload().release();
        } else if (frame instanceof CompressedFrame compressed && compressed.payload().refCnt() > 0) {
            compressed.payload().release();
        }
    }

    public static void release(final Iterable<? extends BeatsFrame> frames) {
        for (BeatsFrame frame : frames) {
            release(frame);
        }
    }

    /** Releases all resources owned by a processing work item. */
    public static void release(final ProcessingFrame frame) {
        if (frame == null) {
            return;
        }
        release(frame.wireBatch().frames());
        frame.reservation().release();
    }

    /** Releases all resources owned by a post-expansion processing batch. */
    public static void release(final ProcessingBatch batch) {
        if (batch == null) {
            return;
        }
        release(batch.frames());
        batch.lease().release();
    }
}
