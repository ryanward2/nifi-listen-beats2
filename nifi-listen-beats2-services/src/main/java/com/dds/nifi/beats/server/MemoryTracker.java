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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class MemoryTracker {
    private final long maximumEvents;
    private final long maximumBytes;
    private final long highWaterEvents;
    private final long lowWaterEvents;
    private final long highWaterBytes;
    private final long lowWaterBytes;
    private final AtomicLong events = new AtomicLong();
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicBoolean globalPressure = new AtomicBoolean();
    private volatile Runnable capacityListener = () -> { };

    public MemoryTracker(final long maximumEvents, final long maximumBytes, final int highWaterPercent, final int lowWaterPercent) {
        this.maximumEvents = maximumEvents;
        this.maximumBytes = maximumBytes;
        this.highWaterEvents = Math.max(1, maximumEvents * highWaterPercent / 100);
        this.lowWaterEvents = maximumEvents * lowWaterPercent / 100;
        this.highWaterBytes = Math.max(1, maximumBytes * highWaterPercent / 100);
        this.lowWaterBytes = maximumBytes * lowWaterPercent / 100;
    }

    public void capacityListener(final Runnable listener) {
        capacityListener = listener == null ? () -> { } : listener;
    }

    public boolean tryReserve(final long eventBytes) {
        if (eventBytes < 0) {
            throw new IllegalArgumentException("Event bytes must be non-negative");
        }
        while (true) {
            final long currentEvents = events.get();
            final long currentBytes = bytes.get();
            if (currentEvents >= maximumEvents || eventBytes > maximumBytes - currentBytes) {
                capacityListener.run();
                return false;
            }
            if (!events.compareAndSet(currentEvents, currentEvents + 1)) {
                continue;
            }
            if (bytes.compareAndSet(currentBytes, currentBytes + eventBytes)) {
                if (highWaterReached()) {
                    capacityListener.run();
                }
                return true;
            }
            events.decrementAndGet();
        }
    }

    public void release(final long eventCount, final long byteCount) {
        if (eventCount < 0 || byteCount < 0) {
            throw new IllegalArgumentException("Released event and byte counts must be non-negative");
        }
        final long updatedEvents = events.addAndGet(-eventCount);
        final long updatedBytes = bytes.addAndGet(-byteCount);
        if (updatedEvents < 0 || updatedBytes < 0) {
            events.addAndGet(eventCount);
            bytes.addAndGet(byteCount);
            throw new IllegalStateException("Accepted-event memory accounting underflow");
        }
        capacityListener.run();
    }

    public boolean highWaterReached() {
        return bytes.get() >= highWaterBytes || events.get() >= highWaterEvents;
    }

    public boolean enterGlobalPressure() {
        return globalPressure.compareAndSet(false, true);
    }

    public boolean belowLowWater() {
        return bytes.get() <= lowWaterBytes && events.get() <= lowWaterEvents;
    }

    public void clearGlobalPressure() { globalPressure.set(false); }
    public boolean globalPressure() { return globalPressure.get(); }
    public boolean hasCapacity() { return events.get() < maximumEvents && bytes.get() < maximumBytes; }
    public long events() { return events.get(); }
    public long bytes() { return bytes.get(); }
}
