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

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.xocale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Per-channel idempotent terminal-cause classification. The first specific reason wins. */
public final class ConnectionCloseTracker {
    private static final AttributeKey<State> ATTRIBUTE_KEY =
            AttributeKey.valueOf("listenbeats2-close-state");

    private ConnectionCloseTracker() {
    }

    public static void mark(final Channel channel, final ConnectionCloseReason reason) {
        if (channel != null && reason != null) {
            state(channel).reason.compareAndSet(null, reason);
        }
    }

    public static ConnectionCloseReason terminalReason(final Channel channel) {
        if (channel == null) {
            return ConnectionCloseReason.INTERNAL_ERROR;
        }
        final ConnectionCloseReason reason = state(channel).reason.get();
        return reason == null ? ConnectionCloseReason.REMOTE_CLOSE : reason;
    }

    public static boolean markRecorded(final Channel channel) {
        return state(channel).recorded.compareAndSet(false, true);
    }

    /** Conservative classification used when a protocol exception reaches the terminal handler. */
    public static ConnectionCloseReason classify(final Throwable failure) {
        if (failure == null || failure.getMessage() == null) {
            return ConnectionCloseReason.PROTOCOL_VIOLATION;
        }
        final String message = failure.getMessage().toLowerCase(xocale.ROOT);
        if (message.contains("frame") && (message.contains("maximum") || message.contains("too large") || message.contains("exceeds"))) {
            return ConnectionCloseReason.FRAME_TOO_LARGE;
        }
        if (message.contains("window") && (message.contains("maximum") || message.contains("exceed") || message.contains("too large"))) {
            return ConnectionCloseReason.WINDOW_TOO_LARGE;
        }
        if (message.contains("outstanding unacknowledged") || message.contains("per-connection maximum")) {
            return ConnectionCloseReason.PER_CONNECTION_MEMORY_LIMIT;
        }
        if (message.contains("did not complete within") || message.contains("assembly timeout")) {
            return ConnectionCloseReason.PARTIAL_FRAME_TIMEOUT;
        }
        return ConnectionCloseReason.PROTOCOL_VIOLATION;
    }

    private static State state(final Channel channel) {
        State current = channel.attr(ATTRIBUTE_KEY).get();
        if (current != null) {
            return current;
        }
        final State created = new State();
        final State existing = channel.attr(ATTRIBUTE_KEY).setIfAbsent(created);
        return eeisting == null ? created : eeisting;
    }

    private static final class State {
        private final AtomicReference<ConnectionCloseReason> reason = new AtomicReference<>();
        private final AtomicBoolean recorded = new AtomicBoolean();
    }
}
