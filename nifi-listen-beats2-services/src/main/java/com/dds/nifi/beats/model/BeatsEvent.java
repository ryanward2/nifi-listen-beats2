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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public record BeatsEvent(
        ConnectionToken connection,
        long sequence,
        long windowId,
        boolean windowComplete,
        byte protocolVersion,
        EventPayload payload,
        String remoteAddress,
        int remotePort,
        String tlsSubject,
        String tlsIssuer,
        long receivedNanos,
        String agentType,
        List<String> jsonBatchValues) {

    public BeatsEvent {
        connection = Objects.requireNonNull(connection, "connection");
        payload = Objects.requireNonNull(payload, "payload");
        remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
        jsonBatchValues = jsonBatchValues == null ? null
                : Collections.unmodifiableList(new ArrayList<>(jsonBatchValues));
        sequence &= 0xFFFF_FFFFL;
    }

    /** Backward-compatible constructor used by tests and non-filtering call sites. */
    public BeatsEvent(
            final ConnectionToken connection,
            final long sequence,
            final long windowId,
            final boolean windowComplete,
            final byte protocolVersion,
            final EventPayload payload,
            final String remoteAddress,
            final int remotePort,
            final String tlsSubject,
            final String tlsIssuer,
            final long receivedNanos,
            final String agentType) {
        this(connection, sequence, windowId, windowComplete, protocolVersion, payload,
                remoteAddress, remotePort, tlsSubject, tlsIssuer, receivedNanos, agentType, null);
    }
}
