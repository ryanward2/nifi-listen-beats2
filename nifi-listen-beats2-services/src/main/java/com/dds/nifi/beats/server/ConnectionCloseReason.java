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

/** Stable, low-cardinality reason codes for terminal connection closure. */
public enum ConnectionCloseReason {
    REMOTE_CLOSE,
    NORMAL_SHUTDOWN,
    TLS_FAILURE,
    TLS_TIMEOUT,
    TLS_LIMIT,
    PROTOCOL_VIOLATION,
    FRAME_TOO_LARGE,
    WINDOW_TOO_LARGE,
    PROCESSING_OVERLOAD,
    CLEANUP_OVERLOAD,
    ACK_TIMEOUT,
    ACK_WRITE_FAILURE,
    GLOBAL_MEMORY_PRESSURE,
    PER_CONNECTION_MEMORY_LIMIT,
    GLOBAL_CONNECTION_LIMIT,
    PER_SOURCE_CONNECTION_LIMIT,
    GLOBAL_CONNECTION_RATE_LIMIT,
    PER_SOURCE_CONNECTION_RATE_LIMIT,
    PROTOCOL_FRAME_RATE_LIMIT,
    FIRST_BYTE_TIMEOUT,
    IDLE_TIMEOUT,
    PARTIAL_FRAME_TIMEOUT,
    INTERNAL_ERROR
}
