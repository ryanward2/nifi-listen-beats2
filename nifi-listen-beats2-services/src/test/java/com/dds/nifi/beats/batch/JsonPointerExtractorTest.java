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

import com.dds.nifi.beats.model.EventPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonPointerExtractorTest {
    @Test
    void extractsOnlyConfiguredScalars() throws Exception {
        final JsonPointerExtractor extractor = new JsonPointerExtractor(List.of("/agent/type", "/event/dataset", "/missing"));
        final byte[] json = "{\"agent\":{\"type\":\"winlogbeat\",\"version\":\"9.0\"},\"event\":{\"dataset\":\"windows.security\"}}"
                .getBytes(StandardCharsets.UTF_8);
        final EventPayload payload = new EventPayload(Unpooled.wrappedBuffer(json));
        try {
            final List<String> values = extractor.extract(payload);
            assertEquals("winlogbeat", values.get(0));
            assertEquals("windows.security", values.get(1));
            assertNull(values.get(2));
        } finally {
            payload.release();
        }
    }
    @Test
    void extractsFromPreviouslyParsedTreeWithoutReReadingPayload() throws Exception {
        final JsonPointerExtractor extractor = new JsonPointerExtractor(List.of("/agent/type", "/event/dataset", "/missing"));
        final var root = new ObjectMapper().readTree(
                "{\"agent\":{\"type\":\"winlogbeat\"},\"event\":{\"dataset\":\"windows.security\"}}");

        final List<String> values = extractor.extract(root);
        assertEquals("winlogbeat", values.get(0));
        assertEquals("windows.security", values.get(1));
        assertNull(values.get(2));
    }

    @Test
    void validatesRemainderAfterAllConfiguredValuesAreFound() {
        final JsonPointerExtractor extractor = new JsonPointerExtractor(List.of("/agent/type"));
        final byte[] malformed = "{\"agent\":{\"type\":\"winlogbeat\"},\"broken\":[1,2}"
                .getBytes(StandardCharsets.UTF_8);
        final EventPayload payload = new EventPayload(Unpooled.wrappedBuffer(malformed));
        try {
            assertThrows(IOException.class, () -> extractor.extract(payload));
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsTruncatedObjectInsteadOfLoopingAtEndOfInput() {
        final JsonPointerExtractor extractor = new JsonPointerExtractor(List.of("/agent/type"));
        final byte[] malformed = "{\"agent\":{\"type\":\"winlogbeat\"}"
                .getBytes(StandardCharsets.UTF_8);
        final EventPayload payload = new EventPayload(Unpooled.wrappedBuffer(malformed));
        try {
            assertThrows(IOException.class, () -> extractor.extract(payload));
        } finally {
            payload.release();
        }
    }

}
