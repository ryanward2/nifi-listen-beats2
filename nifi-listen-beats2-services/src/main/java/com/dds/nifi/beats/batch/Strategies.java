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

import com.dds.nifi.beats.model.BeatsEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

public final class Strategies {
    private static final ThreadLocal<MessageDigest> SHA_256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    });

    private Strategies() {
    }

    public static BatchKeyStrategy create(
            final BatchingStrategy strategy,
            final List<String> pointers,
            final MissingKeyPolicy missingKeyPolicy,
            final String defaultBucket,
            final int maximumKeyLength,
            final boolean hashKeys) {

        return switch (strategy) {
            case NONE -> event -> new BatchKey(event.connection().id() + ":" + Long.toUnsignedString(event.sequence()));
            case PER_SOURCE -> event -> normalize(event.remoteAddress(), defaultBucket, maximumKeyLength, hashKeys);
            case CONNECTION -> event -> new BatchKey(event.connection().id().toString());
            case WINDOW -> event -> new BatchKey(event.connection().id() + ":" + Long.toUnsignedString(event.windowId()));
            case PER_AGENT_TYPE -> jsonStrategy(List.of("/agent/type"), missingKeyPolicy, defaultBucket, maximumKeyLength, hashKeys);
            case JSON_KV -> jsonStrategy(pointers, missingKeyPolicy, defaultBucket, maximumKeyLength, hashKeys);
            case SIZE_TIME -> event -> new BatchKey("all");
            case HYBRID -> pointers.isEmpty()
                    ? jsonStrategy(List.of("/agent/type"), missingKeyPolicy, defaultBucket, maximumKeyLength, hashKeys)
                    : jsonStrategy(pointers, missingKeyPolicy, defaultBucket, maximumKeyLength, hashKeys);
        };
    }

    private static BatchKeyStrategy jsonStrategy(
            final List<String> pointers,
            final MissingKeyPolicy missingKeyPolicy,
            final String defaultBucket,
            final int maximumKeyLength,
            final boolean hashKeys) {

        final JsonPointerExtractor extractor = new JsonPointerExtractor(pointers);
        return event -> {
            final List<String> preExtracted = event.jsonBatchValues();
            final List<String> values = preExtracted != null && preExtracted.size() == pointers.size()
                    ? preExtracted
                    : extractor.extract(event.payload());
            if (values.stream().anyMatch(value -> value == null || value.isBlank())) {
                if (missingKeyPolicy == MissingKeyPolicy.REJECT) {
                    throw new IllegalArgumentException("Required JSON batch key is missing");
                }
                return normalize(defaultBucket, defaultBucket, maximumKeyLength, hashKeys);
            }
            return normalize(String.join("", values), defaultBucket, maximumKeyLength, hashKeys);
        };
    }

    private static BatchKey normalize(
            final String input,
            final String defaultBucket,
            final int maximumKeyLength,
            final boolean hashKeys) {

        final String value = input == null || input.isBlank() ? defaultBucket : input.trim();
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (hashKeys || bytes.length > maximumKeyLength) {
            final MessageDigest digest = SHA_256.get();
            digest.reset();
            return new BatchKey(HexFormat.of().formatHex(digest.digest(bytes)));
        }
        return new BatchKey(value);
    }
}
