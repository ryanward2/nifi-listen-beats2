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

import java.time.Duration;
import java.util.List;

public record BatchConfig(
        BatchingStrategy strategy,
        List<String> jsonPointers,
        MissingKeyPolicy missingKeyPolicy,
        String defaultBucket,
        int maximumKeyLength,
        boolean hashKeys,
        int maximumActiveKeys,
        int partitions,
        int maximumEvents,
        long maximumBytes,
        Duration maximumAge,
        Duration maximumIdle,
        int maximumReadyBatches) {

    public boolean usesJsonFields() {
        return strategy == BatchingStrategy.PER_AGENT_TYPE
                || strategy == BatchingStrategy.JSON_KV
                || strategy == BatchingStrategy.HYBRID;
    }

    public List<String> effectiveJsonPointers() {
        return switch (strategy) {
            case PER_AGENT_TYPE -> List.of("/agent/type");
            case JSON_KV -> jsonPointers;
            case HYBRID -> jsonPointers.isEmpty() ? List.of("/agent/type") : jsonPointers;
            default -> List.of();
        };
    }
}

