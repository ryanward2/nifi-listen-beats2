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

package com.dds.nifi.routendjson.expression;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Evaluation state for one JSON event. The original tree-backed constructor is
 * retained for RouteNdjsonEvents compatibility. ListenBeats2 uses the
 * array-backed constructor with values populated by its streaming extractor.
 */
public final class EvaluationContext {
    private final JsonNode root;
    private final Map<String, Value> pathCache;
    private final Map<String, Integer> pathIds;
    private final Value[] extractedValues;

    public EvaluationContext(final JsonNode root) {
        this.root = root;
        this.pathCache = new HashMap<>();
        this.pathIds = null;
        this.extractedValues = null;
    }

    public EvaluationContext(
            final Map<String, Integer> pathIds,
            final Value[] extractedValues) {
        this.root = null;
        this.pathCache = null;
        this.pathIds = Objects.requireNonNull(pathIds, "Path index required");
        this.extractedValues = Objects.requireNonNull(extractedValues, "Extracted values required");
    }

    public JsonNode root() {
        return root;
    }

    public Value path(final String source, final String[] segments) {
        if (pathIds != null) {
            final Integer pathId = pathIds.get(source);
            if (pathId == null || pathId < 0 || pathId >= extractedValues.length) {
                return Value.MISSING;
            }
            final Value value = extractedValues[pathId];
            return value == null ? Value.MISSING : value;
        }
        return pathCache.computeIfAbsent(source, ignored -> resolvePath(segments));
    }

    private Value resolvePath(final String[] segments) {
        if (root == null) {
            return Value.MISSING;
        }
        JsonNode current = root;
        for (String segment : segments) {
            if (current == null || !current.isObject() || !current.has(segment)) {
                return Value.MISSING;
            }
            current = current.get(segment);
        }
        return Value.fromJsonNode(current);
    }
}
