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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Compiled JSON Pointer trie with one complete validating streaming parse. */
public final class JsonPointerExtractor {
    private final JsonFactory jsonFactory = JsonFactory.builder().build();
    private final Node root = new Node();
    private final int pathCount;

    public JsonPointerExtractor(final List<String> pointers) {
        int index = 0;
        for (String pointer : pointers) {
            Node node = root;
            for (String segment : compile(pointer)) {
                node = node.children.computeIfAbsent(segment, ignored -> new Node());
            }
            if (node.targetIndex >= 0) {
                throw new IllegalArgumentException("Duplicate JSON Pointer: " + pointer);
            }
            node.targetIndex = index++;
        }
        pathCount = index;
    }

    public List<String> extract(final EventPayload payload) throws IOException {
        final String[] values = new String[pathCount];
        final boolean[] present = new boolean[pathCount];
        try (InputStream input = payload.openStream(); JsonParser parser = jsonFactory.createParser(input)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("Root JSON value is not an object");
            }
            scanObject(parser, root, values, present);
            if (parser.nextToken() != null) {
                throw new IOException("Trailing content after JSON object");
            }
        }
        return Arrays.asList(values);
    }


    /** Extracts configured pointer values from a tree already parsed by the JEL filter. */
    public List<String> extract(final JsonNode rootNode) throws IOException {
        if (rootNode == null || !rootNode.isObject()) {
            throw new IOException("Root JSON value is not an object");
        }
        final String[] values = new String[pathCount];
        extractTree(root, rootNode, values);
        return Arrays.asList(values);
    }

    private static void extractTree(final Node plan, final JsonNode current, final String[] values) {
        for (Map.Entry<String, Node> entry : plan.children.entrySet()) {
            final JsonNode childValue = current.get(entry.getKey());
            if (childValue == null) {
                continue;
            }
            final Node childPlan = entry.getValue();
            if (childPlan.targetIndex >= 0 && childValue.isValueNode()) {
                values[childPlan.targetIndex] = childValue.isNull() ? null : childValue.asText();
            }
            if (childValue.isObject() && !childPlan.children.isEmpty()) {
                extractTree(childPlan, childValue, values);
            }
        }
    }

    private void scanObject(
            final JsonParser parser,
            final Node parent,
            final String[] values,
            final boolean[] present) throws IOException {
        JsonToken fieldToken;
        while ((fieldToken = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (fieldToken == null) {
                throw new IOException("Unexpected end of JSON object");
            }
            if (fieldToken != JsonToken.FIELD_NAME) {
                throw new IOException("Expected JSON field name, found " + fieldToken);
            }
            final String field = parser.currentName();
            final JsonToken valueToken = parser.nextToken();
            if (valueToken == null) {
                throw new IOException("Unexpected end of JSON value for field " + field);
            }
            final Node child = parent.children.get(field);
            if (child == null) {
                if (valueToken == JsonToken.START_OBJECT || valueToken == JsonToken.START_ARRAY) {
                    parser.skipChildren();
                }
                continue;
            }

            if (child.targetIndex >= 0 && valueToken != null && valueToken.isScalarValue()) {
                if (!present[child.targetIndex]) {
                    present[child.targetIndex] = true;
                    values[child.targetIndex] = valueToken == JsonToken.VALUE_NULL ? null : parser.getValueAsString();
                }
            }

            if (valueToken == JsonToken.START_OBJECT && !child.children.isEmpty()) {
                scanObject(parser, child, values, present);
            } else if (valueToken == JsonToken.START_OBJECT || valueToken == JsonToken.START_ARRAY) {
                parser.skipChildren();
            }
        }
    }

    private static List<String> compile(final String pointer) {
        if (pointer == null || !pointer.startsWith("/") || pointer.length() == 1) {
            throw new IllegalArgumentException("JSON Pointer must start with '/' and identify a field: " + pointer);
        }
        final String[] raw = pointer.substring(1).split("/", -1);
        final List<String> compiled = new ArrayList<>(raw.length);
        for (String segment : raw) {
            final String decoded = segment.replace("~1", "/").replace("~0", "~");
            if (decoded.isEmpty()) {
                throw new IllegalArgumentException("Empty JSON Pointer segments are not supported");
            }
            compiled.add(decoded);
        }
        return List.copyOf(compiled);
    }

    private static final class Node {
        private final Map<String, Node> children = new HashMap<>();
        private int targetIndex = -1;
    }
}
