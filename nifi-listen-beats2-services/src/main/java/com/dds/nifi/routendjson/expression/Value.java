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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public final class Value {
    public enum Kind { MISSING, NULL, BOOLEAN, NUMBER, STRING, ARRAY, OBJECT }

    public static final Value MISSING = new Value(Kind.MISSING, null);
    public static final Value NULL = new Value(Kind.NULL, null);
    public static final Value TRUE = new Value(Kind.BOOLEAN, Boolean.TRUE);
    public static final Value FALSE = new Value(Kind.BOOLEAN, Boolean.FALSE);

    private final Kind kind;
    private final Object value;

    private Value(final Kind kind, final Object value) {
        this.kind = kind;
        this.value = value;
    }

    public static Value ofBoolean(final boolean value) {
        return value ? TRUE : FALSE;
    }

    public static Value ofNumber(final BigDecimal value) {
        return value == null ? NULL : new Value(Kind.NUMBER, value.stripTrailingZeros());
    }

    public static Value ofString(final String value) {
        return value == null ? NULL : new Value(Kind.STRING, value);
    }

    public static Value ofArray(final List<Value> values) {
        return new Value(Kind.ARRAY, values == null ? List.of() : List.copyOf(values));
    }

    public static Value ofObject(final JsonNode object) {
        return new Value(Kind.OBJECT, object);
    }

    public static Value fromJsonNode(final JsonNode node) {
        if (node == null || node.isMissingNode()) {
            return MISSING;
        }
        if (node.isNull()) {
            return NULL;
        }
        if (node.isBoolean()) {
            return ofBoolean(node.asBoolean());
        }
        if (node.isNumber()) {
            return ofNumber(node.decimalValue());
        }
        if (node.isTextual()) {
            return ofString(node.asText());
        }
        if (node.isArray()) {
            final List<Value> values = new ArrayList<>();
            node.forEach(child -> values.add(fromJsonNode(child)));
            return ofArray(values);
        }
        return ofObject(node);
    }

    public Kind kind() {
        return kind;
    }

    public boolean isMissing() {
        return kind == Kind.MISSING;
    }

    public boolean isNull() {
        return kind == Kind.NULL;
    }

    public boolean isBoolean() {
        return kind == Kind.BOOLEAN;
    }

    public boolean isNumber() {
        return kind == Kind.NUMBER;
    }

    public boolean isString() {
        return kind == Kind.STRING;
    }

    public boolean isArray() {
        return kind == Kind.ARRAY;
    }

    public boolean asBoolean() {
        return Boolean.TRUE.equals(value);
    }

    public BigDecimal asNumberOrNull() {
        if (kind == Kind.NUMBER) {
            return (BigDecimal) value;
        }
        if (kind == Kind.STRING) {
            try {
                return new BigDecimal((String) value).stripTrailingZeros();
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public List<Value> asArrayOrEmpty() {
        if (kind == Kind.ARRAY) {
            return (List<Value>) value;
        }
        return List.of();
    }

    public String asStringOrNull() {
        switch (kind) {
            case STRING: return (String) value;
            case NUMBER: return ((BigDecimal) value).toPlainString();
            case BOOLEAN: return Boolean.toString((Boolean) value);
            default: return null;
        }
    }

    public Value lower() {
        final String s = asStringOrNull();
        return s == null ? NULL : ofString(s.toLowerCase(Locale.ROOT));
    }

    public Value upper() {
        final String s = asStringOrNull();
        return s == null ? NULL : ofString(s.toUpperCase(Locale.ROOT));
    }

    public boolean valueEquals(final Value other) {
        if (other == null) {
            return false;
        }
        if (this.isMissing() || other.isMissing()) {
            return false;
        }
        if (this.isNull() || other.isNull()) {
            return this.isNull() && other.isNull();
        }

        final BigDecimal leftNumber = this.asNumberOrNull();
        final BigDecimal rightNumber = other.asNumberOrNull();
        if (leftNumber != null && rightNumber != null && (this.isNumber() || other.isNumber())) {
            return leftNumber.compareTo(rightNumber) == 0;
        }

        if (this.kind == Kind.BOOLEAN || other.kind == Kind.BOOLEAN) {
            return this.kind == other.kind && Objects.equals(this.value, other.value);
        }

        final String leftString = this.asStringOrNull();
        final String rightString = other.asStringOrNull();
        return leftString != null && rightString != null && leftString.equals(rightString);
    }

    public Integer compareToValue(final Value other) {
        if (other == null || this.isMissing() || other.isMissing() || this.isNull() || other.isNull()) {
            return null;
        }

        final BigDecimal leftNumber = this.asNumberOrNull();
        final BigDecimal rightNumber = other.asNumberOrNull();
        if (leftNumber != null && rightNumber != null) {
            return leftNumber.compareTo(rightNumber);
        }

        final String leftString = this.asStringOrNull();
        final String rightString = other.asStringOrNull();
        if (leftString != null && rightString != null) {
            return leftString.compareTo(rightString);
        }

        return null;
    }


    public String indexKeyOrNull() {
        switch (kind) {
            case NULL: return "Z:null";
            case BOOLEAN: return "B:" + value;
            case NUMBER: return "N:" + ((BigDecimal) value).stripTrailingZeros().toPlainString();
            case STRING: return "S:" + value;
            default: return null;
        }
    }

    public boolean arrayContains(final Value needle) {
        if (!isArray()) {
            return false;
        }
        for (Value item : asArrayOrEmpty()) {
            if (item.valueEquals(needle)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return kind + (value == null ? "" : ":" + value);
    }
}
