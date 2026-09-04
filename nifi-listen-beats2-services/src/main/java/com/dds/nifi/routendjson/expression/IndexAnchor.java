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

import java.util.List;
import java.util.Objects;

public final class IndexAnchor {
    private final String path;
    private final List<Value> values;

    public IndexAnchor(final String path, final List<Value> values) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("Index anchor path is required");
        }
        this.path = path;
        this.values = values == null ? List.of() : List.copyOf(values);
    }

    public String path() { return path; }
    public List<Value> values() { return values; }

    @Override
    public boolean equals(final Object other) {
        if (this == other) return true;
        if (!(other instanceof IndexAnchor)) return false;
        final IndexAnchor that = (IndexAnchor) other;
        return path.equals(that.path) && values.equals(that.values);
    }

    @Override
    public int hashCode() { return Objects.hash(path, values); }

    @Override
    public String toString() { return "IndexAnchor{" + path + '=' + values + '}'; }
}
