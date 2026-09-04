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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class CompiledExpression {
    private final String source;
    private final Expression expression;
    private final List<IndexAnchor> indexAnchors;
    private final Set<String> referencedPaths;

    CompiledExpression(final String source, final Expression expression) {
        this.source = source;
        this.expression = expression;
        this.indexAnchors = List.copyOf(expression.indexAnchors());
        this.referencedPaths = discoverReferencedPaths(source);
    }

    public String source() {
        return source;
    }

    public List<IndexAnchor> indexAnchors() {
        return indexAnchors;
    }

    public Set<String> referencedPaths() {
        return referencedPaths;
    }

    public boolean evaluateBoolean(final JsonNode root) {
        return evaluateBoolean(new EvaluationContext(root));
    }

    public boolean evaluateBoolean(final EvaluationContext context) {
        final Value value = expression.evaluate(context);
        return value.isBoolean() && value.asBoolean();
    }

    /**
     * The tokenizer already distinguishes identifiers from string literals. An
     * identifier followed by '(' is a function name; every other identifier is
     * a dotted JSON path. Keeping this discovery beside the compiler preserves
     * exact JEL syntax without exposing the private AST implementation.
     */
    private static Set<String> discoverReferencedPaths(final String source) {
        final List<Tokenizer.Token> tokens = new Tokenizer(source).tokenize();
        final LinkedHashSet<String> paths = new LinkedHashSet<>();
        for (int index = 0; index < tokens.size(); index++) {
            final Tokenizer.Token token = tokens.get(index);
            if (token.type() != Tokenizer.Type.IDENT) {
                continue;
            }
            final boolean functionName = index + 1 < tokens.size()
                    && tokens.get(index + 1).type() == Tokenizer.Type.LPAREN;
            if (!functionName) {
                paths.add(token.text());
            }
        }
        return Collections.unmodifiableSet(paths);
    }
}
