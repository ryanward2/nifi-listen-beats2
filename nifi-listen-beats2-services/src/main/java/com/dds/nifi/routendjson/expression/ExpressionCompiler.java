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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class ExpressionCompiler {
    private ExpressionCompiler() { }

    public static CompiledExpression compile(final String source) {
        final Parser parser = new Parser(source);
        return new CompiledExpression(source, parser.parse());
    }

    private static final class Parser {
        private final String source;
        private final List<Tokenizer.Token> tokens;
        private int current;

        Parser(final String source) {
            this.source = source == null ? "" : source;
            this.tokens = new Tokenizer(this.source).tokenize();
        }

        Expression parse() {
            final Expression expression = parseOr();
            expect(Tokenizer.Type.EOF, "Expected end of expression");
            return expression;
        }

        private Expression parseOr() {
            Expression expr = parseAnd();
            while (match(Tokenizer.Type.OR)) {
                final Expression left = expr;
                final Expression right = parseAnd();
                expr = new OrExpression(left, right);
            }
            return expr;
        }

        private Expression parseAnd() {
            final List<Expression> terms = new ArrayList<>();
            terms.add(parseComparison());
            while (match(Tokenizer.Type.AND)) {
                terms.add(parseComparison());
            }
            if (terms.size() == 1) {
                return terms.get(0);
            }

            // Cost-based ordering inside pure AND expressions. This preserves boolean semantics for this DSL
            // and makes cheap equality/exists checks run before regex/contains work.
            terms.sort(Comparator.comparingInt(Expression::cost));
            return new AndExpression(terms);
        }

        private Expression parseComparison() {
            Expression expr = parseUnary();
            if (match(Tokenizer.Type.EQ, Tokenizer.Type.NE, Tokenizer.Type.LT, Tokenizer.Type.LE, Tokenizer.Type.GT, Tokenizer.Type.GE)) {
                final Tokenizer.Token operator = previous();
                final Expression left = expr;
                final Expression right = parseUnary();
                expr = new CompareExpression(operator.type(), left, right);
            }
            return expr;
        }

        private Expression parseUnary() {
            if (match(Tokenizer.Type.NOT)) {
                return new NotExpression(parseUnary());
            }
            return parsePrimary();
        }

        private Expression parsePrimary() {
            if (match(Tokenizer.Type.NUMBER)) {
                return new LiteralExpression(Value.ofNumber(new BigDecimal(previous().text())));
            }
            if (match(Tokenizer.Type.STRING)) {
                return new LiteralExpression(Value.ofString(previous().text()));
            }
            if (match(Tokenizer.Type.TRUE)) {
                return new LiteralExpression(Value.ofBoolean(true));
            }
            if (match(Tokenizer.Type.FALSE)) {
                return new LiteralExpression(Value.ofBoolean(false));
            }
            if (match(Tokenizer.Type.NULL)) {
                return new LiteralExpression(Value.NULL);
            }
            if (match(Tokenizer.Type.LPAREN)) {
                final Expression expression = parseOr();
                expect(Tokenizer.Type.RPAREN, "Expected )");
                return expression;
            }
            if (match(Tokenizer.Type.LBRACKET)) {
                final List<Expression> values = new ArrayList<>();
                if (!check(Tokenizer.Type.RBRACKET)) {
                    do {
                        values.add(parseOr());
                    } while (match(Tokenizer.Type.COMMA));
                }
                expect(Tokenizer.Type.RBRACKET, "Expected ]");
                return new ArrayExpression(values);
            }
            if (match(Tokenizer.Type.IDENT)) {
                final String identifier = previous().text();
                if (match(Tokenizer.Type.LPAREN)) {
                    return parseFunctionCall(identifier);
                }
                return new PathExpression(identifier);
            }

            throw error("Expected expression", peek());
        }

        private Expression parseFunctionCall(final String functionName) {
            final List<Expression> args = new ArrayList<>();
            if (!check(Tokenizer.Type.RPAREN)) {
                do {
                    args.add(parseOr());
                } while (match(Tokenizer.Type.COMMA));
            }
            expect(Tokenizer.Type.RPAREN, "Expected ) after function arguments");

            final String normalized = functionName.toLowerCase(Locale.ROOT);
            switch (normalized) {
                case "contains": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 35));
                case "containsany": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 35));
                case "startswith": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 30));
                case "startswithany": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 30));
                case "endswith": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 30));
                case "endswithany": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 30));
                case "containsignorecase": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 35));
                case "containsanyignorecase": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 35));
                case "startswithignorecase": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 30));
                case "startswithanyignorecase": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 30));
                case "endswithignorecase": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 30));
                case "endswithanyignorecase": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 30));
                case "equalsignorecase": return requireArity(functionName, args, 2, new FunctionExpression(normalized, args, 20));
                case "matchesregex": return requireArity(functionName, args, 2, regexExpression(args));
                case "exists": return requireArity(functionName, args, 1, new FunctionExpression(normalized, args, 2));
                case "isnull": return requireArity(functionName, args, 1, new FunctionExpression(normalized, args, 2));
                case "in": return requireArity(functionName, args, 2, inExpression(args));
                case "lower": return requireArity(functionName, args, 1, new FunctionExpression(normalized, args, 20));
                case "upper": return requireArity(functionName, args, 1, new FunctionExpression(normalized, args, 20));
                case "cidrcontains": return requireArity(functionName, args, 2, cidrExpression(args));
                case "cidrcontainsany": return requireArity(functionName, args, 2, cidrAnyExpression(args));
                case "isip": return requireArity(functionName, args, 1, new FunctionExpression(normalized, args, 10));
                case "isipv4": return requireArity(functionName, args, 1, new FunctionExpression(normalized, args, 10));
                case "isipv6": return requireArity(functionName, args, 1, new FunctionExpression(normalized, args, 10));
                default: throw new ExpressionCompileException("Unsupported function [" + functionName + "]");
            }
        }

        private Expression regexExpression(final List<Expression> args) {
            if (args.get(1) instanceof LiteralExpression && ((LiteralExpression) args.get(1)).value().isString()) {
                final LiteralExpression literal = (LiteralExpression) args.get(1);
                final String patternText = literal.value().asStringOrNull();
                final Pattern pattern;
                try {
                    pattern = Pattern.compile(patternText);
                } catch (PatternSyntaxException e) {
                    throw new ExpressionCompileException("Invalid regex pattern [" + patternText + "]: " + e.getMessage(), e);
                }
                return new RegexExpression(args.get(0), args.get(1), pattern);
            }
            return new FunctionExpression("matchesregex", args, 100);
        }

        private Expression cidrExpression(final List<Expression> args) {
            if (args.get(1) instanceof LiteralExpression && ((LiteralExpression) args.get(1)).value().isString()) {
                final LiteralExpression literal = (LiteralExpression) args.get(1);
                final String cidrText = literal.value().asStringOrNull();
                try {
                    return new CidrContainsExpression(args.get(0), IpCidr.parseCidrOrThrow(cidrText));
                } catch (IllegalArgumentException e) {
                    throw new ExpressionCompileException("Invalid CIDR [" + cidrText + "]: " + e.getMessage(), e);
                }
            }
            return new FunctionExpression("cidrcontains", args, 45);
        }

        private Expression cidrAnyExpression(final List<Expression> args) {
            if (args.get(1) instanceof ArrayExpression && ((ArrayExpression) args.get(1)).isLiteralArray()) {
                final ArrayExpression array = (ArrayExpression) args.get(1);
                final List<IpCidr.CidrRange> ranges = new ArrayList<>();
                for (Value value : array.literalValues()) {
                    if (!value.isString()) {
                        throw new ExpressionCompileException("cidrContainsAny expects a literal array of CIDR strings when the second argument is an array literal");
                    }
                    final String cidrText = value.asStringOrNull();
                    try {
                        ranges.add(IpCidr.parseCidrOrThrow(cidrText));
                    } catch (IllegalArgumentException e) {
                        throw new ExpressionCompileException("Invalid CIDR [" + cidrText + "]: " + e.getMessage(), e);
                    }
                }
                return new CidrContainsAnyExpression(args.get(0), ranges);
            }
            return new FunctionExpression("cidrcontainsany", args, 45);
        }

        private Expression inExpression(final List<Expression> args) {
            if (args.get(1) instanceof ArrayExpression && ((ArrayExpression) args.get(1)).isLiteralArray()) {
                final ArrayExpression array = (ArrayExpression) args.get(1);
                return new InLiteralArrayExpression(args.get(0), array.literalValues());
            }
            return new FunctionExpression("in", args, 4);
        }

        private Expression requireArity(final String name, final List<Expression> args, final int expected, final Expression expression) {
            if (args.size() != expected) {
                throw new ExpressionCompileException("Function [" + name + "] expects " + expected + " arguments but got " + args.size());
            }
            return expression;
        }

        private boolean match(final Tokenizer.Type... types) {
            for (Tokenizer.Type type : types) {
                if (check(type)) {
                    current++;
                    return true;
                }
            }
            return false;
        }

        private void expect(final Tokenizer.Type type, final String message) {
            if (check(type)) {
                current++;
                return;
            }
            throw error(message, peek());
        }

        private boolean check(final Tokenizer.Type type) {
            return peek().type() == type;
        }

        private Tokenizer.Token peek() {
            return tokens.get(current);
        }

        private Tokenizer.Token previous() {
            return tokens.get(current - 1);
        }

        private ExpressionCompileException error(final String message, final Tokenizer.Token token) {
            return new ExpressionCompileException(message + " at character " + token.position());
        }
    }

    private static final class LiteralExpression implements Expression {
        private final Value value;

        private LiteralExpression(final Value value) {
            this.value = value;
        }

        Value value() {
            return value;
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            return value;
        }

        @Override
        public int cost() {
            return 0;
        }
    }

    private static final class ArrayExpression implements Expression {
        private final List<Expression> values;

        private ArrayExpression(final List<Expression> values) {
            this.values = List.copyOf(values);
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            final List<Value> evaluated = new ArrayList<>(values.size());
            for (Expression valueExpression : values) {
                evaluated.add(valueExpression.evaluate(context));
            }
            return Value.ofArray(evaluated);
        }

        boolean isLiteralArray() {
            for (Expression expression : values) {
                if (!(expression instanceof LiteralExpression)) {
                    return false;
                }
            }
            return true;
        }

        List<Value> literalValues() {
            final List<Value> result = new ArrayList<>(values.size());
            for (Expression expression : values) {
                result.add(((LiteralExpression) expression).value());
            }
            return result;
        }

        @Override
        public int cost() {
            return 1;
        }
    }

    private static final class PathExpression implements Expression {
        private final String source;
        private final String[] segments;

        private PathExpression(final String source) {
            this.source = source;
            this.segments = source.split("\\.");
            if (segments.length == 0) {
                throw new ExpressionCompileException("Empty path expression");
            }
            for (String segment : segments) {
                if (segment.isEmpty()) {
                    throw new ExpressionCompileException("Invalid dotted path [" + source + "]");
                }
            }
        }

        String source() {
            return source;
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            return context.path(source, segments);
        }

        @Override
        public int cost() {
            return 1;
        }

        @Override
        public String toString() {
            return source;
        }
    }

    private static final class NotExpression implements Expression {
        private final Expression inner;

        private NotExpression(final Expression inner) {
            this.inner = inner;
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            final Value value = inner.evaluate(context);
            return Value.ofBoolean(!(value.isBoolean() && value.asBoolean()));
        }

        @Override
        public int cost() {
            return inner.cost() + 1;
        }
    }

    private static final class AndExpression implements Expression {
        private final List<Expression> terms;

        private AndExpression(final List<Expression> terms) {
            this.terms = List.copyOf(terms);
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            for (Expression term : terms) {
                final Value value = term.evaluate(context);
                if (!value.isBoolean() || !value.asBoolean()) {
                    return Value.ofBoolean(false);
                }
            }
            return Value.ofBoolean(true);
        }

        @Override
        public List<IndexAnchor> indexAnchors() {
            final List<IndexAnchor> anchors = new ArrayList<>();
            for (Expression term : terms) {
                anchors.addAll(term.indexAnchors());
            }
            return anchors;
        }

        @Override
        public int cost() {
            int sum = 0;
            for (Expression term : terms) {
                sum += term.cost();
            }
            return sum;
        }
    }

    private static final class OrExpression implements Expression {
        private final Expression left;
        private final Expression right;

        private OrExpression(final Expression left, final Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            final Value leftValue = left.evaluate(context);
            if (leftValue.isBoolean() && leftValue.asBoolean()) {
                return Value.ofBoolean(true);
            }
            final Value rightValue = right.evaluate(context);
            return Value.ofBoolean(rightValue.isBoolean() && rightValue.asBoolean());
        }

        @Override
        public int cost() {
            return left.cost() + right.cost() + 2;
        }
    }

    private static final class CompareExpression implements Expression {
        private final Tokenizer.Type operator;
        private final Expression left;
        private final Expression right;

        private CompareExpression(final Tokenizer.Type operator, final Expression left, final Expression right) {
            this.operator = operator;
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            final Value l = left.evaluate(context);
            final Value r = right.evaluate(context);
            switch (operator) {
                case EQ:
                    return Value.ofBoolean(l.valueEquals(r));
                case NE:
                    return Value.ofBoolean(!l.valueEquals(r));
                case LT:
                case LE:
                case GT:
                case GE:
                    final Integer cmp = l.compareToValue(r);
                    if (cmp == null) {
                        return Value.FALSE;
                    }
                    switch (operator) {
                        case LT: return Value.ofBoolean(cmp < 0);
                        case LE: return Value.ofBoolean(cmp <= 0);
                        case GT: return Value.ofBoolean(cmp > 0);
                        case GE: return Value.ofBoolean(cmp >= 0);
                        default: return Value.FALSE;
                    }
                default:
                    throw new IllegalStateException("Unexpected comparison operator " + operator);
            }
        }

        @Override
        public List<IndexAnchor> indexAnchors() {
            if (operator != Tokenizer.Type.EQ) {
                return List.of();
            }
            if (left instanceof PathExpression && right instanceof LiteralExpression) {
                final PathExpression path = (PathExpression) left;
                final LiteralExpression literal = (LiteralExpression) right;
                return List.of(new IndexAnchor(path.source(), List.of(literal.value())));
            }
            if (right instanceof PathExpression && left instanceof LiteralExpression) {
                final PathExpression path = (PathExpression) right;
                final LiteralExpression literal = (LiteralExpression) left;
                return List.of(new IndexAnchor(path.source(), List.of(literal.value())));
            }
            return List.of();
        }

        @Override
        public int cost() {
            return operator == Tokenizer.Type.EQ || operator == Tokenizer.Type.NE ? 3 : 6;
        }
    }

    private static final class InLiteralArrayExpression implements Expression {
        private final Expression valueExpression;
        private final List<Value> literalValues;

        private InLiteralArrayExpression(final Expression valueExpression, final List<Value> literalValues) {
            this.valueExpression = valueExpression;
            this.literalValues = List.copyOf(literalValues);
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            final Value value = valueExpression.evaluate(context);
            if (value.isArray()) {
                for (Value item : value.asArrayOrEmpty()) {
                    if (containsLiteral(item)) {
                        return Value.TRUE;
                    }
                }
                return Value.FALSE;
            }
            return Value.ofBoolean(containsLiteral(value));
        }

        private boolean containsLiteral(final Value value) {
            for (Value literal : literalValues) {
                if (literal.valueEquals(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public List<IndexAnchor> indexAnchors() {
            if (valueExpression instanceof PathExpression) {
                final PathExpression path = (PathExpression) valueExpression;
                return List.of(new IndexAnchor(path.source(), literalValues));
            }
            return List.of();
        }

        @Override
        public int cost() {
            return 3;
        }
    }

    private static final class FunctionExpression implements Expression {
        private final String name;
        private final List<Expression> args;
        private final int cost;

        private FunctionExpression(final String name, final List<Expression> args, final int cost) {
            this.name = name;
            this.args = List.copyOf(args);
            this.cost = cost;
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            switch (name) {
                case "contains": return contains(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "containsany": return containsAny(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "startswith": return startsWith(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "startswithany": return startsWithAny(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "endswith": return endsWith(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "endswithany": return endsWithAny(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "containsignorecase": return containsIgnoreCase(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "containsanyignorecase": return containsAnyIgnoreCase(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "startswithignorecase": return startsWithIgnoreCase(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "startswithanyignorecase": return startsWithAnyIgnoreCase(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "endswithignorecase": return endsWithIgnoreCase(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "endswithanyignorecase": return endsWithAnyIgnoreCase(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "equalsignorecase": return equalsIgnoreCase(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "matchesregex": return matchesRegex(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "exists": return Value.ofBoolean(!args.get(0).evaluate(context).isMissing());
                case "isnull": return Value.ofBoolean(args.get(0).evaluate(context).isNull());
                case "in": return in(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "lower": return args.get(0).evaluate(context).lower();
                case "upper": return args.get(0).evaluate(context).upper();
                case "cidrcontains": return cidrContains(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "cidrcontainsany": return cidrContainsAny(args.get(0).evaluate(context), args.get(1).evaluate(context));
                case "isip": return isIp(args.get(0).evaluate(context));
                case "isipv4": return isIpv4(args.get(0).evaluate(context));
                case "isipv6": return isIpv6(args.get(0).evaluate(context));
                default: throw new IllegalStateException("Unsupported function [" + name + "]");
            }
        }

        @Override
        public List<IndexAnchor> indexAnchors() {
            if ("in".equals(name) && args.size() == 2
                    && args.get(0) instanceof PathExpression
                    && args.get(1) instanceof ArrayExpression
                    && ((ArrayExpression) args.get(1)).isLiteralArray()) {
                final PathExpression path = (PathExpression) args.get(0);
                final ArrayExpression array = (ArrayExpression) args.get(1);
                return List.of(new IndexAnchor(path.source(), array.literalValues()));
            }
            return List.of();
        }

        @Override
        public int cost() {
            return cost;
        }
    }

    private static final class RegexExpression implements Expression {
        private final Expression valueExpression;
        private final Expression patternExpression;
        private final Pattern pattern;

        private RegexExpression(final Expression valueExpression, final Expression patternExpression, final Pattern pattern) {
            this.valueExpression = valueExpression;
            this.patternExpression = patternExpression;
            this.pattern = pattern;
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            return Value.ofBoolean(matchesPattern(valueExpression.evaluate(context), pattern));
        }

        @Override
        public int cost() {
            return 100;
        }
    }

    private static final class CidrContainsExpression implements Expression {
        private final Expression ipExpression;
        private final IpCidr.CidrRange cidrRange;

        private CidrContainsExpression(final Expression ipExpression, final IpCidr.CidrRange cidrRange) {
            this.ipExpression = ipExpression;
            this.cidrRange = cidrRange;
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            return Value.ofBoolean(ipValueInAnyRange(ipExpression.evaluate(context), List.of(cidrRange)));
        }

        @Override
        public int cost() {
            return 35;
        }
    }

    private static final class CidrContainsAnyExpression implements Expression {
        private final Expression ipExpression;
        private final List<IpCidr.CidrRange> cidrRanges;

        private CidrContainsAnyExpression(final Expression ipExpression, final List<IpCidr.CidrRange> cidrRanges) {
            this.ipExpression = ipExpression;
            this.cidrRanges = List.copyOf(cidrRanges);
        }

        @Override
        public Value evaluate(final EvaluationContext context) {
            return Value.ofBoolean(ipValueInAnyRange(ipExpression.evaluate(context), cidrRanges));
        }

        @Override
        public int cost() {
            return 35;
        }
    }

    private static Value containsIgnoreCase(final Value haystack, final Value needle) {
        if (haystack.isArray()) {
            for (Value item : haystack.asArrayOrEmpty()) {
                if (containsIgnoreCase(item, needle).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        final String s = haystack.asStringOrNull();
        final String n = needle.asStringOrNull();
        return Value.ofBoolean(s != null && n != null && containsIgnoreCaseScalar(s, n));
    }

    private static Value containsAny(final Value haystack, final Value needles) {
        return anyStringPredicate(haystack, needles, false, StringMatch.CONTAINS);
    }

    private static Value containsAnyIgnoreCase(final Value haystack, final Value needles) {
        return anyStringPredicate(haystack, needles, true, StringMatch.CONTAINS);
    }

    private static Value startsWithAny(final Value value, final Value prefixes) {
        return anyStringPredicate(value, prefixes, false, StringMatch.STARTS_WITH);
    }

    private static Value startsWithAnyIgnoreCase(final Value value, final Value prefixes) {
        return anyStringPredicate(value, prefixes, true, StringMatch.STARTS_WITH);
    }

    private static Value endsWithAny(final Value value, final Value suffixes) {
        return anyStringPredicate(value, suffixes, false, StringMatch.ENDS_WITH);
    }

    private static Value endsWithAnyIgnoreCase(final Value value, final Value suffixes) {
        return anyStringPredicate(value, suffixes, true, StringMatch.ENDS_WITH);
    }

    private enum StringMatch { CONTAINS, STARTS_WITH, ENDS_WITH }

    private static Value anyStringPredicate(final Value value, final Value needles, final boolean ignoreCase, final StringMatch match) {
        if (!needles.isArray()) {
            return Value.ofBoolean(false);
        }
        return Value.ofBoolean(anyStringPredicate(value, needles.asArrayOrEmpty(), ignoreCase, match));
    }

    private static boolean anyStringPredicate(final Value value, final List<Value> needles, final boolean ignoreCase, final StringMatch match) {
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (anyStringPredicate(item, needles, ignoreCase, match)) {
                    return true;
                }
            }
            return false;
        }
        final String s = value.asStringOrNull();
        if (s == null) {
            return false;
        }
        for (Value needleValue : needles) {
            final String needle = needleValue.asStringOrNull();
            if (needle == null) {
                continue;
            }
            if (stringMatches(s, needle, ignoreCase, match)) {
                return true;
            }
        }
        return false;
    }

    private static boolean stringMatches(final String value, final String needle, final boolean ignoreCase, final StringMatch match) {
        switch (match) {
            case CONTAINS:
                return ignoreCase ? containsIgnoreCaseScalar(value, needle) : value.contains(needle);
            case STARTS_WITH:
                return ignoreCase ? value.regionMatches(true, 0, needle, 0, needle.length()) : value.startsWith(needle);
            case ENDS_WITH:
                if (needle.length() > value.length()) {
                    return false;
                }
                return ignoreCase
                        ? value.regionMatches(true, value.length() - needle.length(), needle, 0, needle.length())
                        : value.endsWith(needle);
            default:
                return false;
        }
    }

    private static boolean containsIgnoreCaseScalar(final String value, final String needle) {
        if (needle.isEmpty()) {
            return true;
        }
        final int max = value.length() - needle.length();
        for (int i = 0; i <= max; i++) {
            if (value.regionMatches(true, i, needle, 0, needle.length())) {
                return true;
            }
        }
        return false;
    }

    private static Value startsWithIgnoreCase(final Value value, final Value prefix) {
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (startsWithIgnoreCase(item, prefix).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        final String s = value.asStringOrNull();
        final String p = prefix.asStringOrNull();
        return Value.ofBoolean(s != null && p != null && s.regionMatches(true, 0, p, 0, p.length()));
    }

    private static Value endsWithIgnoreCase(final Value value, final Value suffix) {
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (endsWithIgnoreCase(item, suffix).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        final String s = value.asStringOrNull();
        final String suf = suffix.asStringOrNull();
        if (s == null || suf == null || suf.length() > s.length()) {
            return Value.ofBoolean(false);
        }
        return Value.ofBoolean(s.regionMatches(true, s.length() - suf.length(), suf, 0, suf.length()));
    }

    private static Value equalsIgnoreCase(final Value left, final Value right) {
        if (left.isArray()) {
            for (Value item : left.asArrayOrEmpty()) {
                if (equalsIgnoreCase(item, right).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        if (right.isArray()) {
            for (Value item : right.asArrayOrEmpty()) {
                if (equalsIgnoreCase(left, item).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        final String l = left.asStringOrNull();
        final String r = right.asStringOrNull();
        return Value.ofBoolean(l != null && r != null && l.equalsIgnoreCase(r));
    }

    private static Value cidrContains(final Value ipValue, final Value cidrValue) {
        final String cidrText = cidrValue.asStringOrNull();
        if (cidrText == null) {
            return Value.ofBoolean(false);
        }
        final IpCidr.CidrRange range;
        try {
            range = IpCidr.parseCidr(cidrText);
        } catch (IllegalArgumentException e) {
            return Value.ofBoolean(false);
        }
        return Value.ofBoolean(ipValueInAnyRange(ipValue, List.of(range)));
    }

    private static Value cidrContainsAny(final Value ipValue, final Value cidrsValue) {
        if (!cidrsValue.isArray()) {
            return Value.ofBoolean(false);
        }
        final List<IpCidr.CidrRange> ranges = new ArrayList<>();
        for (Value item : cidrsValue.asArrayOrEmpty()) {
            final String cidrText = item.asStringOrNull();
            if (cidrText == null) {
                continue;
            }
            try {
                ranges.add(IpCidr.parseCidr(cidrText));
            } catch (IllegalArgumentException ignored) {
                return Value.ofBoolean(false);
            }
        }
        return Value.ofBoolean(ipValueInAnyRange(ipValue, ranges));
    }

    private static boolean ipValueInAnyRange(final Value ipValue, final List<IpCidr.CidrRange> ranges) {
        if (ipValue.isArray()) {
            for (Value item : ipValue.asArrayOrEmpty()) {
                if (ipValueInAnyRange(item, ranges)) {
                    return true;
                }
            }
            return false;
        }
        final String ipText = ipValue.asStringOrNull();
        if (ipText == null) {
            return false;
        }
        for (IpCidr.CidrRange range : ranges) {
            if (range.contains(ipText)) {
                return true;
            }
        }
        return false;
    }

    private static Value isIp(final Value value) {
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (isIp(item).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        final String s = value.asStringOrNull();
        return Value.ofBoolean(s != null && IpCidr.isIp(s));
    }

    private static Value isIpv4(final Value value) {
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (isIpv4(item).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        final String s = value.asStringOrNull();
        return Value.ofBoolean(s != null && IpCidr.isIpv4(s));
    }

    private static Value isIpv6(final Value value) {
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (isIpv6(item).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        final String s = value.asStringOrNull();
        return Value.ofBoolean(s != null && IpCidr.isIpv6(s));
    }

    private static Value contains(final Value haystack, final Value needle) {
        if (haystack.isMissing() || haystack.isNull() || needle.isMissing() || needle.isNull()) {
            return Value.ofBoolean(false);
        }
        if (haystack.isArray()) {
            return Value.ofBoolean(haystack.arrayContains(needle));
        }
        final String s = haystack.asStringOrNull();
        final String n = needle.asStringOrNull();
        return Value.ofBoolean(s != null && n != null && s.contains(n));
    }

    private static Value startsWith(final Value value, final Value prefix) {
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (startsWith(item, prefix).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        final String s = value.asStringOrNull();
        final String p = prefix.asStringOrNull();
        return Value.ofBoolean(s != null && p != null && s.startsWith(p));
    }

    private static Value endsWith(final Value value, final Value suffix) {
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (endsWith(item, suffix).asBoolean()) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        final String s = value.asStringOrNull();
        final String suf = suffix.asStringOrNull();
        return Value.ofBoolean(s != null && suf != null && s.endsWith(suf));
    }

    private static Value matchesRegex(final Value value, final Value patternValue) {
        final String p = patternValue.asStringOrNull();
        if (p == null) {
            return Value.ofBoolean(false);
        }
        try {
            return Value.ofBoolean(matchesPattern(value, Pattern.compile(p)));
        } catch (PatternSyntaxException ignored) {
            return Value.ofBoolean(false);
        }
    }

    private static boolean matchesPattern(final Value value, final Pattern pattern) {
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (matchesPattern(item, pattern)) {
                    return true;
                }
            }
            return false;
        }
        final String s = value.asStringOrNull();
        return s != null && pattern.matcher(s).find();
    }

    private static Value in(final Value value, final Value array) {
        if (!array.isArray()) {
            return Value.ofBoolean(false);
        }
        if (value.isArray()) {
            for (Value item : value.asArrayOrEmpty()) {
                if (array.arrayContains(item)) {
                    return Value.ofBoolean(true);
                }
            }
            return Value.ofBoolean(false);
        }
        return Value.ofBoolean(array.arrayContains(value));
    }
}
