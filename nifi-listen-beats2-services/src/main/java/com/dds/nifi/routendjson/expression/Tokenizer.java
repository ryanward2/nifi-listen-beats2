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
import java.util.List;

final class Tokenizer {
    enum Type {
        IDENT, STRING, NUMBER, TRUE, FALSE, NULL,
        EQ, NE, LT, LE, GT, GE, AND, OR, NOT,
        LPAREN, RPAREN, LBRACKET, RBRACKET, COMMA,
        EOF
    }

    static final class Token {
        private final Type type;
        private final String text;
        private final int position;

        Token(final Type type, final String text, final int position) {
            this.type = type;
            this.text = text;
            this.position = position;
        }

        Type type() { return type; }
        String text() { return text; }
        int position() { return position; }
    }

    private final String input;
    private final int length;
    private int position;

    Tokenizer(final String input) {
        this.input = input == null ? "" : input;
        this.length = this.input.length();
    }

    List<Token> tokenize() {
        final List<Token> tokens = new ArrayList<>();
        Token token;
        do {
            token = nextToken();
            tokens.add(token);
        } while (token.type() != Type.EOF);
        return tokens;
    }

    private Token nextToken() {
        skipWhitespace();
        if (position >= length) {
            return new Token(Type.EOF, "", position);
        }

        final char c = input.charAt(position);
        final int start = position;

        if (isIdentifierStart(c)) {
            position++;
            while (position < length && isIdentifierPart(input.charAt(position))) {
                position++;
            }
            final String text = input.substring(start, position);
            if ("true".equals(text)) return new Token(Type.TRUE, text, start);
            if ("false".equals(text)) return new Token(Type.FALSE, text, start);
            if ("null".equals(text)) return new Token(Type.NULL, text, start);
            return new Token(Type.IDENT, text, start);
        }

        if (Character.isDigit(c) || (c == '-' && position + 1 < length && Character.isDigit(input.charAt(position + 1)))) {
            position++;
            while (position < length && Character.isDigit(input.charAt(position))) position++;
            if (position < length && input.charAt(position) == '.') {
                position++;
                while (position < length && Character.isDigit(input.charAt(position))) position++;
            }
            if (position < length && (input.charAt(position) == 'e' || input.charAt(position) == 'E')) {
                position++;
                if (position < length && (input.charAt(position) == '+' || input.charAt(position) == '-')) position++;
                while (position < length && Character.isDigit(input.charAt(position))) position++;
            }
            final String text = input.substring(start, position);
            try {
                new BigDecimal(text);
            } catch (NumberFormatException e) {
                throw error("Invalid numeric literal [" + text + "]", start);
            }
            return new Token(Type.NUMBER, text, start);
        }

        if (c == '\'' || c == '"') return stringToken(c);

        position++;
        switch (c) {
            case '(': return new Token(Type.LPAREN, "(", start);
            case ')': return new Token(Type.RPAREN, ")", start);
            case '[': return new Token(Type.LBRACKET, "[", start);
            case ']': return new Token(Type.RBRACKET, "]", start);
            case ',': return new Token(Type.COMMA, ",", start);
            case '!': return match('=') ? new Token(Type.NE, "!=", start) : new Token(Type.NOT, "!", start);
            case '=':
                if (match('=')) return new Token(Type.EQ, "==", start);
                throw error("Expected ==, found =", start);
            case '<': return match('=') ? new Token(Type.LE, "<=", start) : new Token(Type.LT, "<", start);
            case '>': return match('=') ? new Token(Type.GE, ">=", start) : new Token(Type.GT, ">", start);
            case '&':
                if (match('&')) return new Token(Type.AND, "&&", start);
                throw error("Expected &&", start);
            case '|':
                if (match('|')) return new Token(Type.OR, "||", start);
                throw error("Expected ||", start);
            default: throw error("Unexpected character [" + c + "]", start);
        }
    }

    private Token stringToken(final char quote) {
        final int start = position;
        position++;
        final StringBuilder sb = new StringBuilder();
        while (position < length) {
            final char c = input.charAt(position++);
            if (c == quote) return new Token(Type.STRING, sb.toString(), start);
            if (c == '\\') {
                if (position >= length) throw error("Unterminated escape sequence", position - 1);
                final char e = input.charAt(position++);
                switch (e) {
                    case '"': sb.append('"'); break;
                    case '\'': sb.append('\''); break;
                    case '\\': sb.append('\\'); break;
                    case '/': sb.append('/'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    case 'u':
                        if (position + 4 > length) throw error("Invalid unicode escape", position - 2);
                        final String hex = input.substring(position, position + 4);
                        try {
                            sb.append((char) Integer.parseInt(hex, 16));
                        } catch (NumberFormatException ex) {
                            throw error("Invalid unicode escape [" + hex + "]", position - 2);
                        }
                        position += 4;
                        break;
                    default: sb.append(e);
                }
            } else {
                sb.append(c);
            }
        }
        throw error("Unterminated string literal", start);
    }

    private void skipWhitespace() {
        while (position < length && Character.isWhitespace(input.charAt(position))) position++;
    }

    private boolean match(final char expected) {
        if (position < length && input.charAt(position) == expected) {
            position++;
            return true;
        }
        return false;
    }

    private static boolean isIdentifierStart(final char c) {
        return Character.isLetter(c) || c == '_' || c == '@';
    }

    private static boolean isIdentifierPart(final char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '.' || c == '@';
    }

    private ExpressionCompileException error(final String message, final int at) {
        return new ExpressionCompileException(message + " at character " + at);
    }
}
