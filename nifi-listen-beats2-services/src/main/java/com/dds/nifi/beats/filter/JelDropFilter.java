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

package com.dds.nifi.beats.filter;

import com.dds.nifi.beats.model.EventPayload;
import com.dds.nifi.beats.protocol.ProtocolException;
import com.dds.nifi.beats.server.ProcessorMetrics;
import com.dds.nifi.routendjson.expression.CompiledExpression;
import com.dds.nifi.routendjson.expression.EvaluationContext;
import com.dds.nifi.routendjson.expression.ExpressionCompiler;
import com.dds.nifi.routendjson.expression.IndexAnchor;
import com.dds.nifi.routendjson.expression.Value;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * Immutable pre-ACK JEL filter using candidate-indexed, selective streaming extraction.
 *
 * <p>Filtering disabled preserves the raw no-JSON-parse path. When enabled, pass one validates
 * the complete event and extracts safe equality/in() anchor fields. A second pass is performed
 * only when the selected candidates or JSON-key batching require additional fields. If all rules
 * are unindexed, all required fields are extracted in one validation pass.</p>
 *
 * <p>The raw pooled event payload is never copied, advanced, rewritten, or retained again. Each
 * ordered processing worker reuses its candidate BitSet, required-path BitSet, Value array, and
 * EvaluationContext. JSON batch-key values are extracted in the same pass plan, avoiding a third
 * JSON parse when JEL and field-based batching are both enabled.</p>
 */
public final class JelDropFilter {
    private static final int MAX_EXTRACTED_CONTAINER_DEPTH = 32;
    private static final int MAX_EXTRACTED_CONTAINER_NODES = 16_384;
    private static final int MAX_EXTRACTED_CONTAINER_TEXT_CHARACTERS = 2 * 1024 * 1024;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();

    private static final JelDropFilter DISABLED = new JelDropFilter(
            EventFilteringMode.DISABLED,
            FilterEvaluationErrorPolicy.KEEP_EVENT,
            DroppedEventAuditMode.NONE,
            Collections.emptyList(),
            PathPlan.empty(),
            CandidateIndex.empty(),
            null);

    private final EventFilteringMode mode;
    private final FilterEvaluationErrorPolicy errorPolicy;
    private final DroppedEventAuditMode auditMode;
    private final List<Rule> rules;
    private final PathPlan pathPlan;
    private final CandidateIndex candidateIndex;
    private final ProcessorMetrics metrics;
    private final boolean trackRuleMatches;

    private final LongAdder evaluatedEvents = new LongAdder();
    private final LongAdder candidateRulesSelected = new LongAdder();
    private final LongAdder ruleEvaluations = new LongAdder();
    private final LongAdder jsonPasses = new LongAdder();
    private final LongAdder extractedValues = new LongAdder();

    private final ThreadLocal<FilterScratch> scratch;

    private JelDropFilter(
            final EventFilteringMode mode,
            final FilterEvaluationErrorPolicy errorPolicy,
            final DroppedEventAuditMode auditMode,
            final List<Rule> rules,
            final PathPlan pathPlan,
            final CandidateIndex candidateIndex,
            final ProcessorMetrics metrics) {
        this.mode = Objects.requireNonNull(mode, "Filtering mode required");
        this.errorPolicy = Objects.requireNonNull(errorPolicy, "Error policy required");
        this.auditMode = Objects.requireNonNull(auditMode, "Audit mode required");
        this.rules = Collections.unmodifiableList(new ArrayList<>(rules));
        this.pathPlan = Objects.requireNonNull(pathPlan, "Path plan required");
        this.candidateIndex = Objects.requireNonNull(candidateIndex, "Candidate index required");
        this.metrics = metrics;
        this.trackRuleMatches = auditMode == DroppedEventAuditMode.COUNTERS_ONLY;
        this.scratch = ThreadLocal.withInitial(() -> new FilterScratch(
                this.pathPlan,
                this.evaluatedEvents,
                this.candidateRulesSelected,
                this.ruleEvaluations,
                this.jsonPasses,
                this.extractedValues));
    }

    public static JelDropFilter disabled() {
        return DISABLED;
    }

    public static JelDropFilter compile(
            final EventFilteringMode mode,
            final FilterEvaluationErrorPolicy errorPolicy,
            final DroppedEventAuditMode auditMode,
            final Map<String, String> orderedRules,
            final ProcessorMetrics metrics) {
        return compile(mode, errorPolicy, auditMode, orderedRules, List.of(), metrics);
    }

    public static JelDropFilter compile(
            final EventFilteringMode mode,
            final FilterEvaluationErrorPolicy errorPolicy,
            final DroppedEventAuditMode auditMode,
            final Map<String, String> orderedRules,
            final List<String> jsonBatchPointers,
            final ProcessorMetrics metrics) {
        if (mode == EventFilteringMode.DISABLED) {
            return disabled();
        }

        final Map<String, String> safeRules = orderedRules == null ? Map.of() : orderedRules;
        final List<String> safeBatchPointers = jsonBatchPointers == null ? List.of() : List.copyOf(jsonBatchPointers);
        final List<RuleDefinition> definitions = new ArrayList<>();
        final LinkedHashSet<String> referencedPaths = new LinkedHashSet<>();
        int index = 0;
        for (Map.Entry<String, String> entry : safeRules.entrySet()) {
            final CompiledExpression expression = ExpressionCompiler.compile(entry.getValue());
            referencedPaths.addAll(expression.referencedPaths());
            definitions.add(new RuleDefinition(index++, entry.getKey(), expression));
        }

        final PathPlan pathPlan = PathPlan.build(referencedPaths, safeBatchPointers);
        final List<Rule> compiled = new ArrayList<>(definitions.size());
        for (RuleDefinition definition : definitions) {
            compiled.add(new Rule(
                    definition.index,
                    definition.name,
                    definition.expression,
                    pathPlan.pathMask(definition.expression.referencedPaths()),
                    auditMode == DroppedEventAuditMode.COUNTERS_ONLY));
        }

        return new JelDropFilter(
                mode,
                errorPolicy,
                auditMode,
                compiled,
                pathPlan,
                CandidateIndex.build(compiled, pathPlan),
                metrics);
    }

    public boolean isEnabled() {
        return mode == EventFilteringMode.DROP_MATCHING && !rules.isEmpty();
    }

    public EventFilteringMode mode() {
        return mode;
    }

    public int getRuleCount() {
        return rules.size();
    }

    public int getIndexedRuleCount() {
        return candidateIndex.getIndexedRuleCount();
    }

    public int getUnindexedRuleCount() {
        return candidateIndex.getUnindexedRuleCount();
    }

    public int getReferencedPathCount() {
        return pathPlan.jelPathCount();
    }

    public int getAnchorPathCount() {
        return candidateIndex.getAnchorPathCount();
    }

    public int getBatchPathCount() {
        return pathPlan.batchPathCount();
    }

    public long getEvaluatedEventCount() {
        return evaluatedEvents.sum();
    }

    public long getCandidateRulesSelectedCount() {
        return candidateRulesSelected.sum();
    }

    public long getRuleEvaluationCount() {
        return ruleEvaluations.sum();
    }

    public long getJsonPassCount() {
        return jsonPasses.sum();
    }

    public long getExtractedValueCount() {
        return extractedValues.sum();
    }

    public double getAverageCandidateRulesPerEvent() {
        final long events = getEvaluatedEventCount();
        return events == 0L ? 0.0d : (double) getCandidateRulesSelectedCount() / (double) events;
    }

    public double getAverageRuleEvaluationsPerEvent() {
        final long events = getEvaluatedEventCount();
        return events == 0L ? 0.0d : (double) getRuleEvaluationCount() / (double) events;
    }

    public double getAverageJsonPassesPerEvent() {
        final long events = getEvaluatedEventCount();
        return events == 0L ? 0.0d : (double) getJsonPassCount() / (double) events;
    }

    public double getAverageExtractedValuesPerEvent() {
        final long events = getEvaluatedEventCount();
        return events == 0L ? 0.0d : (double) getExtractedValueCount() / (double) events;
    }

    /** Evaluates one raw event without changing or releasing its pooled payload. */
    public Decision evaluate(final EventPayload payload) {
        Objects.requireNonNull(payload, "Event payload required");
        if (!isEnabled()) {
            return Decision.keep(null);
        }

        if (metrics != null) {
            metrics.filterEventsInput.increment();
        }

        final FilterScratch state = scratch.get();
        state.reset();
        int candidateCount = 0;
        int evaluatedCount = 0;
        int passCount = 0;
        int extractedCount = 0;

        try {
            if (candidateIndex.getIndexedRuleCount() == 0) {
                candidateIndex.selectCandidates(state.values, state.candidates);
                candidateCount = state.candidates.cardinality();
                addCandidateRequiredPaths(state);
                state.requiredPaths.or(pathPlan.batchPathMask);
                extractedCount += StreamingExtractor.extract(
                        payload, pathPlan, state.requiredPaths, state.values);
                passCount++;
            } else {
                extractedCount += StreamingExtractor.extract(
                        payload, pathPlan, candidateIndex.anchorPathIds, state.values);
                passCount++;

                candidateIndex.selectCandidates(state.values, state.candidates);
                candidateCount = state.candidates.cardinality();
                if (candidateCount == 0) {
                    state.requiredPaths.or(pathPlan.batchPathMask);
                    state.requiredPaths.andNot(candidateIndex.anchorPathIds);
                    if (!state.requiredPaths.isEmpty()) {
                        extractedCount += StreamingExtractor.extract(
                                payload, pathPlan, state.requiredPaths, state.values);
                        passCount++;
                    }
                    state.accumulator.record(candidateCount, evaluatedCount, passCount, extractedCount);
                    recordKeep();
                    return Decision.keep(pathPlan.batchValues(state.values));
                }

                addCandidateRequiredPaths(state);
                state.requiredPaths.or(pathPlan.batchPathMask);
                state.requiredPaths.andNot(candidateIndex.anchorPathIds);
                if (!state.requiredPaths.isEmpty()) {
                    extractedCount += StreamingExtractor.extract(
                            payload, pathPlan, state.requiredPaths, state.values);
                    passCount++;
                }
            }

            for (int ruleIndex = state.candidates.nextSetBit(0);
                    ruleIndex >= 0;
                    ruleIndex = state.candidates.nextSetBit(ruleIndex + 1)) {
                final Rule rule = rules.get(ruleIndex);
                evaluatedCount++;
                if (rule.expression.evaluateBoolean(state.context)) {
                    state.accumulator.record(candidateCount, evaluatedCount, passCount, extractedCount);
                    recordDrop(rule, payload.length());
                    return Decision.drop(rule.name, false);
                }
            }

            state.accumulator.record(candidateCount, evaluatedCount, passCount, extractedCount);
            recordKeep();
            return Decision.keep(pathPlan.batchValues(state.values));
        } catch (final RuntimeException | IOException e) {
            state.accumulator.record(candidateCount, evaluatedCount, passCount, extractedCount);
            if (metrics != null) {
                metrics.filterEvaluationErrors.increment();
            }
            return switch (errorPolicy) {
                case DROP_EVENT -> {
                    if (metrics != null) {
                        metrics.filterEventsDropped.increment();
                        metrics.filterBytesDropped.add(payload.length());
                    }
                    yield Decision.drop("evaluation_error", true);
                }
                case CLOSE_CONNECTION -> throw new ProtocolException("JEL filter evaluation failed", e);
                case KEEP_EVENT -> {
                    recordKeep();
                    yield Decision.keepWithError();
                }
            };
        }
    }

    private void addCandidateRequiredPaths(final FilterScratch state) {
        for (int ruleIndex = state.candidates.nextSetBit(0);
                ruleIndex >= 0;
                ruleIndex = state.candidates.nextSetBit(ruleIndex + 1)) {
            state.requiredPaths.or(rules.get(ruleIndex).requiredPathIds);
        }
    }

    private void recordKeep() {
        if (metrics != null) {
            metrics.filterEventsKept.increment();
        }
    }

    private void recordDrop(final Rule rule, final int length) {
        if (trackRuleMatches) {
            rule.matches.increment();
        }
        if (metrics != null) {
            metrics.filterEventsDropped.increment();
            metrics.filterBytesDropped.add(length);
        }
    }

    public Map<String, Long> snapshotRuleMatches() {
        if (!trackRuleMatches || rules.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, Long> result = new LinkedHashMap<>();
        for (Rule rule : rules) {
            result.put(rule.name, rule.matches.sum());
        }
        return Collections.unmodifiableMap(result);
    }

    private static final class RuleDefinition {
        private final int index;
        private final String name;
        private final CompiledExpression expression;

        private RuleDefinition(
                final int index,
                final String name,
                final CompiledExpression expression) {
            this.index = index;
            this.name = Objects.requireNonNull(name, "Rule name required");
            this.expression = Objects.requireNonNull(expression, "Compiled expression required");
        }
    }

    private static final class Rule {
        private final int index;
        private final String name;
        private final CompiledExpression expression;
        private final BitSet requiredPathIds;
        private final LongAdder matches;

        private Rule(
                final int index,
                final String name,
                final CompiledExpression expression,
                final BitSet requiredPathIds,
                final boolean trackMatches) {
            this.index = index;
            this.name = Objects.requireNonNull(name, "Rule name required");
            this.expression = Objects.requireNonNull(expression, "Compiled expression required");
            this.requiredPathIds = (BitSet) Objects.requireNonNull(
                    requiredPathIds,
                    "Required paths required").clone();
            this.matches = trackMatches ? new LongAdder() : null;
        }
    }

    private static final class CandidateIndex {
        private static final CandidateIndex EMPTY = new CandidateIndex(
                new BitSet(),
                new BitSet(),
                Collections.<Integer, Map<String, BitSet>>emptyMap(),
                0,
                0);

        private final BitSet unindexedRules;
        private final BitSet anchorPathIds;
        private final Map<Integer, Map<String, BitSet>> index;
        private final int indexedRuleCount;
        private final int unindexedRuleCount;

        private CandidateIndex(
                final BitSet unindexedRules,
                final BitSet anchorPathIds,
                final Map<Integer, Map<String, BitSet>> index,
                final int indexedRuleCount,
                final int unindexedRuleCount) {
            this.unindexedRules = (BitSet) unindexedRules.clone();
            this.anchorPathIds = (BitSet) anchorPathIds.clone();
            this.index = index;
            this.indexedRuleCount = indexedRuleCount;
            this.unindexedRuleCount = unindexedRuleCount;
        }

        static CandidateIndex empty() {
            return EMPTY;
        }

        static CandidateIndex build(final List<Rule> rules, final PathPlan pathPlan) {
            if (rules.isEmpty()) {
                return empty();
            }

            final BitSet unindexed = new BitSet(rules.size());
            final BitSet anchorPaths = new BitSet(pathPlan.pathCount());
            final Map<Integer, Map<String, BitSet>> index =
                    new HashMap<Integer, Map<String, BitSet>>();
            int indexedCount = 0;

            for (Rule rule : rules) {
                final List<IndexAnchor> anchors = rule.expression.indexAnchors();
                boolean indexed = false;
                for (IndexAnchor anchor : anchors) {
                    final int pathId = pathPlan.pathId(anchor.path());
                    if (pathId < 0) {
                        continue;
                    }
                    anchorPaths.set(pathId);
                    final Map<String, BitSet> valueMap = index.computeIfAbsent(
                            pathId,
                            ignored -> new HashMap<String, BitSet>());
                    for (Value value : anchor.values()) {
                        for (String key : indexKeys(value)) {
                            valueMap.computeIfAbsent(
                                    key,
                                    ignored -> new BitSet(rules.size())).set(rule.index);
                            indexed = true;
                        }
                    }
                }
                if (indexed) {
                    indexedCount++;
                } else {
                    unindexed.set(rule.index);
                }
            }

            return new CandidateIndex(
                    unindexed,
                    anchorPaths,
                    immutableIndex(index),
                    indexedCount,
                    unindexed.cardinality());
        }

        private static Map<Integer, Map<String, BitSet>> immutableIndex(
                final Map<Integer, Map<String, BitSet>> source) {
            final Map<Integer, Map<String, BitSet>> outer =
                    new HashMap<Integer, Map<String, BitSet>>();
            for (Map.Entry<Integer, Map<String, BitSet>> pathEntry : source.entrySet()) {
                final Map<String, BitSet> inner = new HashMap<String, BitSet>();
                for (Map.Entry<String, BitSet> valueEntry : pathEntry.getValue().entrySet()) {
                    inner.put(valueEntry.getKey(), (BitSet) valueEntry.getValue().clone());
                }
                outer.put(pathEntry.getKey(), Collections.unmodifiableMap(inner));
            }
            return Collections.unmodifiableMap(outer);
        }

        int getIndexedRuleCount() {
            return indexedRuleCount;
        }

        int getUnindexedRuleCount() {
            return unindexedRuleCount;
        }

        int getAnchorPathCount() {
            return anchorPathIds.cardinality();
        }

        void selectCandidates(final Value[] values, final BitSet result) {
            result.clear();
            result.or(unindexedRules);
            for (Map.Entry<Integer, Map<String, BitSet>> pathEntry : index.entrySet()) {
                final int pathId = pathEntry.getKey();
                final Value value = pathId >= 0 && pathId < values.length
                        ? values[pathId]
                        : Value.MISSING;
                for (String key : indexKeys(value == null ? Value.MISSING : value)) {
                    final BitSet hits = pathEntry.getValue().get(key);
                    if (hits != null) {
                        result.or(hits);
                    }
                }
            }
        }

        private static List<String> indexKeys(final Value value) {
            if (value.kind() == Value.Kind.ARRAY) {
                final List<String> keys = new ArrayList<String>();
                for (Value item : value.asArrayOrEmpty()) {
                    keys.addAll(indexKeys(item));
                }
                return keys;
            }

            final String primary = value.indexKeyOrNull();
            if (primary == null) {
                return Collections.emptyList();
            }
            if (value.kind() == Value.Kind.NUMBER) {
                final String numericString = value.asStringOrNull();
                final List<String> keys = new ArrayList<String>(2);
                keys.add(primary);
                keys.add("S:" + numericString);
                return keys;
            }
            if (value.kind() == Value.Kind.STRING && value.asNumberOrNull() != null) {
                final List<String> keys = new ArrayList<String>(2);
                keys.add(primary);
                keys.add("N:" + value.asNumberOrNull().toPlainString());
                return keys;
            }
            return Collections.singletonList(primary);
        }
    }

    private static final class PathPlan {
        private static final PathPlan EMPTY = new PathPlan(
                Collections.emptyMap(),
                new TrieNode(),
                0,
                0,
                new int[0],
                new BitSet());

        private final Map<String, Integer> pathIds;
        private final TrieNode root;
        private final int pathCount;
        private final int jelPathCount;
        private final int[] batchPathIds;
        private final BitSet batchPathMask;

        private PathPlan(
                final Map<String, Integer> pathIds,
                final TrieNode root,
                final int pathCount,
                final int jelPathCount,
                final int[] batchPathIds,
                final BitSet batchPathMask) {
            this.pathIds = pathIds;
            this.root = root;
            this.pathCount = pathCount;
            this.jelPathCount = jelPathCount;
            this.batchPathIds = batchPathIds;
            this.batchPathMask = (BitSet) batchPathMask.clone();
        }

        static PathPlan empty() {
            return EMPTY;
        }

        static PathPlan build(final Set<String> paths, final List<String> batchPointers) {
            if (paths.isEmpty() && batchPointers.isEmpty()) {
                return empty();
            }

            final LinkedHashMap<String, Integer> jelIds = new LinkedHashMap<>();
            final LinkedHashMap<List<String>, Integer> uniqueIds = new LinkedHashMap<>();
            final TrieNode root = new TrieNode();

            for (String path : paths) {
                if (path == null || path.isEmpty()) {
                    continue;
                }
                final List<String> segments = List.of(path.split("\\."));
                final int pathId = addPath(uniqueIds, root, segments);
                jelIds.put(path, pathId);
            }

            final int[] batchIds = new int[batchPointers.size()];
            final BitSet batchMask = new BitSet();
            for (int index = 0; index < batchPointers.size(); index++) {
                final List<String> segments = compileJsonPointer(batchPointers.get(index));
                final int pathId = addPath(uniqueIds, root, segments);
                batchIds[index] = pathId;
                batchMask.set(pathId);
            }

            return new PathPlan(
                    Collections.unmodifiableMap(jelIds),
                    root,
                    uniqueIds.size(),
                    jelIds.size(),
                    batchIds,
                    batchMask);
        }

        private static int addPath(
                final Map<List<String>, Integer> uniqueIds,
                final TrieNode root,
                final List<String> inputSegments) {
            final List<String> segments = List.copyOf(inputSegments);
            final Integer existing = uniqueIds.get(segments);
            if (existing != null) {
                return existing;
            }

            final int pathId = uniqueIds.size();
            uniqueIds.put(segments, pathId);
            TrieNode node = root;
            root.descendantPathIds.set(pathId);
            for (String segment : segments) {
                node = node.children.computeIfAbsent(segment, ignored -> new TrieNode());
                node.descendantPathIds.set(pathId);
            }
            node.terminalPathId = pathId;
            return pathId;
        }

        private static List<String> compileJsonPointer(final String pointer) {
            if (pointer == null || !pointer.startsWith("/") || pointer.length() == 1) {
                throw new IllegalArgumentException(
                        "JSON Pointer must start with '/' and identify a field: " + pointer);
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

        int pathCount() {
            return pathCount;
        }

        int jelPathCount() {
            return jelPathCount;
        }

        int batchPathCount() {
            return batchPathIds.length;
        }

        int pathId(final String source) {
            final Integer id = pathIds.get(source);
            return id == null ? -1 : id;
        }

        BitSet pathMask(final Set<String> paths) {
            final BitSet result = new BitSet(pathCount);
            for (String path : paths) {
                final int id = pathId(path);
                if (id >= 0) {
                    result.set(id);
                }
            }
            return result;
        }

        List<String> batchValues(final Value[] values) {
            if (batchPathIds.length == 0) {
                return null;
            }
            final String[] extracted = new String[batchPathIds.length];
            for (int index = 0; index < batchPathIds.length; index++) {
                final Value value = values[batchPathIds[index]];
                extracted[index] = value == null ? null : value.asStringOrNull();
            }
            return Collections.unmodifiableList(Arrays.asList(extracted));
        }
    }

    private static final class TrieNode {
        private final Map<String, TrieNode> children = new HashMap<>();
        private final BitSet descendantPathIds = new BitSet();
        private int terminalPathId = -1;
    }

    private static final class FilterScratch {
        private final Value[] values;
        private final EvaluationContext context;
        private final BitSet candidates = new BitSet();
        private final BitSet requiredPaths = new BitSet();
        private final EvaluationAccumulator accumulator;

        private FilterScratch(
                final PathPlan pathPlan,
                final LongAdder evaluatedEvents,
                final LongAdder candidateRulesSelected,
                final LongAdder ruleEvaluations,
                final LongAdder jsonPasses,
                final LongAdder extractedValues) {
            this.values = new Value[pathPlan.pathCount()];
            Arrays.fill(this.values, Value.MISSING);
            this.context = new EvaluationContext(pathPlan.pathIds, this.values);
            this.accumulator = new EvaluationAccumulator(
                    evaluatedEvents,
                    candidateRulesSelected,
                    ruleEvaluations,
                    jsonPasses,
                    extractedValues);
        }

        private void reset() {
            Arrays.fill(values, Value.MISSING);
            candidates.clear();
            requiredPaths.clear();
        }
    }

    private static final class StreamingExtractor {
        private StreamingExtractor() { }

        static int extract(
                final EventPayload payload,
                final PathPlan pathPlan,
                final BitSet targetPathIds,
                final Value[] values) throws IOException {
            try (InputStream input = payload.openStream();
                    JsonParser parser = JSON_FACTORY.createParser(input)) {
                final JsonToken first = parser.nextToken();
                if (first != JsonToken.START_OBJECT) {
                    throw new IOException("Beats event payload is not a JSON object");
                }
                final int extracted = extractObject(
                        parser,
                        pathPlan.root,
                        targetPathIds,
                        values,
                        new ExtractionBudget());
                if (parser.nextToken() != null) {
                    throw new IOException("Trailing content after Beats JSON event");
                }
                return extracted;
            }
        }

        private static int extractObject(
                final JsonParser parser,
                final TrieNode node,
                final BitSet targets,
                final Value[] values,
                final ExtractionBudget budget) throws IOException {
            int extracted = 0;
            JsonToken token;
            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                if (token == null) {
                    throw new IOException("Unexpected end of JSON object");
                }
                if (token != JsonToken.FIELD_NAME) {
                    throw new IOException("Expected JSON field name, found " + token);
                }

                final String fieldName = parser.currentName();
                final JsonToken valueToken = parser.nextToken();
                if (valueToken == null) {
                    throw new IOException("Unexpected end of JSON value for field " + fieldName);
                }

                final TrieNode child = node.children.get(fieldName);
                if (child == null || !child.descendantPathIds.intersects(targets)) {
                    parser.skipChildren();
                    continue;
                }

                final boolean terminalTarget = child.terminalPathId >= 0
                        && targets.get(child.terminalPathId);
                final boolean descendantTarget = hasTargetedDescendant(child, targets);

                if (valueToken == JsonToken.START_OBJECT) {
                    if (terminalTarget) {
                        final JsonNode object = readBoundedNode(parser, valueToken, budget, 1);
                        extracted += populateFromNode(child, object, targets, values);
                    } else if (descendantTarget) {
                        extracted += extractObject(parser, child, targets, values, budget);
                    } else {
                        parser.skipChildren();
                    }
                } else if (terminalTarget) {
                    values[child.terminalPathId] = readValue(parser, valueToken, budget, 0);
                    extracted++;
                } else {
                    // JEL dotted-path traversal does not descend through arrays
                    // or scalars, so deeper paths remain MISSING.
                    parser.skipChildren();
                }
            }
            return extracted;
        }

        private static boolean hasTargetedDescendant(
                final TrieNode node,
                final BitSet targets) {
            if (node.children.isEmpty()) {
                return false;
            }
            for (TrieNode child : node.children.values()) {
                if (child.descendantPathIds.intersects(targets)) {
                    return true;
                }
            }
            return false;
        }

        private static int populateFromNode(
                final TrieNode node,
                final JsonNode current,
                final BitSet targets,
                final Value[] values) {
            int extracted = 0;
            if (node.terminalPathId >= 0 && targets.get(node.terminalPathId)) {
                values[node.terminalPathId] = Value.fromJsonNode(current);
                extracted++;
            }
            if (current != null && current.isObject()) {
                for (Map.Entry<String, TrieNode> childEntry : node.children.entrySet()) {
                    final TrieNode child = childEntry.getValue();
                    if (!child.descendantPathIds.intersects(targets)) {
                        continue;
                    }
                    final JsonNode childNode = current.get(childEntry.getKey());
                    if (childNode != null) {
                        extracted += populateFromNode(child, childNode, targets, values);
                    }
                }
            }
            return extracted;
        }

        private static Value readValue(
                final JsonParser parser,
                final JsonToken token,
                final ExtractionBudget budget,
                final int depth) throws IOException {
            switch (token) {
                case VALUE_STRING:
                    return Value.ofString(parser.getText());
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    return Value.ofNumber(new BigDecimal(parser.getText()));
                case VALUE_TRUE:
                    return Value.TRUE;
                case VALUE_FALSE:
                    return Value.FALSE;
                case VALUE_NULL:
                    return Value.NULL;
                case START_ARRAY:
                    return Value.fromJsonNode(readBoundedNode(parser, token, budget, depth + 1));
                case START_OBJECT:
                    return Value.ofObject(readBoundedNode(parser, token, budget, depth + 1));
                default:
                    throw new IOException("Unsupported JSON token " + token);
            }
        }

        private static JsonNode readBoundedNode(
                final JsonParser parser,
                final JsonToken token,
                final ExtractionBudget budget,
                final int depth) throws IOException {
            budget.checkDepth(depth);
            final JsonNodeFactory factory = JsonNodeFactory.instance;
            return switch (token) {
                case VALUE_STRING -> {
                    final String text = parser.getText();
                    budget.addText(text.length());
                    yield factory.textNode(text);
                }
                case VALUE_NUMBER_INT -> factory.numberNode(parser.getBigIntegerValue());
                case VALUE_NUMBER_FLOAT -> factory.numberNode(parser.getDecimalValue());
                case VALUE_TRUE -> factory.booleanNode(true);
                case VALUE_FALSE -> factory.booleanNode(false);
                case VALUE_NULL -> factory.nullNode();
                case START_ARRAY -> {
                    final ArrayNode array = factory.arrayNode();
                    JsonToken item;
                    while ((item = parser.nextToken()) != JsonToken.END_ARRAY) {
                        if (item == null) {
                            throw new IOException("Unexpected end of JSON array");
                        }
                        budget.addNode();
                        array.add(readBoundedNode(parser, item, budget, depth + 1));
                    }
                    yield array;
                }
                case START_OBJECT -> {
                    final ObjectNode object = factory.objectNode();
                    JsonToken fieldToken;
                    while ((fieldToken = parser.nextToken()) != JsonToken.END_OBJECT) {
                        if (fieldToken == null) {
                            throw new IOException("Unexpected end of JSON object");
                        }
                        if (fieldToken != JsonToken.FIELD_NAME) {
                            throw new IOException("Expected JSON field name, found " + fieldToken);
                        }
                        final String fieldName = parser.currentName();
                        budget.addText(fieldName.length());
                        final JsonToken valueToken = parser.nextToken();
                        if (valueToken == null) {
                            throw new IOException("Unexpected end of JSON value for field " + fieldName);
                        }
                        budget.addNode();
                        object.set(fieldName, readBoundedNode(parser, valueToken, budget, depth + 1));
                    }
                    yield object;
                }
                default -> throw new IOException("Unsupported JSON token " + token);
            };
        }

        private static final class ExtractionBudget {
            private int nodes;
            private int textCharacters;

            private void checkDepth(final int depth) throws IOException {
                if (depth > MAX_EXTRACTED_CONTAINER_DEPTH) {
                    throw new IOException("JEL extracted container exceeds maximum depth "
                            + MAX_EXTRACTED_CONTAINER_DEPTH);
                }
            }

            private void addNode() throws IOException {
                nodes++;
                if (nodes > MAX_EXTRACTED_CONTAINER_NODES) {
                    throw new IOException("JEL extracted containers exceed maximum node count "
                            + MAX_EXTRACTED_CONTAINER_NODES);
                }
            }

            private void addText(final int characters) throws IOException {
                if (characters > MAX_EXTRACTED_CONTAINER_TEXT_CHARACTERS - textCharacters) {
                    throw new IOException("JEL extracted containers exceed maximum text size "
                            + MAX_EXTRACTED_CONTAINER_TEXT_CHARACTERS + " characters");
                }
                textCharacters += characters;
            }
        }
    }

    private static final class EvaluationAccumulator {
        private final LongAdder evaluatedEvents;
        private final LongAdder candidateRulesSelected;
        private final LongAdder ruleEvaluations;
        private final LongAdder jsonPasses;
        private final LongAdder extractedValues;

        private EvaluationAccumulator(
                final LongAdder evaluatedEvents,
                final LongAdder candidateRulesSelected,
                final LongAdder ruleEvaluations,
                final LongAdder jsonPasses,
                final LongAdder extractedValues) {
            this.evaluatedEvents = evaluatedEvents;
            this.candidateRulesSelected = candidateRulesSelected;
            this.ruleEvaluations = ruleEvaluations;
            this.jsonPasses = jsonPasses;
            this.extractedValues = extractedValues;
        }

        private void record(
                final int candidateCount,
                final int evaluatedCount,
                final int passCount,
                final int extractedCount) {
            evaluatedEvents.increment();
            candidateRulesSelected.add(Math.max(0, candidateCount));
            ruleEvaluations.add(Math.max(0, evaluatedCount));
            jsonPasses.add(Math.max(0, passCount));
            extractedValues.add(Math.max(0, extractedCount));
        }
    }

    public static final class Decision {
        private static final Decision KEEP = new Decision(false, null, false, null);
        private static final Decision KEEP_WITH_ERROR = new Decision(false, null, true, null);

        private final boolean drop;
        private final String ruleName;
        private final boolean evaluationError;
        private final List<String> jsonBatchValues;

        private Decision(
                final boolean drop,
                final String ruleName,
                final boolean evaluationError,
                final List<String> jsonBatchValues) {
            this.drop = drop;
            this.ruleName = ruleName;
            this.evaluationError = evaluationError;
            this.jsonBatchValues = jsonBatchValues;
        }

        public static Decision keep(final List<String> jsonBatchValues) {
            return jsonBatchValues == null ? KEEP
                    : new Decision(false, null, false, jsonBatchValues);
        }

        public static Decision keepWithError() {
            return KEEP_WITH_ERROR;
        }

        public static Decision drop(final String ruleName, final boolean evaluationError) {
            return new Decision(true, ruleName, evaluationError, null);
        }

        public boolean isDrop() { return drop; }
        public String getRuleName() { return ruleName; }
        public boolean isEvaluationError() { return evaluationError; }
        public List<String> jsonBatchValues() { return jsonBatchValues; }
    }

}
