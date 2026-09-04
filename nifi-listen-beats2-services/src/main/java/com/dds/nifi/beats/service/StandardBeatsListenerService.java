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

package com.dds.nifi.beats.service;

import com.dds.nifi.beats.api.BatchClaim;
import com.dds.nifi.beats.api.BeatsListenerService;
import com.dds.nifi.beats.api.ListenerSnapshot;
import com.dds.nifi.beats.batch.BatchConfig;
import com.dds.nifi.beats.batch.BatchCoordinator;
import com.dds.nifi.beats.batch.BatchingStrategy;
import com.dds.nifi.beats.batch.MissingKeyPolicy;
import com.dds.nifi.beats.batch.JsonPointerExtractor;
import com.dds.nifi.beats.batch.ReadyBatch;
import com.dds.nifi.beats.filter.DroppedEventAuditMode;
import com.dds.nifi.beats.filter.EventFilteringMode;
import com.dds.nifi.beats.filter.FilterEvaluationErrorPolicy;
import com.dds.nifi.beats.filter.JelDropFilter;
import com.dds.nifi.beats.protocol.ProtocolLimits;
import com.dds.nifi.routendjson.expression.ExpressionCompileException;
import com.dds.nifi.routendjson.expression.ExpressionCompiler;
import com.dds.nifi.beats.server.AckCoordinator;
import com.dds.nifi.beats.server.BeatsServer;
import com.dds.nifi.beats.server.ConnectionRegistry;
import com.dds.nifi.beats.server.MemoryTracker;
import com.dds.nifi.beats.server.ProcessorMetrics;
import com.dds.nifi.beats.server.PressureController;
import com.dds.nifi.beats.server.ServerConfig;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextProvider;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Tags({"beats", "lumberjack", "filebeat", "winlogbeat", "metricbeat", "elastic-agent", "tcp", "tls"})
@CapabilityDescription("Owns a bounded Netty Beats/Lumberjack v2 listener, optional pre-ACK JEL filtering, and committed batch claims for ListenBeats2 processors.")
@DynamicProperty(
        name = "<drop-rule-name>",
        value = "JEL expression evaluated against each Beats JSON event",
        expressionLanguageScope = org.apache.nifi.expression.ExpressionLanguageScope.NONE,
        description = "Defines one pre-ACK drop rule using the same JEL syntax as RouteNdjsonEvents. "
                + "Rules are evaluated in ascending property-name order and the first matching rule drops the raw event.")
public final class StandardBeatsListenerService extends AbstractControllerService implements BeatsListenerService {
    private static final Validator OPTIONAL_STRING_VALIDATOR = (subject, input, context) ->
            new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .build();

    private static final Validator NON_NEGATIVE_INTEGER_VALIDATOR = (subject, input, context) -> {
        try {
            final int value = Integer.parseInt(input);
            return new ValidationResult.Builder().subject(subject).input(input).valid(value >= 0)
                    .explanation(value >= 0 ? null : "Value must be zero or greater").build();
        } catch (RuntimeException e) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation("Value must be an integer").build();
        }
    };

    private static final Validator JSON_POINTER_LIST_VALIDATOR = (subject, input, context) -> {
        try {
            final List<String> pointers = parsePointers(input);
            new JsonPointerExtractor(pointers);
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .build();
        } catch (IllegalArgumentException e) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation(e.getMessage())
                    .build();
        }
    };
    private static PropertyDescriptor integer(final String name, final String display, final String defaultValue, final String description) {
        return new PropertyDescriptor.Builder().name(name).displayName(display).description(description)
                .required(true).defaultValue(defaultValue).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
    }

    private static PropertyDescriptor dataSize(final String name, final String display, final String defaultValue, final String description) {
        return new PropertyDescriptor.Builder().name(name).displayName(display).description(description)
                .required(true).defaultValue(defaultValue).addValidator(StandardValidators.DATA_SIZE_VALIDATOR).build();
    }

    private static PropertyDescriptor duration(final String name, final String display, final String defaultValue, final String description) {
        return new PropertyDescriptor.Builder().name(name).displayName(display).description(description)
                .required(true).defaultValue(defaultValue).addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();
    }

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder().name("listening-port").displayName("Listening Port")
            .description("TCP port to bind on every NiFi node.").required(true).defaultValue("5044")
            .addValidator(StandardValidators.PORT_VALIDATOR).build();
    public static final PropertyDescriptor LOCAL_INTERFACE = new PropertyDescriptor.Builder().name("local-interface").displayName("Local Network Interface")
            .description("Interface name or address to bind. Blank binds on all interfaces.").required(false)
            .expressionLanguageSupported(org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(OPTIONAL_STRING_VALIDATOR).build();
    public static final PropertyDescriptor WORKER_THREADS = integer("worker-threads", "Worker Threads", "8", "Netty socket event-loop count.");
    public static final PropertyDescriptor EVENT_PROCESSING_THREADS = integer("event-processing-threads", "Event Processing Threads", "8", "Bounded worker threads for decompression and JSON batch-key extraction.");
    public static final PropertyDescriptor EVENT_PROCESSING_QUEUE = integer("event-processing-queue-capacity", "Event Processing Queue Capacity", "1024", "Maximum queued heavy-path frames per ordered event-processing worker.");
    public static final PropertyDescriptor EVENT_PROCESSING_QUEUE_HIGH_WATER = integer("event-processing-queue-high-water-percent", "Event Processing Queue High-Water Mark", "70", "Pause accepts and reads when the hottest event-processing worker reaches this percentage of its bounded queue.");
    public static final PropertyDescriptor EVENT_PROCESSING_QUEUE_LOW_WATER = integer("event-processing-queue-low-water-percent", "Event Processing Queue Low-Water Mark", "40", "Resume event-processing pressure below this percentage.");
    public static final PropertyDescriptor EVENT_PROCESSING_MAX_BYTES = dataSize("maximum-event-processing-bytes", "Maximum Event Processing Bytes", "256 MB", "Global bound for compressed input and temporary decompression/JSON extraction memory.");
    public static final PropertyDescriptor CLEANUP_WORKER_THREADS = integer("cleanup-worker-threads", "Connection Cleanup Worker Threads", "4", "Threads for disconnect cleanup that must not block Netty event loops.");
    public static final PropertyDescriptor MAX_CLEANUP_PENDING_TASKS = integer("maximum-cleanup-pending-tasks", "Maximum Connection Cleanup Pending Tasks", "60000", "Capacity of each bounded normal and emergency cleanup queue. Must be at least Maximum Connections.");
    public static final PropertyDescriptor CLEANUP_QUEUE_HIGH_WATER = integer("cleanup-queue-high-water-percent", "Cleanup Queue High-Water Mark", "70", "Pause new connection admission when normal cleanup work reaches this percentage or emergency cleanup is active.");
    public static final PropertyDescriptor CLEANUP_QUEUE_LOW_WATER = integer("cleanup-queue-low-water-percent", "Cleanup Queue Low-Water Mark", "40", "Resume connection admission after normal cleanup falls to this percentage and emergency cleanup is empty.");
    public static final PropertyDescriptor SOCKET_RECEIVE_BUFFER = dataSize("socket-receive-buffer", "Socket Receive Buffer", "64 KB", "Requested SO_RCVBUF per socket.");
    public static final PropertyDescriptor RECEIVE_FRAME_BUFFER = dataSize("receive-frame-buffer", "Maximum Receive Bytes per Read", "64 KB", "Bounds both one Netty receive allocation and aggregate bytes consumed during one socket read loop.");
    public static final PropertyDescriptor LISTEN_BACKLOG = integer("listen-backlog", "Listen Backlog", "8192", "TCP accept backlog.");
    public static final PropertyDescriptor TCP_KEEPALIVE = new PropertyDescriptor.Builder().name("tcp-keepalive").displayName("TCP Keepalive")
            .description("Enable TCP keepalive.").required(true).defaultValue("true").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
    public static final PropertyDescriptor FIRST_PROTOCOL_BYTE_TIMEOUT = duration("first-protocol-byte-timeout", "First Protocol Byte Timeout", "30 sec", "Close a connection that completes admission but sends no Beats/Lumberjack protocol byte within this duration. Listener-induced read suspension pauses the timeout.");
    public static final PropertyDescriptor PROTOCOL_IDLE_TIMEOUT = duration("idle-timeout", "Protocol Idle Timeout", "0 sec", "Close an established connection after this duration without received protocol bytes. Zero disables this timeout; listener pressure and outstanding unacknowledged work pause it.");
    public static final PropertyDescriptor FRAME_ASSEMBLY_TIMEOUT = duration("frame-assembly-timeout", "Frame Assembly Timeout", "30 sec", "Maximum active receive time to complete a declared JSON or compressed frame. Time spent paused by listener backpressure does not expire the frame.");
    public static final PropertyDescriptor EVENT_LOOP_LAG_INTERVAL = duration("event-loop-lag-probe-interval", "Event Loop Lag Probe Interval", "100 ms", "Interval for one bounded lag probe on each Netty socket event loop.");
    public static final PropertyDescriptor MAX_CONNECTIONS = integer("maximum-connections", "Maximum Connections", "30000", "Global connection limit.");
    public static final PropertyDescriptor MAX_CONNECTIONS_PER_SOURCE = new PropertyDescriptor.Builder()
            .name("maximum-connections-per-source").displayName("Maximum Connections per Source")
            .description("Remote-IP connection limit. Zero disables the per-source limit.")
            .required(true).defaultValue("1000").addValidator(NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor MAX_CONNECTION_ATTEMPTS_PER_SECOND = new PropertyDescriptor.Builder()
            .name("maximum-connection-attempts-per-second").displayName("Maximum Connection Attempts per Second")
            .description("Global fixed-window connection-attempt rate limit applied before TLS and pipeline initialization. Zero disables the limit.")
            .required(true).defaultValue("100000").addValidator(NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor MAX_CONNECTION_ATTEMPTS_PER_SOURCE_PER_SECOND = new PropertyDescriptor.Builder()
            .name("maximum-connection-attempts-per-source-per-second").displayName("Maximum Connection Attempts per Source per Second")
            .description("Remote-IP fixed-window connection-attempt rate limit. Zero disables the per-source rate limit; keep disabled behind source-address-masking proxies.")
            .required(true).defaultValue("0").addValidator(NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor MAX_PROTOCOL_FRAMES_PER_SECOND = new PropertyDescriptor.Builder()
            .name("maximum-protocol-frames-per-second").displayName("Maximum Protocol Frames per Connection per Second")
            .description("Per-connection fixed-window frame-rate limit protecting event-processing and ACK paths from zero-window or tiny-frame floods. Zero disables the limit.")
            .required(true).defaultValue("100000").addValidator(NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor POOLED_BUFFERS = new PropertyDescriptor.Builder().name("pooled-direct-buffers").displayName("Pooled Direct Buffers")
            .description("Use Netty pooled direct buffers.").required(true).defaultValue("true").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder().name("ssl-context-service").displayName("SSL Context Service")
            .description("Optional NiFi SSL Context Provider for TLS or mutual TLS.").required(false).identifiesControllerService(SSLContextProvider.class).build();
    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder().name("client-authentication").displayName("Client Authentication")
            .description("TLS client certificate policy.").required(true).defaultValue(ClientAuth.NONE.name()).allowableValues(ClientAuth.values()).build();
    public static final PropertyDescriptor TLS_HANDSHAKE_TIMEOUT = duration("tls-handshake-timeout", "TLS Handshake Timeout", "10 sec", "Maximum TLS handshake duration.");
    public static final PropertyDescriptor MAX_HANDSHAKES = integer("maximum-concurrent-handshakes", "Maximum Concurrent Handshakes", "256", "Global TLS handshake concurrency limit.");

    public static final PropertyDescriptor MAX_FRAME = dataSize("maximum-frame-size", "Maximum Frame Size", "16 MB", "Maximum uncompressed JSON frame payload.");
    public static final PropertyDescriptor MAX_COMPRESSED = dataSize("maximum-compressed-frame-size", "Maximum Compressed Frame Size", "8 MB", "Maximum compressed envelope payload.");
    public static final PropertyDescriptor MAX_DECOMPRESSED = dataSize("maximum-decompressed-size", "Maximum Decompressed Size", "64 MB", "Maximum output from one compressed envelope.");
    public static final PropertyDescriptor MAX_COMPRESSION_RATIO = integer("maximum-compression-ratio", "Maximum Compression Ratio", "100", "Maximum decompressed-to-compressed ratio.");
    public static final PropertyDescriptor MAX_FRAMES_COMPRESSED = integer("maximum-frames-per-compressed-envelope", "Maximum Frames per Compressed Envelope", "10000", "Maximum decoded frames in one compressed envelope.");
    public static final PropertyDescriptor MAX_EVENTS_WINDOW = integer("maximum-events-per-window", "Maximum Events per Window", "4096", "Receiver-side cap on one advertised Beats window.");
    public static final PropertyDescriptor MAX_OUTSTANDING_PER_CONNECTION = integer("maximum-outstanding-events-per-connection", "Maximum Outstanding Events per Connection", "16384", "Maximum received but not yet ACKed events on one connection across pipelined windows.");
    public static final PropertyDescriptor MAX_OUTSTANDING_BYTES_PER_CONNECTION = dataSize("maximum-outstanding-bytes-per-connection", "Maximum Outstanding Bytes per Connection", "32 MB", "Maximum raw event bytes received but not yet commit-eligible for ACK on one connection.");
    public static final PropertyDescriptor ACK_WRITE_TIMEOUT = duration("ack-write-timeout", "ACK Write Timeout", "15 sec", "Maximum time for a committed cumulative ACK write to complete before the connection is closed.");
    public static final PropertyDescriptor PROTOCOL_KEEPALIVE_INTERVAL = duration("protocol-keepalive-interval", "Protocol Keepalive Interval", "0 sec", "Repeat only a successfully written partial cumulative ACK while that same protocol window remains the current head. ACKs from completed windows are never replayed into later sequence-reset windows. Zero disables protocol keepalives.");

    public static final PropertyDescriptor MAX_QUEUED_EVENTS = integer("maximum-queued-events", "Maximum Queued Events", "250000", "Global in-memory event limit including active, ready, and claimed batches.");
    public static final PropertyDescriptor MAX_QUEUED_BYTES = dataSize("maximum-queued-bytes", "Maximum Queued Bytes", "1 GB", "Global raw JSON byte limit.");
    public static final PropertyDescriptor HIGH_WATER = integer("queue-high-water-percent", "Queue High-Water Mark", "80", "Suspend channel reads at this percentage.");
    public static final PropertyDescriptor LOW_WATER = integer("queue-low-water-percent", "Queue Low-Water Mark", "60", "Resume channel reads at or below this percentage.");


    public static final PropertyDescriptor EVENT_FILTERING = new PropertyDescriptor.Builder()
            .name("event-filtering").displayName("Event Filtering")
            .description("Controls pre-ACK JEL filtering. Disabled preserves the zero-parse fast path. Drop Matching Events evaluates dynamic JEL rules and omits matching raw events from NiFi output.")
            .required(true).defaultValue(EventFilteringMode.DISABLED.name())
            .allowableValues(EventFilteringMode.values()).build();
    public static final PropertyDescriptor FILTER_EVALUATION_ERROR_POLICY = new PropertyDescriptor.Builder()
            .name("filter-evaluation-error-policy").displayName("Filter Evaluation Error Policy")
            .description("Handling when JSON parsing or JEL evaluation fails. KEEP_EVENT is the safest default; CLOSE_CONNECTION closes without ACK so Beats retransmits.")
            .required(true).defaultValue(FilterEvaluationErrorPolicy.KEEP_EVENT.name())
            .allowableValues(FilterEvaluationErrorPolicy.values()).build();
    public static final PropertyDescriptor DROPPED_EVENT_AUDIT_MODE = new PropertyDescriptor.Builder()
            .name("dropped-event-audit-mode").displayName("Dropped Event Audit Mode")
            .description("Controls bounded rule-level counters. Dropped payloads are never written to FlowFiles or logs.")
            .required(true).defaultValue(DroppedEventAuditMode.COUNTERS_ONLY.name())
            .allowableValues(DroppedEventAuditMode.values()).build();
    public static final PropertyDescriptor MAXIMUM_FILTER_RULES = integer(
            "maximum-filter-rules", "Maximum Filter Rules", "500",
            "Maximum number of configured dynamic JEL drop rules. Candidate indexing keeps selective anchored rule sets efficient.");
    public static final PropertyDescriptor MAXIMUM_UNINDEXED_FILTER_RULES = new PropertyDescriptor.Builder()
            .name("maximum-unindexed-filter-rules").displayName("Maximum Unindexed Filter Rules")
            .description("Maximum rules without a safe equality or literal in() candidate anchor. Unindexed rules are candidates for every filtered event. Set zero to require every rule to be indexed.")
            .required(true).defaultValue("25").addValidator(NON_NEGATIVE_INTEGER_VALIDATOR).build();

    public static final PropertyDescriptor BATCH_STRATEGY = new PropertyDescriptor.Builder().name("batching-strategy").displayName("Batching Strategy")
            .description("NONE, PER_SOURCE, CONNECTION, WINDOW, PER_AGENT_TYPE, JSON_KV, SIZE_TIME, or HYBRID.").required(true)
            .defaultValue(BatchingStrategy.SIZE_TIME.name()).allowableValues(BatchingStrategy.values()).build();
    public static final PropertyDescriptor JSON_KEYS = new PropertyDescriptor.Builder().name("json-batch-key").displayName("JSON Batch Key")
            .description("Comma-separated JSON Pointers. Used only by JSON_KV or HYBRID; blank HYBRID defaults to /agent/type.")
            .required(false).defaultValue("").addValidator(JSON_POINTER_LIST_VALIDATOR).build();
    public static final PropertyDescriptor MISSING_KEY = new PropertyDescriptor.Builder().name("missing-key-policy").displayName("Missing Key Policy")
            .description("Behavior when a required JSON key is absent.").required(true).defaultValue(MissingKeyPolicy.DEFAULT_BUCKET.name())
            .allowableValues(MissingKeyPolicy.values()).build();
    public static final PropertyDescriptor DEFAULT_BUCKET = new PropertyDescriptor.Builder().name("default-batch-bucket").displayName("Default Batch Bucket")
            .description("Bucket for missing JSON values when allowed.").required(true).defaultValue("_missing").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor MAX_KEY_LENGTH = integer("maximum-batch-key-length", "Maximum Batch Key Length", "256", "Maximum UTF-8 key bytes before hashing.");
    public static final PropertyDescriptor HASH_KEYS = new PropertyDescriptor.Builder().name("hash-batch-keys").displayName("Hash Batch Keys")
            .description("SHA-256 hash batch keys before FlowFile attributes and internal map use.").required(true).defaultValue("false").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
    public static final PropertyDescriptor MAX_ACTIVE_KEYS = integer("maximum-active-batch-keys", "Maximum Active Batch Keys", "2048", "Maximum simultaneous partially-filled keys.");
    public static final PropertyDescriptor BATCH_PARTITIONS = integer("batch-coordinator-partitions", "Batch Coordinator Partitions", "16", "Power-of-two-normalized number of independently locked batch partitions.");
    public static final PropertyDescriptor MAX_EVENTS_BATCH = integer("maximum-events-per-batch", "Maximum Events per Batch", "10000", "Event-count flush threshold.");
    public static final PropertyDescriptor MAX_BYTES_BATCH = dataSize("maximum-bytes-per-batch", "Maximum Bytes per Batch", "8 MB", "Hard raw JSON byte limit for multi-event batches. One individually oversized event is allowed when Maximum Frame Size is larger.");
    public static final PropertyDescriptor MAX_BATCH_AGE = duration("maximum-batch-age", "Maximum Batch Age", "1 sec", "Time since first event before flush.");
    public static final PropertyDescriptor MAX_BATCH_IDLE = duration("maximum-batch-idle-time", "Maximum Batch Idle Time", "250 ms", "Time since last append before flush.");
    public static final PropertyDescriptor MAX_READY_BATCHES = integer("maximum-ready-batches", "Maximum Ready Batches", "2048", "Bound on flushed batches waiting for NiFi.");
    public static final PropertyDescriptor SHUTDOWN_DRAIN_TIMEOUT = duration(
            "shutdown-drain-timeout", "Shutdown Drain Timeout", "30 sec",
            "Maximum time to wait for claimed NiFi transactions to report commit or rollback during service disablement.");
    public static final PropertyDescriptor METRICS_ATTRIBUTE_DETAIL = new PropertyDescriptor.Builder()
            .name("metrics-attribute-detail").displayName("Metrics Attribute Detail")
            .description("Controls listener-wide operational metrics copied to each FlowFile. NONE minimizes FlowFile Repository metadata; BASIC is recommended; FULL is intended for qualification and troubleshooting.")
            .required(true).defaultValue(MetricsAttributeDetail.BASIC.name())
            .allowableValues(MetricsAttributeDetail.values()).build();
    public static final PropertyDescriptor METRICS_LOG_INTERVAL = duration(
            "metrics-log-interval", "Metrics Log Interval", "30 sec",
            "Interval for one sampled aggregate operational log. Zero disables periodic metric logging.");

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            PORT, LOCAL_INTERFACE, WORKER_THREADS, EVENT_PROCESSING_THREADS, EVENT_PROCESSING_QUEUE,
            EVENT_PROCESSING_QUEUE_HIGH_WATER, EVENT_PROCESSING_QUEUE_LOW_WATER, EVENT_PROCESSING_MAX_BYTES,
            CLEANUP_WORKER_THREADS, MAX_CLEANUP_PENDING_TASKS, CLEANUP_QUEUE_HIGH_WATER, CLEANUP_QUEUE_LOW_WATER,
            SOCKET_RECEIVE_BUFFER, RECEIVE_FRAME_BUFFER, LISTEN_BACKLOG, TCP_KEEPALIVE,
            FIRST_PROTOCOL_BYTE_TIMEOUT, PROTOCOL_IDLE_TIMEOUT, FRAME_ASSEMBLY_TIMEOUT, EVENT_LOOP_LAG_INTERVAL,
            MAX_CONNECTIONS, MAX_CONNECTIONS_PER_SOURCE, MAX_CONNECTION_ATTEMPTS_PER_SECOND,
            MAX_CONNECTION_ATTEMPTS_PER_SOURCE_PER_SECOND, MAX_PROTOCOL_FRAMES_PER_SECOND, POOLED_BUFFERS,
            SSL_CONTEXT, CLIENT_AUTH, TLS_HANDSHAKE_TIMEOUT, MAX_HANDSHAKES,
            MAX_FRAME, MAX_COMPRESSED, MAX_DECOMPRESSED, MAX_COMPRESSION_RATIO, MAX_FRAMES_COMPRESSED,
            MAX_EVENTS_WINDOW, MAX_OUTSTANDING_PER_CONNECTION, MAX_OUTSTANDING_BYTES_PER_CONNECTION,
            ACK_WRITE_TIMEOUT, PROTOCOL_KEEPALIVE_INTERVAL,
            MAX_QUEUED_EVENTS, MAX_QUEUED_BYTES, HIGH_WATER, LOW_WATER,
            EVENT_FILTERING, FILTER_EVALUATION_ERROR_POLICY, DROPPED_EVENT_AUDIT_MODE,
            MAXIMUM_FILTER_RULES, MAXIMUM_UNINDEXED_FILTER_RULES,
            BATCH_STRATEGY, JSON_KEYS, MISSING_KEY, DEFAULT_BUCKET, MAX_KEY_LENGTH, HASH_KEYS, MAX_ACTIVE_KEYS, BATCH_PARTITIONS,
            MAX_EVENTS_BATCH, MAX_BYTES_BATCH, MAX_BATCH_AGE, MAX_BATCH_IDLE, MAX_READY_BATCHES,
            SHUTDOWN_DRAIN_TIMEOUT, METRICS_ATTRIBUTE_DETAIL, METRICS_LOG_INTERVAL);

    private final Object claimResolutionLock = new Object();
    private volatile ClaimManager claimManager;
    private volatile ProcessorMetrics metrics;
    private volatile MemoryTracker memory;
    private volatile ConnectionRegistry registry;
    private volatile BatchCoordinator batches;
    private volatile AckCoordinator acknowledgements;
    private volatile PressureController pressure;
    private volatile BeatsServer server;
    private volatile ScheduledExecutorService maintenance;
    private volatile BatchingStrategy activeBatchingStrategy;
    private volatile Duration shutdownDrainTimeout = Duration.ofSeconds(30);
    private volatile MetricsAttributeDetail activeMetricsAttributeDetail = MetricsAttributeDetail.BASIC;
    private volatile JelDropFilter activeEventFilter = JelDropFilter.disabled();
    private volatile long metricsLogIntervalNanos;
    private volatile long nextMetricsLogNanos;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Pre-ACK JEL drop rule. Rules run in ascending dynamic-property name order; the first matching rule drops the raw event.")
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(org.apache.nifi.expression.ExpressionLanguageScope.NONE)
                .addValidator((subject, input, validationContext) -> {
                    if (subject == null || !subject.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")) {
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                                .explanation("Rule name must start with an alphanumeric character and contain only letters, numbers, dot, dash, or underscore; maximum length is 128")
                                .build();
                    }
                    if (input == null || input.trim().isEmpty()) {
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                                .explanation("JEL expression is required").build();
                    }
                    try {
                        ExpressionCompiler.compile(input);
                        return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
                    } catch (ExpressionCompileException e) {
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                                .explanation(e.getMessage()).build();
                    }
                })
                .build();
    }

    @OnEnabled
    public void enabled(final ConfigurationContext context) throws Exception {
        final String interfaceName = context.getProperty(LOCAL_INTERFACE).evaluateAttributeExpressions().getValue();
        final InetAddress bindAddress = resolveBindAddress(interfaceName);
        final SSLContextProvider provider = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextProvider.class);
        final SSLContext sslContext = provider == null ? null : provider.createContext();

        final EventFilteringMode filteringMode = EventFilteringMode.valueOf(
                context.getProperty(EVENT_FILTERING).getValue());
        final FilterEvaluationErrorPolicy filterErrorPolicy = FilterEvaluationErrorPolicy.valueOf(
                context.getProperty(FILTER_EVALUATION_ERROR_POLICY).getValue());
        final DroppedEventAuditMode droppedEventAuditMode = DroppedEventAuditMode.valueOf(
                context.getProperty(DROPPED_EVENT_AUDIT_MODE).getValue());
        final Map<String, String> configuredFilterRules = configuredFilterRules(context);
        final int maximumFilterRules = context.getProperty(MAXIMUM_FILTER_RULES).asInteger();
        final int maximumUnindexedFilterRules = context.getProperty(MAXIMUM_UNINDEXED_FILTER_RULES).asInteger();
        if (maximumUnindexedFilterRules > maximumFilterRules) {
            throw new IllegalArgumentException("Maximum Unindexed Filter Rules cannot exceed Maximum Filter Rules");
        }
        if (configuredFilterRules.size() > maximumFilterRules) {
            throw new IllegalArgumentException("Configured JEL filter rule count "
                    + configuredFilterRules.size() + " exceeds Maximum Filter Rules " + maximumFilterRules);
        }

        final BatchingStrategy batchingStrategy = BatchingStrategy.valueOf(context.getProperty(BATCH_STRATEGY).getValue());
        final BatchConfig batchConfig = new BatchConfig(
                batchingStrategy,
                parsePointers(context.getProperty(JSON_KEYS).getValue()),
                MissingKeyPolicy.valueOf(context.getProperty(MISSING_KEY).getValue()),
                context.getProperty(DEFAULT_BUCKET).getValue(),
                context.getProperty(MAX_KEY_LENGTH).asInteger(),
                context.getProperty(HASH_KEYS).asBoolean(),
                context.getProperty(MAX_ACTIVE_KEYS).asInteger(),
                context.getProperty(BATCH_PARTITIONS).asInteger(),
                context.getProperty(MAX_EVENTS_BATCH).asInteger(),
                context.getProperty(MAX_BYTES_BATCH).asDataSize(DataUnit.B).longValue(),
                Duration.ofMillis(context.getProperty(MAX_BATCH_AGE).asTimePeriod(TimeUnit.MILLISECONDS)),
                Duration.ofMillis(context.getProperty(MAX_BATCH_IDLE).asTimePeriod(TimeUnit.MILLISECONDS)),
                context.getProperty(MAX_READY_BATCHES).asInteger());

        final ProtocolLimits protocolLimits = new ProtocolLimits(
                intDataSize(context, MAX_FRAME),
                intDataSize(context, MAX_COMPRESSED),
                intDataSize(context, MAX_DECOMPRESSED),
                context.getProperty(MAX_COMPRESSION_RATIO).asInteger(),
                context.getProperty(MAX_FRAMES_COMPRESSED).asInteger());

        final ProcessorMetrics compiledMetrics = new ProcessorMetrics();
        final JelDropFilter compiledEventFilter = JelDropFilter.compile(
                filteringMode,
                filterErrorPolicy,
                droppedEventAuditMode,
                configuredFilterRules,
                batchConfig.effectiveJsonPointers(),
                compiledMetrics);
        if (compiledEventFilter.getUnindexedRuleCount() > maximumUnindexedFilterRules) {
            throw new IllegalArgumentException("Configured JEL unindexed rule count "
                    + compiledEventFilter.getUnindexedRuleCount()
                    + " exceeds Maximum Unindexed Filter Rules " + maximumUnindexedFilterRules
                    + ". Add equality or literal in() anchors, or raise the limit after benchmarking.");
        }

        final boolean streamingJsonParsingEnabled = compiledEventFilter.isEnabled()
                || batchConfig.usesJsonFields();
        validateConfiguration(context, batchConfig, protocolLimits, sslContext, streamingJsonParsingEnabled);
        activeBatchingStrategy = batchingStrategy;
        shutdownDrainTimeout = Duration.ofMillis(context.getProperty(SHUTDOWN_DRAIN_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));
        activeMetricsAttributeDetail = MetricsAttributeDetail.valueOf(context.getProperty(METRICS_ATTRIBUTE_DETAIL).getValue());
        metricsLogIntervalNanos = Duration.ofMillis(context.getProperty(METRICS_LOG_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS)).toNanos();
        nextMetricsLogNanos = System.nanoTime() + metricsLogIntervalNanos;

        final ServerConfig serverConfig = new ServerConfig(
                bindAddress,
                context.getProperty(PORT).asInteger(),
                context.getProperty(WORKER_THREADS).asInteger(),
                context.getProperty(EVENT_PROCESSING_THREADS).asInteger(),
                context.getProperty(EVENT_PROCESSING_QUEUE).asInteger(),
                context.getProperty(EVENT_PROCESSING_QUEUE_HIGH_WATER).asInteger(),
                context.getProperty(EVENT_PROCESSING_QUEUE_LOW_WATER).asInteger(),
                context.getProperty(EVENT_PROCESSING_MAX_BYTES).asDataSize(DataUnit.B).longValue(),
                context.getProperty(CLEANUP_WORKER_THREADS).asInteger(),
                context.getProperty(MAX_CLEANUP_PENDING_TASKS).asInteger(),
                context.getProperty(CLEANUP_QUEUE_HIGH_WATER).asInteger(),
                context.getProperty(CLEANUP_QUEUE_LOW_WATER).asInteger(),
                intDataSize(context, SOCKET_RECEIVE_BUFFER),
                intDataSize(context, RECEIVE_FRAME_BUFFER),
                context.getProperty(LISTEN_BACKLOG).asInteger(),
                context.getProperty(TCP_KEEPALIVE).asBoolean(),
                context.getProperty(POOLED_BUFFERS).asBoolean(),
                Duration.ofMillis(context.getProperty(FIRST_PROTOCOL_BYTE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)),
                Duration.ofMillis(context.getProperty(PROTOCOL_IDLE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)),
                Duration.ofMillis(context.getProperty(FRAME_ASSEMBLY_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)),
                Duration.ofMillis(context.getProperty(EVENT_LOOP_LAG_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS)),
                context.getProperty(MAX_CONNECTIONS).asInteger(),
                context.getProperty(MAX_CONNECTIONS_PER_SOURCE).asInteger(),
                context.getProperty(MAX_CONNECTION_ATTEMPTS_PER_SECOND).asInteger(),
                context.getProperty(MAX_CONNECTION_ATTEMPTS_PER_SOURCE_PER_SECOND).asInteger(),
                context.getProperty(MAX_PROTOCOL_FRAMES_PER_SECOND).asInteger(),
                context.getProperty(MAX_QUEUED_EVENTS).asInteger(),
                context.getProperty(MAX_QUEUED_BYTES).asDataSize(DataUnit.B).longValue(),
                context.getProperty(HIGH_WATER).asInteger(),
                context.getProperty(LOW_WATER).asInteger(),
                context.getProperty(MAX_EVENTS_WINDOW).asInteger(),
                context.getProperty(MAX_OUTSTANDING_PER_CONNECTION).asInteger(),
                context.getProperty(MAX_OUTSTANDING_BYTES_PER_CONNECTION).asDataSize(DataUnit.B).longValue(),
                Duration.ofMillis(context.getProperty(ACK_WRITE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)),
                Duration.ofMillis(context.getProperty(PROTOCOL_KEEPALIVE_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS)),
                protocolLimits,
                batchConfig,
                sslContext,
                ClientAuth.valueOf(context.getProperty(CLIENT_AUTH).getValue()),
                Duration.ofMillis(context.getProperty(TLS_HANDSHAKE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)),
                context.getProperty(MAX_HANDSHAKES).asInteger());

        metrics = compiledMetrics;
        activeEventFilter = compiledEventFilter;
        try {
            if (filteringMode == EventFilteringMode.DROP_MATCHING && configuredFilterRules.isEmpty()) {
                getLogger().warn("Event Filtering is enabled but no dynamic JEL rules are configured; all events will be kept without JSON parsing");
            }
            memory = new MemoryTracker(serverConfig.maximumQueuedEvents(), serverConfig.maximumQueuedBytes(),
                    serverConfig.highWaterPercent(), serverConfig.lowWaterPercent());
            registry = new ConnectionRegistry(
                    serverConfig.maximumConnections(),
                    serverConfig.maximumConnectionsPerSource(),
                    serverConfig.maximumConnectionAttemptsPerSecond(),
                    serverConfig.maximumConnectionAttemptsPerSourcePerSecond(),
                    metrics);
            final ByteBufAllocator batchAllocator = serverConfig.pooledDirectBuffers()
                    ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
            batches = new BatchCoordinator(batchConfig, metrics, batchAllocator);
            pressure = new PressureController(registry, memory, batches, metrics);
            memory.capacityListener(pressure::signal);
            batches.capacityListener(pressure::signal);
            acknowledgements = new AckCoordinator(registry, metrics, pressure,
                    serverConfig.acknowledgementWriteTimeout(), serverConfig.protocolKeepAliveInterval());
            claimManager = new ClaimManager(metrics);
            server = new BeatsServer(serverConfig, registry, batches, memory, metrics, pressure, acknowledgements, activeEventFilter);
            pressure.attachServer(server);
            server.start();
            pressure.signal();

            maintenance = Executors.newSingleThreadScheduledExecutor(runnable -> {
                final Thread thread = new Thread(runnable, "ListenBeats2-maintenance");
                thread.setDaemon(true);
                return thread;
            });
            maintenance.scheduleWithFixedDelay(this::maintenance, 25, 25, TimeUnit.MILLISECONDS);
            getLogger().info("ListenBeats2 service listening on port [{}], event filtering [{}], JEL rules [{}] (indexed [{}], unindexed [{}]), referenced paths [{}], anchor paths [{}]",
                    server.listeningPort(), filteringMode, activeEventFilter.getRuleCount(),
                    activeEventFilter.getIndexedRuleCount(), activeEventFilter.getUnindexedRuleCount(),
                    activeEventFilter.getReferencedPathCount(), activeEventFilter.getAnchorPathCount());
        } catch (Exception failure) {
            cleanupAfterFailedEnable();
            throw failure;
        } catch (Error failure) {
            cleanupAfterFailedEnable();
            throw failure;
        }
    }

    @OnDisabled
    public void onDisabled() {
        final ClaimManager activeClaims = claimManager;
        if (activeClaims != null) {
            try {
                activeClaims.quiesce();
            } catch (Throwable cleanupFailure) {
                getLogger().warn("Failed to quiesce ListenBeats2 claims during disable", cleanupFailure);
            }
        }
        if (server != null) {
            try {
                server.quiesce();
            } catch (Throwable cleanupFailure) {
                getLogger().warn("Failed to quiesce ListenBeats2 server during disable", cleanupFailure);
            }
        }
        if (batches != null) {
            try {
                batches.flushAll("service-disabled");
            } catch (Throwable cleanupFailure) {
                getLogger().warn("Failed to flush all ListenBeats2 batches during disable", cleanupFailure);
            }
        }

        if (activeClaims != null) {
            try {
                if (!activeClaims.awaitEmpty(shutdownDrainTimeout)) {
                    final List<ReadyBatch> abandoned = activeClaims.abandonOutstanding();
                    if (metrics != null) {
                        metrics.claimsAbandoned.add(abandoned.size());
                    }
                    if (batches != null) {
                        for (int index = abandoned.size() - 1; index >= 0; index--) {
                            batches.requeueFirst(abandoned.get(index));
                        }
                    }
                    getLogger().warn("ListenBeats2 shutdown drain timeout expired; requeued [{}] unconfirmed claims without ACK", abandoned.size());
                }
            } catch (Throwable cleanupFailure) {
                getLogger().warn("Failed while draining ListenBeats2 claims during disable", cleanupFailure);
            }
        }

        // Claim commit/rollback finalization and destructive service cleanup are mutually
        // exclusive. This also contains the shutdown-timeout case: a late rollback cannot requeue
        // a claimed batch after the batch coordinator has already been discarded.
        synchronized (claimResolutionLock) {
            shutdownMaintenance(maintenance);
            maintenance = null;
            final BeatsServer activeServer = server;
            if (activeServer != null) {
                try {
                    activeServer.stop();
                } catch (Throwable cleanupFailure) {
                    getLogger().warn("Failed to completely stop ListenBeats2 during disable", cleanupFailure);
                }
            }
            final PressureController activePressure = pressure;
            if (activePressure != null) {
                try {
                    activePressure.close();
                } catch (Throwable cleanupFailure) {
                    getLogger().warn("Failed to close ListenBeats2 pressure controller during disable", cleanupFailure);
                }
            }
            final BatchCoordinator activeBatches = batches;
            final MemoryTracker activeMemory = memory;
            if (activeBatches != null && activeMemory != null) {
                try {
                    activeBatches.discardAll(activeMemory);
                } catch (Throwable cleanupFailure) {
                    getLogger().warn("Failed to discard ListenBeats2 batches during disable", cleanupFailure);
                }
            }
            clearRuntimeReferences();
        }
    }

    private void cleanupAfterFailedEnable() {
        final ScheduledExecutorService activeMaintenance = maintenance;
        maintenance = null;
        shutdownMaintenance(activeMaintenance);
        final BeatsServer activeServer = server;
        if (activeServer != null) {
            try {
                activeServer.stop();
            } catch (Throwable cleanupFailure) {
                getLogger().warn("Failed to completely stop ListenBeats2 after enablement failure", cleanupFailure);
            }
        }
        final PressureController activePressure = pressure;
        if (activePressure != null) {
            try {
                activePressure.close();
            } catch (Throwable cleanupFailure) {
                getLogger().warn("Failed to close pressure controller after enablement failure", cleanupFailure);
            }
        }
        final BatchCoordinator activeBatches = batches;
        final MemoryTracker activeMemory = memory;
        if (activeBatches != null && activeMemory != null) {
            try {
                activeBatches.discardAll(activeMemory);
            } catch (Throwable cleanupFailure) {
                getLogger().warn("Failed to discard batches after enablement failure", cleanupFailure);
            }
        }
        clearRuntimeReferences();
    }

    private void shutdownMaintenance(final ScheduledExecutorService activeMaintenance) {
        if (activeMaintenance == null) {
            return;
        }
        activeMaintenance.shutdownNow();
        try {
            activeMaintenance.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void clearRuntimeReferences() {
        claimManager = null;
        memory = null;
        registry = null;
        batches = null;
        acknowledgements = null;
        pressure = null;
        server = null;
        maintenance = null;
        metrics = null;
        activeBatchingStrategy = null;
        activeEventFilter = JelDropFilter.disabled();
    }

    private void maintenance() {
        try {
            batches.flushExpired();
            server.refreshProcessingExecutorPressure();
            acknowledgements.maintenance();
            pressure.signal();
            logMetricsIfDue();
        } catch (Throwable t) {
            getLogger().warn("ListenBeats2 maintenance failure", t);
        }
    }

    @Override
    public List<BatchClaim> claimBatches(
            final int maximumBatches,
            final int maximumEvents,
            final long maximumBytes) {
        synchronized (claimResolutionLock) {
            final ClaimManager activeClaims = claimManager;
            final BatchCoordinator activeBatches = batches;
            final ProcessorMetrics activeMetrics = metrics;
            if (activeClaims == null || activeBatches == null || activeMetrics == null
                    || !activeClaims.accepting()) {
                return List.of();
            }

            final List<BatchClaim> result = new ArrayList<>();
            int events = 0;
            long bytes = 0;
            while (result.size() < maximumBatches) {
                final ReadyBatch batch = activeBatches.poll();
                if (batch == null) {
                    break;
                }
                if (!result.isEmpty() && (events + batch.eventCount() > maximumEvents
                        || bytes + batch.payloadBytes() > maximumBytes)) {
                    activeBatches.requeueFirst(batch);
                    break;
                }
                if (batch.receipts().oldestReceivedNanos() >= 0) {
                    activeMetrics.recordQueueAge(System.nanoTime() - batch.receipts().oldestReceivedNanos());
                }
                final UUID claimId = activeClaims.claim(batch);
                if (claimId == null) {
                    activeBatches.requeueFirst(batch);
                    break;
                }
                activeMetrics.claimsCreated.increment();
                final Map<String, String> attributes = attributes(batch);
                result.add(new BatchClaim(
                        claimId,
                        List.of(batch.content()),
                        attributes,
                        transitUri(batch),
                        batch.payloadBytes(),
                        batch.eventCount()));
                events += batch.eventCount();
                bytes += batch.payloadBytes();
            }
            return result;
        }
    }

    @Override
    public void commitClaims(final Collection<UUID> claimIds) {
        synchronized (claimResolutionLock) {
            final ClaimManager activeClaims = claimManager;
            final ProcessorMetrics activeMetrics = metrics;
            final AckCoordinator activeAcknowledgements = acknowledgements;
            final MemoryTracker activeMemory = memory;
            final BatchCoordinator activeBatches = batches;
            final PressureController activePressure = pressure;
            if (activeClaims == null || activeMetrics == null || activeMemory == null || activeBatches == null) {
                return;
            }

            final List<ReadyBatch> committed = activeClaims.confirmCommitted(claimIds);
            if (committed.isEmpty()) {
                return;
            }
            try {
                activeMetrics.claimsCommitted.add(committed.size());

                // NiFi has already committed. Finalize every claim independently and never throw back to the processor.
                for (ReadyBatch batch : committed) {
                    if (activeAcknowledgements != null) {
                        try {
                            activeAcknowledgements.committed(batch.receipts());
                        } catch (RuntimeException e) {
                            activeMetrics.claimFinalizationFailures.increment();
                            getLogger().warn("Unable to schedule Beats ACK after committed NiFi transaction", e);
                        }
                    }
                    finalizeCommittedBatch(batch, activeMemory, activeBatches, activeMetrics);
                }
                if (activePressure != null) {
                    try {
                        activePressure.signal();
                    } catch (RuntimeException e) {
                        activeMetrics.claimFinalizationFailures.increment();
                        getLogger().warn("Unable to refresh ListenBeats2 pressure state after committed transaction", e);
                    }
                }
            } finally {
                activeClaims.resolutionCompleted();
            }
        }
    }

    /** Finalization is deliberately non-throwing because the NiFi transaction is already committed. */
    private void finalizeCommittedBatch(
            final ReadyBatch batch,
            final MemoryTracker activeMemory,
            final BatchCoordinator activeBatches,
            final ProcessorMetrics activeMetrics) {
        try {
            batch.content().release();
        } catch (RuntimeException e) {
            activeMetrics.claimFinalizationFailures.increment();
            getLogger().warn("Unable to release committed pooled Beats batch content", e);
        }
        try {
            activeMemory.release(batch.eventCount(), batch.payloadBytes());
            activeMetrics.eventsCommitted.add(batch.eventCount());
            activeMetrics.payloadBytesCommitted.add(batch.payloadBytes());
        } catch (RuntimeException e) {
            activeMetrics.claimFinalizationFailures.increment();
            getLogger().warn("Unable to release committed Beats memory accounting", e);
        }
        try {
            activeBatches.claimCommitted();
        } catch (RuntimeException e) {
            activeMetrics.claimFinalizationFailures.increment();
            getLogger().warn("Unable to release a committed ready-batch slot", e);
        }
    }

    @Override
    public void rollbackClaims(final Collection<UUID> claimIds) {
        synchronized (claimResolutionLock) {
            final ClaimManager activeClaims = claimManager;
            final ProcessorMetrics activeMetrics = metrics;
            final BatchCoordinator activeBatches = batches;
            if (activeClaims == null || activeMetrics == null || activeBatches == null) {
                return;
            }
            final List<ReadyBatch> rollback = activeClaims.confirmRolledBack(claimIds);
            if (rollback.isEmpty()) {
                return;
            }
            try {
                activeMetrics.claimsRolledBack.add(rollback.size());
                for (int index = rollback.size() - 1; index >= 0; index--) {
                    activeBatches.requeueFirst(rollback.get(index));
                }
            } finally {
                activeClaims.resolutionCompleted();
            }
        }
    }

    @Override
    public ListenerSnapshot snapshot() {
        final int claimed = claimManager == null ? 0 : claimManager.size();
        return new ListenerSnapshot(
                server == null ? 0 : server.listeningPort(),
                metrics == null ? 0 : metrics.currentConnections.get(),
                metrics == null ? 0 : metrics.acceptedConnections.sum(),
                metrics == null ? 0 : metrics.rejectedConnections.sum(),
                memory == null ? 0 : memory.events(),
                memory == null ? 0 : memory.bytes(),
                batches == null ? 0 : batches.activeSize(),
                batches == null ? 0 : batches.readySize(),
                claimed,
                metrics == null ? 0 : metrics.framesDecoded.sum(),
                metrics == null ? 0 : metrics.protocolErrors.sum(),
                metrics == null ? 0 : metrics.acknowledgementsSent.sum(),
                metrics == null ? 0 : metrics.acknowledgementFailures.sum(),
                metrics == null ? 0 : metrics.readSuspendedChannels.get(),
                server == null ? 0 : server.processingTasks(),
                server == null ? 0 : server.processingBytes(),
                metrics == null ? 0 : metrics.partialFrameReservedBytes.get(),
                metrics == null ? 0 : metrics.deferredFrames.get(),
                server != null && server.acceptsPaused(),
                memory != null && memory.globalPressure(),
                metrics == null ? 0 : metrics.compressedFrames.sum(),
                metrics == null ? 0 : metrics.compressedBytes.sum(),
                metrics == null ? 0 : metrics.decompressedBytes.sum(),
                metrics == null ? 0 : metrics.eventsAccepted.sum(),
                metrics == null ? 0 : metrics.eventsCommitted.sum(),
                metrics == null ? 0 : metrics.pressureRetryAttempts.sum(),
                metrics == null ? 0 : metrics.claimsAbandoned.sum(),
                metrics == null ? 0 : metrics.claimFinalizationFailures.sum(),
                metrics == null ? 0 : metrics.disconnectedBeforeCommitEvents.sum(),
                metrics == null ? 0 : metrics.disconnectedAfterCommitEvents.sum(),
                metrics == null ? 0 : metrics.queueAgeAverageMillis(),
                metrics == null ? 0 : metrics.queueAgeMaximumMillis(),
                metrics == null ? 0 : metrics.commitLatencyAverageMillis(),
                metrics == null ? 0 : metrics.commitLatencyMaximumMillis(),
                metrics == null ? 0 : metrics.acknowledgementLatencyAverageMillis(),
                metrics == null ? 0 : metrics.acknowledgementLatencyMaximumMillis(),
                metrics == null ? 0 : metrics.queueAgeP50Millis(),
                metrics == null ? 0 : metrics.queueAgeP95Millis(),
                metrics == null ? 0 : metrics.queueAgeP99Millis(),
                metrics == null ? 0 : metrics.commitLatencyP50Millis(),
                metrics == null ? 0 : metrics.commitLatencyP95Millis(),
                metrics == null ? 0 : metrics.commitLatencyP99Millis(),
                metrics == null ? 0 : metrics.acknowledgementLatencyP50Millis(),
                metrics == null ? 0 : metrics.acknowledgementLatencyP95Millis(),
                metrics == null ? 0 : metrics.acknowledgementLatencyP99Millis(),
                metrics == null ? 0 : metrics.eventsAcceptedPerSecond(),
                metrics == null ? 0 : metrics.eventsCommittedPerSecond(),
                metrics == null ? 0 : metrics.payloadBytesAcceptedPerSecond(),
                metrics == null ? 0 : metrics.payloadBytesCommittedPerSecond(),
                metrics == null ? 0 : metrics.keepAlivesSent.sum(),
                metrics == null ? 0 : metrics.keepAliveFailures.sum(),
                metrics == null ? 0 : metrics.acknowledgementWriteTimeouts.sum(),
                metrics == null ? 0 : metrics.pendingAcknowledgementWrites.get(),
                metrics == null ? 0 : metrics.frameAssemblyTimeouts.sum(),
                metrics == null ? 0 : metrics.processingExecutorPendingTasks.get(),
                metrics == null ? 0 : metrics.processingExecutorHottestQueue.get(),
                metrics == null ? 0 : metrics.processingExecutorHottestQueueMaximum.get(),
                metrics == null ? 0 : metrics.processingExecutorRejections.sum(),
                server == null ? 0 : server.cleanupPendingTasks(),
                server == null ? 0 : server.cleanupPeakPendingTasks(),
                metrics == null ? 0 : metrics.cleanupRejections.sum(),
                metrics == null ? 0 : metrics.cleanupEmergencySubmitted.sum(),
                metrics != null && metrics.cleanupPressureActive.get() != 0L,
                metrics == null ? 0 : metrics.tlsConcurrentHandshakes.get(),
                metrics == null ? 0 : metrics.tlsPeakConcurrentHandshakes.get(),
                metrics == null ? 0 : metrics.tlsHandshakeTimeouts.sum(),
                metrics == null ? 0 : metrics.tlsHandshakeLatencyP50Millis(),
                metrics == null ? 0 : metrics.tlsHandshakeLatencyP95Millis(),
                metrics == null ? 0 : metrics.tlsHandshakeLatencyP99Millis(),
                metrics == null ? 0 : metrics.firstByteTimeouts.sum(),
                metrics == null ? 0 : metrics.idleConnectionCloses.sum(),
                metrics == null ? 0 : metrics.eventLoopLagCurrentMillis(),
                metrics == null ? 0 : metrics.eventLoopLagMaximumMillis(),
                metrics == null ? 0 : metrics.eventLoopLagOver50Millis.sum(),
                metrics == null ? 0 : metrics.eventLoopLagOver100Millis.sum(),
                metrics == null ? 0 : metrics.eventLoopLagOver1000Millis.sum(),
                activeEventFilter == null ? EventFilteringMode.DISABLED.name() : activeEventFilter.mode().name(),
                activeEventFilter == null ? 0 : activeEventFilter.getRuleCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getIndexedRuleCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getUnindexedRuleCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getReferencedPathCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getAnchorPathCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getBatchPathCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getEvaluatedEventCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getCandidateRulesSelectedCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getRuleEvaluationCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getJsonPassCount(),
                activeEventFilter == null ? 0 : activeEventFilter.getExtractedValueCount(),
                activeEventFilter == null ? 0.0d : activeEventFilter.getAverageCandidateRulesPerEvent(),
                activeEventFilter == null ? 0.0d : activeEventFilter.getAverageRuleEvaluationsPerEvent(),
                activeEventFilter == null ? 0.0d : activeEventFilter.getAverageJsonPassesPerEvent(),
                activeEventFilter == null ? 0.0d : activeEventFilter.getAverageExtractedValuesPerEvent(),
                metrics == null ? 0 : metrics.filterEventsInput.sum(),
                metrics == null ? 0 : metrics.filterEventsKept.sum(),
                metrics == null ? 0 : metrics.filterEventsDropped.sum(),
                metrics == null ? 0 : metrics.filterBytesDropped.sum(),
                metrics == null ? 0 : metrics.filterEvaluationErrors.sum(),
                activeEventFilter == null ? Map.of() : activeEventFilter.snapshotRuleMatches(),
                metrics == null ? Map.of() : metrics.connectionCloseReasonSnapshot());
    }

    private void logMetricsIfDue() {
        if (metricsLogIntervalNanos <= 0) {
            return;
        }
        final long now = System.nanoTime();
        if (now < nextMetricsLogNanos) {
            return;
        }
        nextMetricsLogNanos = now + metricsLogIntervalNanos;
        getLogger().info(
                "ListenBeats2 metrics: connections={}, accepted={}, rejected={}, frames={}, events.accepted={}, events.committed={}, "
                        + "queued.events={}, queued.bytes={}, active.batches={}, ready.batches={}, claimed={}, "
                        + "processing.tasks={}, processing.bytes={}, processing.queue.pending={}, processing.queue.hottest={}, "
                        + "cleanup.pending={}, cleanup.peak={}, cleanup.pressure={}, cleanup.rejections={}, "
                        + "partial.bytes={}, suspended={}, accepts.paused={}, acks={}, ack.failures={}, ack.timeouts={}, "
                        + "protocol.errors={}, pressure.global={}, tls.current={}, tls.peak={}, tls.timeouts={}, "
                        + "event.loop.lag.current.ms={}, event.loop.lag.max.ms={}, first.byte.timeouts={}, idle.timeouts={}, "
                        + "filter.mode={}, filter.rules={}, filter.input={}, filter.kept={}, filter.dropped={}, filter.dropped.bytes={}, filter.errors={}, "
                        + "latency.queue.avg.ms={}, latency.queue.max.ms={}, latency.commit.avg.ms={}, latency.commit.max.ms={}, "
                        + "latency.ack.avg.ms={}, latency.ack.max.ms={}",
                metrics.currentConnections.get(), metrics.acceptedConnections.sum(), metrics.rejectedConnections.sum(),
                metrics.framesDecoded.sum(), metrics.eventsAccepted.sum(), metrics.eventsCommitted.sum(),
                memory.events(), memory.bytes(), batches.activeSize(), batches.readySize(),
                claimManager == null ? 0 : claimManager.size(), server.processingTasks(), server.processingBytes(),
                metrics.processingExecutorPendingTasks.get(), metrics.processingExecutorHottestQueue.get(),
                server.cleanupPendingTasks(), server.cleanupPeakPendingTasks(),
                metrics.cleanupPressureActive.get() != 0L, metrics.cleanupRejections.sum(),
                metrics.partialFrameReservedBytes.get(), metrics.readSuspendedChannels.get(), server.acceptsPaused(),
                metrics.acknowledgementsSent.sum(), metrics.acknowledgementFailures.sum(),
                metrics.acknowledgementWriteTimeouts.sum(), metrics.protocolErrors.sum(), memory.globalPressure(),
                metrics.tlsConcurrentHandshakes.get(), metrics.tlsPeakConcurrentHandshakes.get(),
                metrics.tlsHandshakeTimeouts.sum(), metrics.eventLoopLagCurrentMillis(),
                metrics.eventLoopLagMaximumMillis(), metrics.firstByteTimeouts.sum(), metrics.idleConnectionCloses.sum(),
                activeEventFilter.mode(), activeEventFilter.getRuleCount(), metrics.filterEventsInput.sum(),
                metrics.filterEventsKept.sum(), metrics.filterEventsDropped.sum(), metrics.filterBytesDropped.sum(),
                metrics.filterEvaluationErrors.sum(),
                metrics.queueAgeAverageMillis(), metrics.queueAgeMaximumMillis(),
                metrics.commitLatencyAverageMillis(), metrics.commitLatencyMaximumMillis(),
                metrics.acknowledgementLatencyAverageMillis(), metrics.acknowledgementLatencyMaximumMillis());
    }

    private Map<String, String> attributes(final ReadyBatch batch) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("mime.type", "application/x-ndjson");
        attributes.put("beats.protocol.version", "2");
        attributes.put("beats.event.count", Integer.toString(batch.eventCount()));
        attributes.put("beats.uncompressed.bytes", Long.toString(batch.payloadBytes()));
        attributes.put("beats.encoded.bytes", Long.toString(batch.encodedBytes()));
        attributes.put("beats.batch.strategy", activeBatchingStrategy == null ? "UNKNOWN" : activeBatchingStrategy.name());
        attributes.put("beats.batch.key", batch.key().value());
        attributes.put("beats.batch.flush.reason", batch.flushReason());
        attributes.put("beats.ack.policy", "AFTER_SESSION_COMMIT");
        attributes.put("record.count", Integer.toString(batch.eventCount()));
        attributes.put("beats.listener.port", Integer.toString(server.listeningPort()));

        // Listener-wide metrics are optional because each additional FlowFile attribute is persisted
        // by the FlowFile Repository. BASIC is the production default; FULL is for qualification.
        if (activeMetricsAttributeDetail != MetricsAttributeDetail.NONE) {
            final ListenerSnapshot listener = snapshot();
            attributes.put("beats.active.connections", Long.toString(listener.currentConnections()));
            attributes.put("beats.pending.records", Long.toString(listener.queuedEvents()));
            attributes.put("beats.pending.bytes", Long.toString(listener.queuedBytes()));
            attributes.put("beats.pressure.paused", Boolean.toString(listener.globalPressure()));
            attributes.put("beats.ack.failures", Long.toString(listener.acknowledgementFailures()));
            attributes.put("beats.queue.age.avg.ms", Long.toString(listener.queueAgeAverageMillis()));
            attributes.put("beats.queue.age.max.ms", Long.toString(listener.queueAgeMaximumMillis()));
            attributes.put("beats.commit.latency.avg.ms", Long.toString(listener.commitLatencyAverageMillis()));
            attributes.put("beats.commit.latency.max.ms", Long.toString(listener.commitLatencyMaximumMillis()));
            attributes.put("beats.ack.latency.avg.ms", Long.toString(listener.acknowledgementLatencyAverageMillis()));
            attributes.put("beats.ack.latency.max.ms", Long.toString(listener.acknowledgementLatencyMaximumMillis()));
            attributes.put("beats.disconnected.before.commit", Long.toString(listener.disconnectedBeforeCommitEvents()));
            attributes.put("beats.disconnected.after.commit", Long.toString(listener.disconnectedAfterCommitEvents()));
            attributes.put("beats.filter.mode", listener.eventFilteringMode());
            attributes.put("beats.filter.rules.configured", Long.toString(listener.configuredFilterRules()));
            attributes.put("beats.filter.rules.indexed", Long.toString(listener.indexedFilterRules()));
            attributes.put("beats.filter.rules.unindexed", Long.toString(listener.unindexedFilterRules()));
            attributes.put("beats.filter.paths.referenced", Long.toString(listener.filterReferencedPaths()));
            attributes.put("beats.filter.paths.anchors", Long.toString(listener.filterAnchorPaths()));
            attributes.put("beats.filter.paths.batch", Long.toString(listener.filterBatchPaths()));
            attributes.put("beats.filter.candidate.rules.avg", Double.toString(listener.filterCandidateRulesAverage()));
            attributes.put("beats.filter.rules.evaluated.avg", Double.toString(listener.filterRuleEvaluationsAverage()));
            attributes.put("beats.filter.json.passes.avg", Double.toString(listener.filterJsonPassesAverage()));
            attributes.put("beats.filter.values.extracted.avg", Double.toString(listener.filterValuesExtractedAverage()));
            attributes.put("beats.filter.events.input", Long.toString(listener.filterEventsInput()));
            attributes.put("beats.filter.events.kept", Long.toString(listener.filterEventsKept()));
            attributes.put("beats.filter.events.dropped", Long.toString(listener.filterEventsDropped()));
            attributes.put("beats.filter.bytes.dropped", Long.toString(listener.filterBytesDropped()));
            attributes.put("beats.filter.evaluation.errors", Long.toString(listener.filterEvaluationErrors()));

            if (activeMetricsAttributeDetail == MetricsAttributeDetail.FULL) {
                attributes.put("beats.active.batches", Long.toString(listener.activeBatches()));
                attributes.put("beats.ready.batches", Long.toString(listener.readyBatches()));
                attributes.put("beats.claimed.batches", Long.toString(listener.claimedBatches()));
                attributes.put("beats.processing.tasks", Long.toString(listener.processingTasks()));
                attributes.put("beats.processing.bytes", Long.toString(listener.processingBytes()));
                attributes.put("beats.partial.frame.reserved.bytes", Long.toString(listener.partialFrameReservedBytes()));
                attributes.put("beats.deferred.frames", Long.toString(listener.deferredFrames()));
                attributes.put("beats.accept.paused", Boolean.toString(listener.acceptsPaused()));
                attributes.put("beats.ack.sent", Long.toString(listener.acknowledgementsSent()));
                attributes.put("beats.queue.age.p50.ms", Long.toString(listener.queueAgeP50Millis()));
                attributes.put("beats.queue.age.p95.ms", Long.toString(listener.queueAgeP95Millis()));
                attributes.put("beats.queue.age.p99.ms", Long.toString(listener.queueAgeP99Millis()));
                attributes.put("beats.commit.latency.p50.ms", Long.toString(listener.commitLatencyP50Millis()));
                attributes.put("beats.commit.latency.p95.ms", Long.toString(listener.commitLatencyP95Millis()));
                attributes.put("beats.commit.latency.p99.ms", Long.toString(listener.commitLatencyP99Millis()));
                attributes.put("beats.ack.latency.p50.ms", Long.toString(listener.acknowledgementLatencyP50Millis()));
                attributes.put("beats.ack.latency.p95.ms", Long.toString(listener.acknowledgementLatencyP95Millis()));
                attributes.put("beats.ack.latency.p99.ms", Long.toString(listener.acknowledgementLatencyP99Millis()));
                attributes.put("beats.events.accepted.per.second", Long.toString(listener.eventsAcceptedPerSecond()));
                attributes.put("beats.events.committed.per.second", Long.toString(listener.eventsCommittedPerSecond()));
                attributes.put("beats.payload.accepted.bytes.per.second", Long.toString(listener.payloadBytesAcceptedPerSecond()));
                attributes.put("beats.payload.committed.bytes.per.second", Long.toString(listener.payloadBytesCommittedPerSecond()));
                attributes.put("beats.keepalive.sent", Long.toString(listener.keepAlivesSent()));
                attributes.put("beats.keepalive.failures", Long.toString(listener.keepAliveFailures()));
                attributes.put("beats.ack.write.timeouts", Long.toString(listener.acknowledgementWriteTimeouts()));
                attributes.put("beats.ack.write.pending", Long.toString(listener.pendingAcknowledgementWrites()));
                attributes.put("beats.frame.assembly.timeouts", Long.toString(listener.frameAssemblyTimeouts()));
                attributes.put("beats.processing.executor.pending.tasks", Long.toString(listener.processingExecutorPendingTasks()));
                attributes.put("beats.processing.executor.hottest.queue", Long.toString(listener.processingExecutorHottestQueue()));
                attributes.put("beats.processing.executor.rejections", Long.toString(listener.processingExecutorRejections()));
                attributes.put("beats.cleanup.pending.tasks", Long.toString(listener.cleanupPendingTasks()));
                attributes.put("beats.cleanup.pending.tasks.max", Long.toString(listener.cleanupPeakPendingTasks()));
                attributes.put("beats.cleanup.rejections", Long.toString(listener.cleanupRejections()));
                attributes.put("beats.cleanup.emergency.submitted", Long.toString(listener.cleanupEmergencySubmitted()));
                attributes.put("beats.cleanup.pressure.active", Boolean.toString(listener.cleanupPressure()));
                attributes.put("beats.tls.handshakes.current", Long.toString(listener.tlsConcurrentHandshakes()));
                attributes.put("beats.tls.handshakes.peak", Long.toString(listener.tlsPeakConcurrentHandshakes()));
                attributes.put("beats.tls.handshake.timeouts", Long.toString(listener.tlsHandshakeTimeouts()));
                attributes.put("beats.tls.handshake.latency.p50.ms", Long.toString(listener.tlsHandshakeLatencyP50Millis()));
                attributes.put("beats.tls.handshake.latency.p95.ms", Long.toString(listener.tlsHandshakeLatencyP95Millis()));
                attributes.put("beats.tls.handshake.latency.p99.ms", Long.toString(listener.tlsHandshakeLatencyP99Millis()));
                attributes.put("beats.first.byte.timeouts", Long.toString(listener.firstByteTimeouts()));
                attributes.put("beats.idle.connection.timeouts", Long.toString(listener.idleConnectionTimeouts()));
                attributes.put("beats.event.loop.lag.current.ms", Long.toString(listener.eventLoopLagCurrentMillis()));
                attributes.put("beats.event.loop.lag.max.ms", Long.toString(listener.eventLoopLagMaximumMillis()));
                attributes.put("beats.event.loop.lag.over.50ms", Long.toString(listener.eventLoopLagOver50Millis()));
                attributes.put("beats.event.loop.lag.over.100ms", Long.toString(listener.eventLoopLagOver100Millis()));
                attributes.put("beats.event.loop.lag.over.1000ms", Long.toString(listener.eventLoopLagOver1000Millis()));
                attributes.put("beats.filter.events.evaluated", Long.toString(listener.filterEventsEvaluated()));
                attributes.put("beats.filter.candidate.rules.selected", Long.toString(listener.filterCandidateRulesSelected()));
                attributes.put("beats.filter.rule.evaluations", Long.toString(listener.filterRuleEvaluations()));
                attributes.put("beats.filter.json.passes", Long.toString(listener.filterJsonPasses()));
                attributes.put("beats.filter.values.extracted", Long.toString(listener.filterValuesExtracted()));
                for (Map.Entry<String, Long> rule : listener.filterRuleMatches().entrySet()) {
                    attributes.put("beats.filter.rule." + sanitizeAttributeSegment(rule.getKey()) + ".matches",
                            Long.toString(rule.getValue()));
                }
                for (Map.Entry<String, Long> closeReason : listener.connectionCloseReasons().entrySet()) {
                    if (closeReason.getValue() != 0L) {
                        attributes.put("beats.connections.closed.reason." + closeReason.getKey(),
                                Long.toString(closeReason.getValue()));
                    }
                }
            }
        }

        final var receipts = batch.receipts();
        if (receipts.singleSender()) {
            attributes.put("beats.sender", receipts.commonRemoteAddress());
        }
        if (receipts.singleConnection()) {
            attributes.put("beats.sender.port", Integer.toString(receipts.commonRemotePort()));
            if (receipts.singleWindow()) {
                attributes.put("beats.window.id", Long.toUnsignedString(receipts.commonWindowId()));
                attributes.put("beats.sequence.first", Long.toUnsignedString(receipts.firstSequence()));
                attributes.put("beats.sequence.last", Long.toUnsignedString(receipts.lastSequence()));
            }
            if (receipts.commonTlsSubject() != null) {
                attributes.put("beats.tls.client.subject", receipts.commonTlsSubject());
                if (receipts.commonTlsIssuer() != null) {
                    attributes.put("beats.tls.client.issuer", receipts.commonTlsIssuer());
                }
            }
        }
        return Map.copyOf(attributes);
    }

    private String transitUri(final ReadyBatch batch) {
        final String sender = batch.receipts().singleSender()
                ? batch.receipts().commonRemoteAddress()
                : "multiple";
        return "beats://" + sender + ':' + server.listeningPort();
    }

    private static int intDataSize(final ConfigurationContext context, final PropertyDescriptor descriptor) {
        final long value = context.getProperty(descriptor).asDataSize(DataUnit.B).longValue();
        if (value < 1 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(descriptor.getDisplayName() + " must be between 1 byte and " + Integer.MAX_VALUE + " bytes");
        }
        return (int) value;
    }

    private static void validateConfiguration(
            final ConfigurationContext context,
            final BatchConfig batchConfig,
            final ProtocolLimits protocolLimits,
            final SSLContext sslContext,
            final boolean streamingJsonParsingEnabled) {

        final long receiveFrameBuffer = context.getProperty(RECEIVE_FRAME_BUFFER).asDataSize(DataUnit.B).longValue();
        if (receiveFrameBuffer < 1024L || receiveFrameBuffer > 1024L * 1024L) {
            throw new IllegalArgumentException("Receive Frame Buffer must be between 1 KB and 1 MB");
        }
        final int highWater = context.getProperty(HIGH_WATER).asInteger();
        final int lowWater = context.getProperty(LOW_WATER).asInteger();
        if (highWater < 1 || highWater > 100) {
            throw new IllegalArgumentException("Queue High-Water Mark must be between 1 and 100");
        }
        if (lowWater < 0 || lowWater >= highWater) {
            throw new IllegalArgumentException("Queue Low-Water Mark must be at least 0 and lower than the high-water mark");
        }
        if (protocolLimits.maximumCompressedBytes() > protocolLimits.maximumDecompressedBytes()) {
            throw new IllegalArgumentException("Maximum Compressed Frame Size cannot exceed Maximum Decompressed Size");
        }
        final int jsonWorkingSetMultiplier = streamingJsonParsingEnabled
                ? com.dds.nifi.beats.protocol.BeatsFrameDecoder.STREAMING_JSON_WORKING_SET_MULTIPLIER
                : com.dds.nifi.beats.protocol.BeatsFrameDecoder.RAW_JSON_WORKING_SET_MULTIPLIER;
        final long jsonWorkingSet = ((long) protocolLimits.maximumFrameBytes() * jsonWorkingSetMultiplier)
                + (64L * 1024L);
        final long compressedWorkingSet = ((long) protocolLimits.maximumCompressedBytes() * 2L)
                + Math.min((long) protocolLimits.maximumDecompressedBytes() * 2L,
                ((long) protocolLimits.maximumFrameBytes() * jsonWorkingSetMultiplier) + (128L * 1024L));
        final long minimumProcessingBytes = Math.max(jsonWorkingSet, compressedWorkingSet);
        final int processingHigh = context.getProperty(EVENT_PROCESSING_QUEUE_HIGH_WATER).asInteger();
        final int processingLow = context.getProperty(EVENT_PROCESSING_QUEUE_LOW_WATER).asInteger();
        if (processingHigh > 100 || processingLow > 100 || processingLow >= processingHigh) {
            throw new IllegalArgumentException("Event Processing Queue Low-Water Mark must be below High-Water Mark and both must be at most 100");
        }
        final int cleanupHigh = context.getProperty(CLEANUP_QUEUE_HIGH_WATER).asInteger();
        final int cleanupLow = context.getProperty(CLEANUP_QUEUE_LOW_WATER).asInteger();
        if (cleanupHigh >= 100 || cleanupLow >= cleanupHigh) {
            throw new IllegalArgumentException("Cleanup Queue Low-Water Mark must be below High-Water Mark and High-Water must be below 100");
        }
        if (context.getProperty(MAX_CLEANUP_PENDING_TASKS).asInteger()
                < context.getProperty(MAX_CONNECTIONS).asInteger()) {
            throw new IllegalArgumentException("Maximum Connection Cleanup Pending Tasks must be at least Maximum Connections");
        }
        if (context.getProperty(FIRST_PROTOCOL_BYTE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS) <= 0) {
            throw new IllegalArgumentException("First Protocol Byte Timeout must be positive");
        }
        if (context.getProperty(PROTOCOL_IDLE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS) < 0) {
            throw new IllegalArgumentException("Protocol Idle Timeout cannot be negative");
        }
        if (context.getProperty(EVENT_LOOP_LAG_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS) <= 0) {
            throw new IllegalArgumentException("Event Loop Lag Probe Interval must be positive");
        }
        if (context.getProperty(FRAME_ASSEMBLY_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS) <= 0) {
            throw new IllegalArgumentException("Frame Assembly Timeout must be positive");
        }
        if (context.getProperty(ACK_WRITE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS) <= 0) {
            throw new IllegalArgumentException("ACK Write Timeout must be positive");
        }
        if (context.getProperty(PROTOCOL_KEEPALIVE_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS) < 0) {
            throw new IllegalArgumentException("Protocol Keepalive Interval cannot be negative");
        }

        if (context.getProperty(EVENT_PROCESSING_MAX_BYTES).asDataSize(DataUnit.B).longValue() < minimumProcessingBytes) {
            throw new IllegalArgumentException("Maximum Event Processing Bytes must be at least "
                    + minimumProcessingBytes + " bytes for one maximum frame working set");
        }
        if (context.getProperty(MAX_OUTSTANDING_PER_CONNECTION).asInteger()
                < context.getProperty(MAX_EVENTS_WINDOW).asInteger()) {
            throw new IllegalArgumentException("Maximum Outstanding Events per Connection cannot be lower than Maximum Events per Window");
        }
        if (context.getProperty(MAX_OUTSTANDING_BYTES_PER_CONNECTION).asDataSize(DataUnit.B).longValue()
                < protocolLimits.maximumFrameBytes()) {
            throw new IllegalArgumentException("Maximum Outstanding Bytes per Connection cannot be lower than Maximum Frame Size");
        }
        if (batchConfig.maximumBytes() > context.getProperty(MAX_QUEUED_BYTES).asDataSize(DataUnit.B).longValue()) {
            throw new IllegalArgumentException("Maximum Bytes per Batch cannot exceed Maximum Queued Bytes");
        }
        if (batchConfig.maximumBytes() + batchConfig.maximumEvents() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Maximum Bytes per Batch plus NDJSON delimiters must fit in a Java buffer length");
        }
        if (batchConfig.maximumReadyBatches() > context.getProperty(MAX_QUEUED_EVENTS).asInteger()) {
            throw new IllegalArgumentException("Maximum Ready Batches cannot exceed Maximum Queued Events");
        }
        if (batchConfig.strategy() == BatchingStrategy.JSON_KV && batchConfig.jsonPointers().isEmpty()) {
            throw new IllegalArgumentException("JSON Batch Key is required for JSON_KV batching");
        }
        final ClientAuth clientAuth = ClientAuth.valueOf(context.getProperty(CLIENT_AUTH).getValue());
        if (sslContext == null && clientAuth != ClientAuth.NONE) {
            throw new IllegalArgumentException("Client Authentication requires an SSL Context Service");
        }
    }

    private static InetAddress resolveBindAddress(final String configured) throws Exception {
        if (configured == null || configured.isBlank()) {
            return InetAddress.getByName("0.0.0.0");
        }
        final NetworkInterface networkInterface = NetworkInterface.getByName(configured);
        if (networkInterface != null) {
            final java.util.Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
            if (addresses.hasMoreElements()) {
                return addresses.nextElement();
            }
            throw new SocketException("Network interface has no addresses: " + configured);
        }
        return InetAddress.getByName(configured);
    }

    private static String sanitizeAttributeSegment(final String value) {
        if (value == null || value.isBlank()) {
            return "unnamed";
        }
        final String sanitized = value.replaceAll("[^A-Za-z0-9_.-]", "_");
        return sanitized.length() <= 128 ? sanitized : sanitized.substring(0, 128);
    }

    private static Map<String, String> configuredFilterRules(final ConfigurationContext context) {
        final Map<String, String> configured = new TreeMap<>();
        for (Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            final String expression = entry.getValue();
            if (descriptor != null && descriptor.isDynamic()
                    && expression != null && !expression.trim().isEmpty()) {
                configured.put(descriptor.getName(), expression);
            }
        }
        return java.util.Collections.unmodifiableMap(new TreeMap<>(configured));
    }

    private static List<String> parsePointers(final String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return java.util.Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(pointer -> !pointer.isEmpty())
                .toList();
    }
}
