# ListenBeats2 for Apache NiFi 2.11

ListenBeats2 is an independent, high-throughput Apache NiFi extension that receives **Beats/Lumberjack v2** traffic directly from Filebeat, Winlogbeat, Metricbeat, and compatible senders and emits **NDJSON FlowFiles**.

The project is designed for large connection counts and sustained ingestion where delivery correctness, bounded memory, explicit backpressure, and predictable ACK behavior matter more than convenience shortcuts.

> **Compatibility target:** Apache NiFi **2.11.0**, Java **21**, extension version **1.0.0**.
>
> This is a community project. It is not an Apache Software Foundation project and is not affiliated with or endorsed by Apache or Elastic.

## Why this exists

A conventional Beats deployment often inserts Logstash or another TCP service in front of NiFi. ListenBeats2 lets NiFi own the Lumberjack v2 connection directly while preserving the important remote-delivery boundary:

**a sender ACK is eligible only after the corresponding NiFi transaction commits.**

That gives the listener **at-least-once delivery semantics**. If NiFi commits but the network connection disappears before the sender receives the ACK, the sender can retransmit and produce a duplicate; the design intentionally prefers possible duplication over silent data loss.

## Components

The bundle contains:

- `StandardBeatsListenerService` — Controller Service that owns the Netty listener, protocol state, filtering, batching, pressure control, TLS, metrics, and ACK coordination.
- `ListenBeats2` — source processor that claims committed-ready batches, writes NDJSON FlowFiles, commits the NiFi session, and only then finalizes claims so cumulative Beats ACKs can advance.
- `nifi-listen-beats2-api` — small service/claim API shared by the processor and Controller Service.
- `nifi-listen-beats2-nar` — installable NiFi NAR.

## Core properties of the design

### Post-commit ACKs

`ListenBeats2` deliberately does **not** use `@SupportsBatching`. It creates and transfers FlowFiles, performs an explicit `ProcessSession.commit()`, then calls the listener service to finalize the claims. A pre-commit failure rolls the claims back to the ready queue without acknowledging the sender.

### Bounded memory

The listener applies independent limits to:

- accepted event count and bytes,
- temporary event-processing working bytes,
- per-connection outstanding events and bytes,
- processing executor queues,
- active batch keys,
- ready batches,
- compressed input size and inflation,
- pending windows,
- connection counts and connection-attempt rates.

Backpressure ultimately reaches the socket by disabling Netty `AUTO_READ` for affected connections.

### Ordered Lumberjack windows

Window sequence tracking supports arbitrary first sequence numbers and unsigned 32-bit wraparound. Commits can complete out of processing order, but cumulative ACK advancement remains ordered at the connection/window boundary.

### Streaming compressed-envelope handling

Compressed frames are inflated incrementally with limits on compressed bytes, decompressed bytes, compression ratio, and nested frame count. The implementation avoids retaining an entire decompressed envelope as one giant byte array.

### Optional pre-ACK JEL filtering

Dynamic Controller Service properties can define JEL drop rules. Candidate indexing uses safe equality/literal anchors so selective rules avoid evaluating every expression for every event. Filtering happens before accepted events enter NiFi, so a matching drop is intentionally ACK-eligible without creating a FlowFile.

See [JEL filtering](docs/JEL_FILTERING.md).

## Supported wire protocol

The listener currently supports Lumberjack/Beats protocol version 2 frames:

- `W` — window frame
- `J` — JSON event frame
- `C` — zlib-compressed envelope containing supported nested v2 frames

Unsupported versions or frame types are rejected as protocol errors.

## Build

Requirements:

- JDK 21
- Maven 3.9.16 or newer (required by the Apache NiFi 2.11.0 parent build)
- network access to Maven Central / Apache artifact repositories on the first build

Build and run the full test suite:

```bash
mvn -B -T1C clean verify
```

or:

```bash
./build.sh
```

The installable artifact is produced at:

```text
nifi-listen-beats2-nar/target/nifi-listen-beats2-nar-1.0.0.nar
```

### Netty leak-detection qualification

For focused ownership tests, run the services tests with paranoid Netty leak detection:

```bash
MAVEN_OPTS="-Dio.netty.leakDetection.level=paranoid" \
  mvn -B -pl nifi-listen-beats2-services -am test
```

Do not use paranoid leak detection as a normal production setting.

## Install in NiFi 2.11

1. Stop NiFi on the target node.
2. Copy `nifi-listen-beats2-nar-1.0.0.nar` into the NiFi NAR extension location used by your installation.
3. Start NiFi.
4. Add a `StandardBeatsListenerService` Controller Service.
5. Configure the listening port, connection/memory limits, batching, TLS, and optional JEL rules.
6. Enable the Controller Service.
7. Add a `ListenBeats2` processor and select that service.
8. Connect `success` to the downstream flow and start the processor.

For a clustered NiFi deployment, each node that runs `ListenBeats2` needs the NAR installed. How Beats clients are distributed across nodes is an external load-balancing/DNS decision; connection state is node-local.

## Minimal starting configuration

A safe starting point depends heavily on heap size, direct-memory allowance, CPU count, event size, batching strategy, TLS, and expected client count. Do not copy large-scale values blindly.

At minimum review these Controller Service groups before enabling production traffic:

- **Network:** port, local interface, worker threads, receive buffer, backlog, connection limits.
- **Processing:** processing threads, processing queue, maximum processing bytes.
- **Protocol:** frame/window limits, per-connection outstanding limits, ACK timeout.
- **Memory:** maximum queued events/bytes and pressure watermarks.
- **Batching:** strategy, batch events/bytes/age/idle, ready batches, active keys/partitions.
- **TLS:** SSL Context Service, client authentication, handshake timeout/concurrency.
- **Filtering:** filtering mode, error policy, rule limits, dynamic JEL rules.

A documented high-throughput example profile is available in [configuration guidance](docs/CONFIGURATION.md).

## Processor scheduling

For high-volume ingestion, a common starting profile for `ListenBeats2` is:

```text
Scheduling Strategy: Timer driven
Run Schedule:        0 sec
Concurrent Tasks:    4-8 per node
Execution:           All Nodes
```

The correct concurrency is a throughput/CPU/repository decision. Increasing processor tasks cannot compensate for an undersized Content Repository, FlowFile Repository, disk subsystem, or downstream backpressure.

## FlowFile output

Each successful FlowFile contains NDJSON and receives listener metadata attributes. Depending on configured detail, attributes include batch counts/bytes, batching key/reason, listener metrics, and connection/filter information.

The raw event JSON is copied into the batch content unchanged except for the NDJSON newline delimiter. ListenBeats2 is not an ECS transformer.

## TLS

TLS is optional and uses NiFi's `SSLContextProvider`. Client authentication can be configured when an SSL Context Service is selected. The first protocol-byte timeout begins only after a successful TLS handshake.

For Internet-exposed listeners, strongly prefer TLS and place network-level rate controls/firewalling in front of NiFi in addition to the listener's own admission limits.

## Delivery and duplicate behavior

The guarantee is intentionally **at least once**, not exactly once.

A duplicate can occur when:

1. NiFi successfully commits a FlowFile transaction.
2. The connection closes before the Beats client receives the corresponding ACK.
3. The client reconnects and retransmits.

Downstream consumers that require uniqueness should use a stable event identity appropriate for the data source (for example, an agent/source identity plus an event record identifier).

## Operational metrics

The Controller Service snapshot exposes counters for connection admission/rejection, protocol frames, event acceptance/filtering, batching, pressure transitions, claims, ACKs, compression, processing queues, cleanup queues, and related failure paths.

The `Metrics Log Interval` property can emit periodic listener snapshots. Use this together with NiFi/JVM metrics, Netty direct-memory observation, GC telemetry, OS socket metrics, and downstream repository/backpressure telemetry.

## Production qualification

Before using this at large connection counts, perform workload-specific qualification. Recommended scenarios include:

- plaintext and TLS/mTLS,
- Filebeat/Winlogbeat/Elastic Agent versions you actually deploy,
- compressed and uncompressed publishers,
- idle and high-rate connections,
- reconnect storms,
- repository/downstream slowdown and recovery,
- processor stop/start while clients remain connected,
- Controller Service disable/re-enable,
- commit-success/ACK-loss duplicate scenarios,
- malformed/truncated/oversized frames,
- long-duration connection churn and 24-72 hour soak tests.

See [performance qualification](docs/PERFORMANCE-QUALIFICATION.md).

## Development status

Version 1.0.0 contains the corrective hardening pass that addressed decoder metadata preservation, pressure-state retention, exception-safe startup/shutdown, bounded JEL container materialization, hard batch-byte behavior, cross-shard batch eviction fairness, JSON validation, deferred retry accounting, TLS timing, rate protection, and keepalive ACK safety.


## Security

Please do not publish suspected vulnerabilities as public issues before maintainers have had a reasonable opportunity to assess them. See [SECURITY.md](SECURITY.md).

## Contributing

Issues, reproducible performance results, protocol interoperability tests, documentation, and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under the [Apache License 2.0](LICENSE).

Apache, Apache NiFi, NiFi, and related marks are trademarks of The Apache Software Foundation. Elastic, Filebeat, Winlogbeat, Metricbeat, Elastic Agent, and related marks are the property of their respective owners. Use of names in this project describes interoperability only.
