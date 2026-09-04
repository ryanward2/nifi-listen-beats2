# Contributing

Contributions are welcome.

## Before opening a pull request

1. Use JDK 21.
2. Build against Apache NiFi 2.11.0.
3. Run `mvn -B -T1C clean verify`.
4. Add tests for behavior changes, especially protocol sequencing, ACK timing, ByteBuf ownership, pressure recovery, or lifecycle changes.
5. Keep remote delivery semantics at least-once: do not make a Beats ACK eligible before the corresponding NiFi transaction is durably committed unless the event is intentionally dropped by configured pre-ACK filtering.

## Network/protocol changes

Changes in the Netty data path should account for:

- event-loop blocking,
- `ByteBuf` reference ownership,
- cleanup on disconnect and handler removal,
- partial frame assembly,
- memory reservations before large accumulation,
- ordered window/sequence semantics,
- retry behavior under pressure,
- commit/ACK races.

For ownership-sensitive changes, include a focused run with Netty paranoid leak detection.

## Performance changes

Please include the workload shape used for performance claims: event size distribution, connection count, TLS/compression settings, JEL rules, batching, CPU/JVM sizing, repository storage, and duration. Throughput numbers without those inputs are difficult to compare.

## Style

Keep domain/protocol logic testable outside NiFi where practical. Prefer explicit state transitions and bounded data structures over implicit/unbounded queues.
