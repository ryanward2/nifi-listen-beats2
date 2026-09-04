# ListenBeats2 Production Qualification Plan

This source revision contains the scale and correctness controls required for serious testing, but production approval still requires measurements in the exact NiFi 2.11.0 deployment.

## Build gate

```bash
mvn -T1C clean install
```

Tests must run; do not use `-DskipTests` for the qualification build.

## Compatibility gate

Test exact deployed patch releases from both Elastic 8.x and 9.x for:

- Filebeat
- Winlogbeat
- Metricbeat
- Auditbeat
- Heartbeat
- Elastic Agent

For each client test plain TCP where allowed, TLS, mutual TLS where required, compressed and uncompressed traffic, default pipelining, pipelining disabled, large windows, reconnect after reset, and delayed ACK.

## Connection stages

Run each stage long enough for heap, direct memory, queues, and repository latency to reach steady state:

```text
1,000
5,000
10,000
20,000
30,000 connections
```

Workloads:

1. Established and idle.
2. One event every several minutes.
3. Uniform continuous traffic.
4. Bursty traffic.
5. A small number of hot senders.
6. High-cardinality JSON batching.
7. Compressed and uncompressed payloads.
8. 250 B, 1 KiB, 4 KiB, and 16 KiB events.

## Required observations

- Current/accepted/rejected connections.
- Open file descriptors.
- Payload and wire throughput.
- Events per second.
- ACK latency p50/p95/p99/max.
- Queue age and NiFi commit latency.
- Heap, direct memory, native TLS memory, and allocation rate.
- GC pause and CPU by Netty worker, processing worker, and NiFi processor tasks.
- Accepted events/bytes, processing tasks/bytes, partial-frame reserved bytes.
- Active/ready/claimed batches.
- Pressure transitions, retry cohorts, suspended channels, and accept suspension.
- Content, FlowFile, and Provenance Repository I/O.
- Duplicate and sequence anomaly counts.

## Fault injection

- Kill NiFi before commit/ACK.
- Kill after commit but before ACK.
- Stop downstream scheduling until listener pressure activates.
- Fill accepted-memory and ready-batch limits.
- Exhaust the processing-task budget.
- Send many partial JSON and compressed frames.
- Corrupt compressed frames and exceed ratio/decompressed/frame-count limits.
- Disk full and repository failure.
- TLS handshake storm.
- Reconnect storm.
- Disable Controller Service with active claims and deferred events.

## Acceptance criteria

- No silent data loss.
- No ACK before NiFi commit.
- Ordinary pressure preserves established connections.
- `connections.closed.overload` remains zero in recoverable pressure tests.
- Memory remains inside configured bounds and reaches a stable plateau.
- No Netty reference-count leaks under paranoid leak detection.
- No unbounded queue, map, metric label, or logging path.
- Sequence gaps never advance cumulative ACK.
- Stop/disable produces no double-release, use-after-release, or stranded claim.
- p99 ACK latency remains below the configured client timeout with margin.
- 30,000 idle connections and the agreed active-throughput target pass soak testing.

## Stop conditions

Stop immediately on silent loss, reference-count leak, direct-memory OOME, negative accounting, monotonically increasing retained memory, ACK beyond a sequence gap, or a pressure loop that repeatedly resumes and suspends the full connection population.
