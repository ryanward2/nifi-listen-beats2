# Configuration guidance

ListenBeats2 exposes low-level controls because it is intended to remain predictable under large connection counts, large frames, compressed payloads, and downstream backpressure. Most installations should begin conservatively and change one pressure domain at a time while observing the listener/JVM/NiFi repositories.

The values below are an **example high-throughput profile**, not universal defaults. It assumes a large dedicated NiFi node, pooled direct buffers, high-volume Beats traffic, and sufficient heap/direct memory. Reduce limits substantially on smaller nodes.

## Controller Service example profile

| Setting | Example |
|---|---:|
| Listening Port | `5044` |
| Local Network Interface | blank or dedicated ingest IP |
| Worker Threads | `16` |
| Event Processing Threads | `24` |
| Event Processing Queue Capacity | `1024` |
| Event Processing Queue High-Water Mark | `70` |
| Event Processing Queue Low-Water Mark | `40` |
| Maximum Event Processing Bytes | `2 GB` |
| Connection Cleanup Worker Threads | `8` |
| Maximum Connection Cleanup Pending Tasks | `60000` |
| Cleanup Queue High-Water Mark | `70` |
| Cleanup Queue Low-Water Mark | `40` |
| Socket Receive Buffer | `128 KB` |
| Maximum Receive Bytes per Read | `256 KB` |
| Listen Backlog | `32768` |
| TCP Keepalive | `true` |
| First Protocol Byte Timeout | `30 sec` |
| Protocol Idle Timeout | `0 sec` |
| Frame Assembly Timeout | `30 sec` |
| Event Loop Lag Probe Interval | `100 ms` |
| Maximum Connections | `30000` |
| Maximum Connections per Source | `1000`* |
| Maximum Connection Attempts per Second | `30000` |
| Maximum Connection Attempts per Source per Second | `0`* |
| Maximum Protocol Frames per Connection per Second | `100000` |
| Pooled Direct Buffers | `true` |
| TLS Handshake Timeout | `10 sec` |
| Maximum Concurrent Handshakes | `512` |
| Maximum Frame Size | `16 MB` |
| Maximum Compressed Frame Size | `128 MB` |
| Maximum Decompressed Size | `512 MB` |
| Maximum Compression Ratio | `100` |
| Maximum Frames per Compressed Envelope | `10000` |
| Maximum Events per Window | `10000` |
| Maximum Outstanding Events per Connection | `40000` |
| Maximum Outstanding Bytes per Connection | `512 MB` |
| ACK Write Timeout | `30 sec` |
| Protocol Keepalive Interval | `0 sec` |
| Maximum Queued Events | `2000000` |
| Maximum Queued Bytes | `4 GB` |
| Queue High-Water Mark | `80` |
| Queue Low-Water Mark | `60` |
| Event Filtering | `DROP_MATCHING` when JEL rules are used |
| Filter Evaluation Error Policy | `KEEP_EVENT` |
| Dropped Event Audit Mode | `COUNTERS_ONLY` |
| Maximum Filter Rules | `500` |
| Maximum Unindexed Filter Rules | `0` preferred after rule qualification |
| Batching Strategy | `SIZE_TIME` |
| JSON Batch Key | blank unless JSON-key batching is needed |
| Missing Key Policy | `DEFAULT_BUCKET` |
| Default Batch Bucket | `_missing` |
| Maximum Batch Key Length | `256` |
| Hash Batch Keys | `false` |
| Maximum Active Batch Keys | `2048` |
| Batch Coordinator Partitions | `32` |
| Maximum Events per Batch | `20000` |
| Maximum Bytes per Batch | `16 MB` |
| Maximum Batch Age | `1 sec` |
| Maximum Batch Idle Time | `250 ms` |
| Maximum Ready Batches | `2048` |
| Shutdown Drain Timeout | `60 sec` |
| Metrics Attribute Detail | `BASIC` |
| Metrics Log Interval | `30 sec` |

\* If NiFi sees many real clients behind one NAT/load balancer address, disable or raise per-source limits appropriately. If the proxy preserves the original source address at a layer the listener can actually observe, size the limit to the expected clients per source.

## TLS

For plaintext, leave `SSL Context Service` unset. For TLS, select a NiFi SSL Context Service and choose the required client-authentication policy. For mutual TLS, use `REQUIRED` and ensure the trust configuration contains the expected client CA chain.

## JEL rules

Dynamic property names determine rule order. Numeric prefixes make the ordering explicit:

```text
010-drop-process-noise = event.code == "4688" && process.name in ("example.exe")
020-drop-path-noise    = event.dataset == "log" && file.path startsWith "/var/log/example/"
```

Prefer selective equality/literal `in()` anchors so candidate indexing can avoid evaluating every rule for every event. See [JEL filtering](JEL_FILTERING.md).

## ListenBeats2 processor example

| Setting | Example |
|---|---:|
| Beats Listener Service | the `StandardBeatsListenerService` instance |
| Maximum Batches per Trigger | `16` |
| Maximum Events per Trigger | `160000` |
| Maximum Bytes per Trigger | `256 MB` |

Scheduling starting point:

```text
Timer driven
Run Schedule: 0 sec
Concurrent Tasks: 8
Execution: All Nodes
```

A downstream connection might begin around `15000` objects / `30 GB` of backpressure for very large ingest nodes, but NiFi repository capacity and downstream latency should determine the real setting.

## What to watch while tuning

Increase capacity only when you can identify the limiting stage. Monitor:

- accepted-memory events/bytes,
- processing reserved bytes/tasks,
- ready batches and active keys,
- per-connection outstanding events/bytes,
- read-suspended channels and pressure reasons,
- processing/cleanup queue depth,
- ACK latency and ACK failures/timeouts,
- direct-memory usage and Netty allocator behavior,
- heap/old-gen and GC pauses,
- NiFi FlowFile/Content Repository latency,
- downstream queue backpressure.
