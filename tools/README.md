# ListenBeats2 load tool

`BeatsV2LoadGenerator.java` is a standalone Java 21, nonblocking, plain-TCP Lumberjack v2 load generator. It emits `W` and `J` frames, supports per-window sequence reset or continuous legacy sequences, and validates ordered cumulative `A` acknowledgements.

```bash
cd tools
javac --release 21 BeatsV2LoadGenerator.java
java BeatsV2LoadGenerator 127.0.0.1 5044 1000 10000 1024 1024 1000 RESET
```

Arguments:

```text
host port connections eventsPerConnection payloadBytes windowSize [connectsPerSecond] [RESET|CONTINUOUS]
```

- `RESET` is the default and starts each advertised window at sequence `1`, matching current go-lumber v2 send behavior.
- `CONTINUOUS` carries the unsigned sequence forward across windows for legacy compatibility testing.

The tool is intended for deterministic framing, connection-count, backpressure, ordered-ACK, and ACK-latency testing. It does not replace compatibility tests with actual Filebeat, Winlogbeat, Metricbeat, Auditbeat, Heartbeat, and Elastic Agent 8/9 clients. It currently uses plain TCP; use real Beats agents for TLS and mutual-TLS qualification.
