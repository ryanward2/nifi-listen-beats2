# Changelog

All notable project changes are documented here.

## 1.0.0 — NiFi 2.11 community release

- Targeted the build at Apache NiFi 2.11.0 and Java 21.
- Preserved post-NiFi-commit Lumberjack ACK semantics.
- Preserved bounded Netty direct-buffer ownership and processing reservations.
- Fixed decoder sequence/version preservation when completing JSON and compressed frames.
- Removed long-lived pressure retry retention of closed connection state.
- Made partial listener startup and shutdown cleanup exception-safe.
- Hardened claimed-batch shutdown/rollback finalization.
- Enforced hard batch-byte limits except for one individually oversized event.
- Improved cross-shard oldest-batch eviction fairness.
- Validated complete JSON documents used for batching/filter extraction.
- Added bounds for JEL container extraction and immediate diagnostic metrics.
- Prevented deferred frame retries from double-counting protocol metrics.
- Fixed exact-limit deferred-event retry behavior.
- Started first-protocol-byte timing after TLS handshake completion.
- Restricted protocol keepalive ACKs to safe repeatable partial ACKs.
- Added connection-attempt and per-connection protocol-frame rate controls.
- Added/expanded regression coverage for protocol, pressure, batching, lifecycle, TLS, and filtering paths.
