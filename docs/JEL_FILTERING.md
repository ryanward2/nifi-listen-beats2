# Pre-ACK JEL Event Filtering for NiFi 2.11

`StandardBeatsListenerService` evaluates optional JSON Expression Language (JEL) rules before matching Beats events are written to NiFi. The implementation uses candidate-indexed selective streaming evaluation while preserving the NiFi 2.11 Controller Service, bounded pipelining, and commit-coupled cumulative ACK model.

## Configuration

JEL rules are dynamic properties on `StandardBeatsListenerService` because the service owns event admission and ACK eligibility.

| Controller Service property | Default | Guidance |
|---|---:|---|
| Event Filtering | `DISABLED` | Enable only after qualifying the rule set |
| Filter Evaluation Error Policy | `KEEP_EVENT` | Safest default |
| Dropped Event Audit Mode | `COUNTERS_ONLY` | Use `NONE` for the lowest hot-path overhead |
| Maximum Filter Rules | `500` | Hard bound on compiled dynamic rules |
| Maximum Unindexed Filter Rules | `25` | Set `0` to require an indexable anchor on every rule |

Example dynamic properties:

```text
001_drop_security_5156 = winlog.event_id == 5156
002_drop_debug = equalsIgnoreCase(log.level, "debug")
003_drop_processes = endsWithAnyIgnoreCase(process.executable, ["\\cmd.exe", "\\powershell.exe"])
```

Rules are compiled when the service is enabled, sorted by dynamic-property name, and evaluated in that order. The first matching rule wins.

An **indexed rule** contains at least one safe equality or literal `in()` anchor that can select it without evaluating every configured rule. An **unindexed rule** remains a candidate for every event. The service refuses to enable when the compiled unindexed-rule count exceeds the configured maximum.

## Selective streaming evaluation

Filtering no longer builds a complete Jackson `JsonNode` tree for every event.

```text
pooled raw EventPayload
  -> pass 1: validate complete JSON + extract candidate-index anchors
  -> candidate index selects possible rules
  -> pass 2 only when needed: extract remaining candidate fields
  -> evaluate candidates in original rule order
```

Both passes open independent duplicate views over the same pooled payload. The original bytes and buffer indices are unchanged.

Pass 2 is skipped when:

- no indexed or unindexed candidate remains;
- every selected rule uses only fields already extracted as anchors; or
- all rules are unindexed, in which case the implementation performs one complete validation pass that extracts the union of required fields.

Worker-local reusable state includes:

- candidate-rule `BitSet`;
- required-path `BitSet`;
- fixed `Value[]` indexed by compiled path ID;
- array-backed `EvaluationContext`.

Candidate and extraction diagnostic counters update thread-safe `LongAdder` counters on every event, so low-volume snapshots and shutdown-time metrics do not omit a worker-local remainder.

When a selected JEL terminal is an array or object, materialization is bounded to 32 levels, 16,384 contained nodes, and 2,097,152 text characters per parse. Exceeding a bound follows `Filter Evaluation Error Policy` instead of allowing an unbounded heap graph.

## JEL plus JSON-key batching

When filtering and field-based batching are both enabled, the service builds one combined extraction plan from:

- JEL dotted paths; and
- batch JSON Pointers used by `PER_AGENT_TYPE`, `JSON_KV`, or JSON-keyed `HYBRID`.

Equivalent paths such as `agent.type` and `/agent/type` share one compiled path ID. Batch-key scalar values are returned with the filter decision and reused by the batching strategy.

This prevents the former pattern:

```text
full JEL tree parse
  -> second JSON Pointer parse for batching
```

The updated path is at most two streaming passes total for JEL and batching combined. When JEL is disabled, JSON-key batching retains its existing one-pass JSON Pointer extractor. Non-JSON batching modes still perform no JSON parsing.

## Delivery and ACK behavior

Retained event:

```text
receive and validate sequence/window
  -> evaluate JEL
  -> append unchanged raw JSON to a bounded batch
  -> write and commit NiFi FlowFile
  -> mark sequence range committed
  -> send highest contiguous cumulative ACK
```

Intentionally dropped event:

```text
receive and validate sequence/window
  -> evaluate JEL
  -> release pooled payload immediately
  -> mark sequence accepted without a FlowFile
  -> ACK only when the ordered cumulative frontier can advance
```

A dropped suffix cannot ACK past an earlier retained event that has not committed. Consecutive dropped events are coalesced. An all-dropped protocol window creates no empty FlowFile.

Dropped data is intentionally discarded before NiFi durability. This is a configured source-side loss policy, not exactly-once delivery or durable filtering audit.

## Error policies

- `KEEP_EVENT`: retain an event after filter parsing/evaluation failure. If a JSON-key batching mode is also selected, that batching mode can still reject malformed JSON.
- `DROP_EVENT`: intentionally discard an event that could not be evaluated.
- `CLOSE_CONNECTION`: close without ACK so Beats retransmits unacknowledged data.

## Metrics

BASIC metrics include:

```text
beats.filter.rules.configured
beats.filter.rules.indexed
beats.filter.rules.unindexed
beats.filter.paths.referenced
beats.filter.paths.anchors
beats.filter.paths.batch
beats.filter.candidate.rules.avg
beats.filter.rules.evaluated.avg
beats.filter.json.passes.avg
beats.filter.values.extracted.avg
beats.filter.events.input
beats.filter.events.kept
beats.filter.events.dropped
beats.filter.bytes.dropped
beats.filter.evaluation.errors
```

FULL adds cumulative qualification counters:

```text
beats.filter.events.evaluated
beats.filter.candidate.rules.selected
beats.filter.rule.evaluations
beats.filter.json.passes
beats.filter.values.extracted
beats.filter.rule.<rule-name>.matches
```

Rule-level counters are allocated only when `Dropped Event Audit Mode = COUNTERS_ONLY`.

## Performance guidance

- Prefer indexed rules such as `event.code == "4688" && ...`.
- Keep the unindexed-rule limit low; unindexed rules are candidates for every event.
- Put inexpensive, high-hit rules first while preserving intended precedence.
- Use `Dropped Event Audit Mode = NONE` after qualification when rule-level counts are not needed.
- Monitor candidate average, evaluated-rule average, JSON passes, extracted values, processing-worker pressure, ACK latency, allocation rate, and GC.
- Benchmark with the actual event shapes and rule set. A selected array or object can be materialized for JEL semantics, but unrelated document content is skipped and selected containers are bounded by the extraction safety limits.

## Non-goals

The prefilter does not provide persistent rule state, cross-window replay suppression, cluster-wide deduplication, exactly-once delivery, or durable storage of dropped payloads. Connection-scoped Lumberjack sequence numbers are not a safe standalone replay identity.
