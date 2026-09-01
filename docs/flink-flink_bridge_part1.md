# Building an Exact Watermark Bridge Between Flink Jobs — Part 1/3

## TL;DR

* A Flink watermark is a promise about event-time progress — and it is **lost at the transport boundary**.
* Downstream jobs today re-estimate it from observed data using an out-of-orderness heuristic, adding **latency** and **imprecision** even when the upstream job had exact knowledge.
* FLIP-467 and DataStream V2 give Flink the explicit mechanism to *carry and emit* progress — but they do not solve how the source connector in the downstream job proves that all covered data actually crossed the transport.
* The missing piece is a **transport cut** — a protocol connecting three things: the event-time boundary, the data it covers, and proof that the covered data crossed the transport.

Part 2 looks at how other streaming engines generalized this problem; Part 3 proposes the exact protocol.

---

## The problem

A common architecture is:

```text
Flink Job A
    │
    │ data
    ▼
Kafka / Pulsar / Pub/Sub / Fluss
    │
    │ data
    ▼
Flink Job B
```

Moving records between jobs is trivial. Preserving **event-time progress** across the boundary is not.

Suppose Job A has established:

```text
watermark = 10:00
```

with the meaning:

> Job A will not produce another event with event time `<= 10:00`.

Ideally Job B should be able to use the same watermark.

Instead, a typical source connector makes Job B derive a new watermark from what it has observed:

```text
max observed event time
        -
allowed out-of-orderness
```

Now Job B is estimating something that Job A already knew.

### Why this matters in practice

* **Latency.** The heuristic makes Job B wait behind an out-of-orderness guard before it can trust its progress. The more uncertainty, the longer the wait — even though Job A already had exact knowledge.
* **Guesswork.** For a new pipeline you rarely know the real out-of-orderness profile, so `maxOutOfOrderness` is seldom tuned. It gets set conservatively large "to be safe" — and that safety margin is paid as latency on every record, forever, even if the actual ordering turns out to be tight.
* **Cost.** Reconstructing watermarks needs extra headroom: buffering, lateness handling, and reprocessing. Jobs that could emit exact progress instead run behind a safety margin, wasting capacity and state.
* **Operational flexibility.** The opposite is equally important. A well-built pipeline should be *easy to split* — break one Flink job into two and scale each side independently — and just as easy to *merge* — combine several jobs into a single dataflow when circumstances change. Whether the boundary between two Flink jobs is a broadcast operator or an external transport should be a deployment decision, not a decision that changes correctness.

The goal of this article is therefore specific:

> **Build a Flink-to-Flink bridge through a durable transport that preserves exact event-time watermarks instead of reconstructing them heuristically at the downstream source node.**

Kafka, Pulsar, Fluss, and Pub/Sub are examples of the transport boundary. The underlying problem is independent of the particular system.

---

# 1. What a Flink watermark means

For event time, a Flink watermark $T$ means the stream has progressed through $T$: events with timestamps at or before $T$ are not expected after the watermark. A watermark can be based on an assumption about out-of-orderness, so late events can still occur when that assumption is violated.

There are therefore two cases:

```text
exact:
    "no future event <= T"

estimated:
    "we expect no future event <= T"
```

The distinction matters for a Flink-to-Flink bridge.

If Job A has exact knowledge, we should not force Job B to recreate an estimate.

---

# 2. How Flink normally generates watermarks

In the DataStream API, a `WatermarkStrategy` defines both timestamp assignment and watermark generation. Flink provides, among others:

```text
forBoundedOutOfOrderness(maxOutOfOrderness)
```

For bounded out-of-orderness(that is used most frequently), Flink's contract is essentially:

$$
W = \max(\text{timestamp observed}) - B
$$

where $B$ is the configured out-of-orderness bound. Flink documents this as an assumption that, after observing timestamp $T$, no event older than $T-B$ will follow.

This works well when the source has a real bound.

For example:

```text
max event time = 12:00
bound = 5 minutes

watermark = 11:55
```

But the 5-minute bound is an assumption. If the upstream system can actually prove that no older event will ever appear, a lateness bound throws away information.

That is the case we care about.

---

# 3. Partitioned sources

Consider Kafka:

```text
partition 0:
    event time 100
    event time 110
    event time 120

partition 1:
    event time  80
    event time  90
    event time 100
```

If event time is monotonic within each partition, the source can track progress separately:

```text
P0 → 120
P1 → 100
```

and the input watermark is constrained by the slower partition:

$$
W = \min(120, 100) = 100
$$

Flink's source architecture explicitly supports split/partition-aware watermark generation, and watermarks from different partitions are merged before they are propagated downstream.

This is important because **a transport position is not automatically an event-time position**.

A Kafka offset tells us where we are in a partition. It does not by itself prove that no future record can have an older event timestamp.

The connector needs an additional property connecting transport progress to event-time progress.

---

# 4. Watermarks inside a Flink job

Once a source emits a watermark, it travels through the Flink dataflow.

At an operator with multiple input channels, Flink keeps track of the watermark from each channel and only propagates aggregate progress when it is safe to do so. `StatusWatermarkValve` is the runtime component responsible for this logic. It tracks each input channel's watermark and stream status and decides when a new watermark can be propagated.

For ordinary event time:

```text
channel 0 → W=100
channel 1 → W=80
channel 2 → W=120

output → min(100,80,120) = 80
```

So the basic rule is:

$$
W_{out}=\min_i(W_i)
$$

The same idea is used repeatedly through a Flink topology.

---

# 5. Why this works inside Flink

A Flink watermark is part of the stream processing protocol, not just a side-channel value.

Conceptually, an input channel contains:

```text
DATA
DATA
DATA
WATERMARK(T)
```

The watermark has a position relative to the records on that channel. The downstream operator processes the preceding records before the watermark. The watermark thus marks a boundary on the channel:

```text
DATA
--------
WATERMARK(T)
```

Inside a Flink dataflow, this gives the runtime a transport-level meaning. The network does not need to globally order the entire topology — what matters is that the progress marker has a well-defined position relative to the data on the relevant input.

---

# 6. The Flink-to-Flink problem

Now put a transport between two Flink jobs:

```text
Flink Job A
    │
    │ data + watermark
    ▼
Kafka / Pulsar / Pub/Sub / Fluss
    │
    │ data + ?
    ▼
Flink Job B
```

Suppose Job A has an exact watermark:

```text
W = 10:00
```

Job A knows:

> No future event generated by Job A will have event time `<= 10:00`.

Now suppose the external transport is unordered.

The transport may contain:

```text
DATA(event_time=09:57)
DATA(event_time=10:00)
WATERMARK(10:00)
```

and Job B may observe:

```text
WATERMARK(10:00)
DATA(event_time=09:57)
```

Job A was not wrong.

Its statement was about **future production**.

It said nothing about records that were already in flight.

So:

$$
\text{upstream watermark} \neq \text{safe downstream watermark}
$$

unless the transport provides an additional guarantee.

---

# 7. The missing piece: a transport cut

To safely propagate the watermark, Job B needs two facts:

### Upstream completeness

Job A has established:

$$
\text{No future upstream event has timestamp} \le T
$$

### Transport completeness

Every record covered by that statement has crossed the transport boundary.

Only then can Job B safely emit:

$$
W = T
$$

This is the core protocol problem:

```text
Job A knows:
    no future event <= T

Transport knows:
    some data is still in flight

Job B needs:
    no future data <= T
```

The upstream watermark solves the first statement.

The transport must solve the second.

---

# 8. FLIP-467: generalized watermarks

Flink's newer watermark work moves the API in a broader direction.

FLIP-467, **Introduce Generalized Watermarks**, shipped with Flink 2.0. It generalizes the watermark mechanism so that a watermark becomes a typed progress/control signal — emitted by sources or operators, propagated through the topology, combined across inputs, and handled by downstream operators. The original event-time watermark is one built-in use case of this framework.

Instead of thinking only about:

```text
event-time watermark
```

the runtime can handle:

```text
watermark type
    identifier
    value
    combination semantics
    handling semantics
```

For example, FLIP-467 defines declarations for `Long` and `Boolean` watermarks and lets the declaration specify how watermarks from multiple inputs are combined.

This matters because the runtime now has an explicit mechanism for carrying progress information beyond the traditional event-time watermark.

---

# 9. DataStream V2 provides the source hook

DataStream API V2 exposes explicit watermark emission from the source.

The `SourceReaderContext` API includes:

```java
emitWatermark(Watermark watermark)
```

and documents it specifically as the mechanism for sending a watermark from a DataStream V2 source.

So the bridge's source node (in Job B) can conceptually do:

```text
external transport
       │
       │ establish safe progress
       ▼
Flink SourceReader
       │
       ▼
emitWatermark(T)
       │
       ▼
Flink Job B
```

The API is sufficient for the final step.

The hard question is what the source connector must establish **before** calling `emitWatermark(T)`.

---

# 10. FLIP-467 is the carrier, not the proof

FLIP-467 solves one half of the problem: once a source actually emits progress, the runtime carries it, combines it, and delivers it as a typed signal. It gives progress a wire format inside Flink.

It does not solve the other half: how a connector establishes that the value it is about to emit is *true*. The runtime will happily propagate a watermark the source cannot justify. FLIP-467 standardizes the plumbing of progress, not the evidence that produces it.

So the condition from Section 7 remains squarely outside Flink:

> Every record covered by an upstream progress assertion has already crossed the transport.

That is a property of the external transport and of the protocol the two jobs run on it. No Flink feature can make an unordered log answer that question on its own.

---

# 11. The sink node has to write the proof, not just see the watermark

The bridge has a write side and a read side. On the read side, the source node in the downstream system must convince itself that covered data crossed the transport. On the write side, the sink node in the upstream system must leave behind the evidence that makes that proof possible. Neither half is Flink's business; both are the protocol's.

FLIP-167 proposed adding watermark handling to the Sink API, driven by multi-stage pipelines: one job's watermark seeding the next job's source watermark. It also raised what a sink node could *do* with the watermark, such as flushing buffered data.

Seeing the watermark is the easy part. Binding it to the data is the hard part:

```text
Flink Job A
    │
    ├── data
    └── watermark
          │
          ▼
       Sink
          │
          ▼
    external transport
          │
          ▼
       Source
          │
          └── watermark
               ↓
          Flink Job B
```

The sink node has to write the records *and* the progress assertion onto the transport so that the downstream system can reconstruct "everything up to T has crossed" — the transport cut from Section 7, viewed from the write side. If the sink node cannot express that binding, the downstream source node has no evidence to build on, and it falls back to the guesswork of Section 2.

---

# 12. The problem, precisely stated

We can now state the problem without depending on a particular transport:

> **Given two Flink jobs connected through a durable, possibly unordered, at-least-once transport, how can Job A's exact event-time watermark be transferred to Job B without introducing a new lateness heuristic?**

The transport may provide:

* persistence,
* retries,
* arbitrary ordering,
* duplicates,
* partitioning,
* offsets or message IDs.

None of those properties alone establishes event-time progress.

The missing protocol has to connect three things:

```text
event-time boundary
        +
the data covered by that boundary
        +
proof that the covered data crossed the transport
```

Only then can Job B safely emit the same watermark.

---

# 13. Why this deserves a broader look

Flink's model is a strong solution **inside a Flink dataflow**.

It uses:

```text
watermarks
+
ordered input semantics
+
per-input progress
+
watermark aggregation
```

FLIP-467 and DataStream V2 make the progress mechanism more general and more explicit.

But a Flink-to-Flink bridge across Kafka, Pulsar, Pub/Sub, Fluss, or another external transport adds a problem that is not solved merely by forwarding a watermark value:

> **How does the downstream system know that all data covered by the watermark has crossed an unordered transport?**

There are other streaming architectures that approach this question differently.

Apache Beam and its managed runner GCP Dataflow treat watermarks and source progress as part of a broader execution model for unbounded data. Differential Dataflow goes further and makes distributed progress tracking a first-class concept.

Those approaches are useful because they let us ask a different question:

> **Can exact event-time progress be preserved without requiring the transport to order the watermark after every data record it covers?**

That question, and how other streaming engines have answered a generalized version of it, is the subject of Part Two. Part Three proposes the exact transport protocol.
