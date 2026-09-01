# Building an Exact Watermark Bridge Between Flink Jobs — Part 2/3

## TL;DR

* **Apache Beam** models a watermark directly as an estimate; its GCP Dataflow runner can tighten it with knowledge of the outstanding backlog (for example the Pub/Sub tracking subscription) instead of a fixed lateness bound. Still an estimate — not a contract.
* **Differential Dataflow** derives progress from what production work can still happen — capabilities and frontiers — not from FIFO delivery of a watermark marker. Progress becomes independent of data ordering.
* Reduced to one-dimensional event time, a frontier is just the minimum over the producers: the same number a Flink watermark would give, obtained differently.
* All three models converge on the same decomposition: the upstream system knows "no future event < T", the transport proves "no covered work in flight", and the downstream system can then treat "watermark T" as safe.
* Part Three derives the concrete transport protocol from that decomposition.

## Apache Beam, GCP Dataflow and Differential Dataflow

Part One looked at Flink's model: a watermark is carried through the dataflow as a progress signal, and its position relative to the records on an input gives it a useful meaning.

For an exact Flink-to-Flink bridge, however, the difficult case is an **unordered external transport**.

An obvious alternative would be to send the watermark in the same channel as the data, the way Flink does inside a job: interleave progress markers between records. Inside Flink that works because the network guarantees order on each channel. Across a transport like Kafka, Pulsar, or Pub/Sub, that does not come for free:

* **It changes the data schema.** If progress is a record in the data stream, the payload format, serializers, and schema validation on both sides must understand control messages. A change in how progress is carried becomes a change in the data contract.
* **It demands ordering guarantees.** A marker is only meaningful if every record it covers arrives before it. Requiring that ordering from the transport couples the two sides: the upstream system must emit in a particular order, and the downstream system depends on it — coupling neither of them needs.

Keeping progress on a separate channel avoids both. The data schema stays untouched, and neither side requires the transport to order anything. The cost is that completeness can no longer be read off the record stream — the downstream system has to establish it some other way.

The upstream system may know:

> "I will not create another event before `T`."

The downstream system still has to account for records before `T` that are already in flight.

Apache Beam, with GCP Dataflow as its managed runner, approaches this from a different direction. Differential Dataflow goes further and makes the underlying idea explicit: progress is about **what work can still produce data**, not necessarily about where a watermark sits in an ordered record stream.

Most engines, however, sit closer to Flink than to either of those. **RisingWave**, the streaming SQL database, follows Flink's model directly: event time is read from record timestamps and the source declares a watermark strategy. **Apache Spark Structured Streaming** uses the same heuristic shape, deriving a watermark as max observed event time minus a configurable delay. **Hazelcast Jet** carries Flink-like watermarks through its pipelines as well. At the opposite end, **Apache Storm** is driven by processing time and has no event-time progress at all, and **Kafka Streams** works with record timestamps but exposes no watermark abstraction. None of this changes our problem — those differences live inside a single job, while the difficulty we are after sits at the transport boundary.

For this article, we deliberately simplify that model to one dimension: ordinary event time.

---

# 1. Apache Beam: the watermark as an estimate of the future

Apache Beam defines a watermark for every `PCollection` — an estimate of the lower bound of timestamps that may still appear. The source node produces the initial watermark; the runner propagates and aggregates it as the `PCollection` is processed, merged, and partitioned.

Apache Beam separates data from event-time progress. It does not require events to arrive in event-time order; watermarks are the mechanism for reasoning about completeness when data arrives out of order:

```text
source node
  │
  ├── data
  └── watermark
        │
        ▼
      runner
        │
        ▼
downstream watermark
```

GCP Dataflow's Pub/Sub connector is the concrete example. Pub/Sub provides no event-time ordering. Messages carry a service timestamp (when Pub/Sub received them) and optionally an application event timestamp as an attribute; the connector can be configured to watermark on the latter.

The interesting part is how GCP Dataflow estimates progress. For normal processing it can inspect the age of the oldest unacknowledged message in the subscription. When event timestamps are used, it creates a separate **tracking subscription** and inspects the event timestamps of messages still in the backlog:

```text
Pub/Sub source node
      │
      ├── messages already processed
      └── messages still outstanding
                 │
                 ▼
          event-time backlog
                 │
                 ▼
             watermark
```

This stands in contrast to Flink's way:

```text
max event time seen
        -
fixed lateness bound
```

GCP Dataflow uses knowledge of **outstanding source data**. That knowledge alone is enough to derive a true watermark, as long as the upstream system produces event times monotonically. GCP Dataflow gets it from the subscription backlog — which is why it needs neither an ordered progress marker nor a tuned lateness parameter.

---

# 2. The gap: an estimate, not a contract

GCP Dataflow's approach is not an exact cross-job watermark protocol. Even with event timestamps and a tracking subscription, the watermark remains an estimate of the outstanding event-time backlog — the upstream system never sends its true watermark across the boundary, so the downstream system can only estimate what the upstream still holds.

While for our bridge we want exact signal not approximation.

GCP Dataflow contributes a useful **architecture**, but not the exact contract we want.

---

# 3. Differential Dataflow: progress as what can still happen

Differential Dataflow makes the underlying idea explicit. A timestamp is attached to data, but the system does not require the data to arrive in timestamp order. Instead it tracks whether operators may still produce data at particular timestamps.

Differential calls this a **capability**. An operator holding a capability for time `T` can still produce data at `T`; as capabilities advance or are dropped, the system learns which timestamps are no longer possible, and exposes the result as a **frontier** — loosely, the earliest event time that may still produce data.

For one-dimensional event time the numbers can look identical to Flink watermark:

```text
watermark = 100
frontier  = 100
```

The difference is conceptual. A Flink watermark is a **progress marker in the stream**; a Differential frontier is the result of **tracking the remaining ability to produce data**.

The difference shows up on an unordered transport. Two records `DATA(t=50)`, `DATA(t=100)` may be delivered in either order. The system does not need FIFO order — it only needs to establish that nothing in the remaining computation can still produce `t=50`.

That reframes the transport problem directly:

> **Progress is derived from the set of outstanding possibilities, not from FIFO delivery of a watermark message.**

The contract is stated on the edges of the computation. An operator cannot advance its output frontier until it has accounted for the input work that could still produce earlier updates; only then can it tell downstream consumers that its output can no longer contain updates below a particular time. The critical information is:

> **What work remains capable of producing output below the current frontier?**

In its most general form the model also covers iterative flows, where the frontier is no longer a single value but a set of incomparable timestamps — an **antichain** — because feedback loops can keep several versions of a timestamp in flight at once. That is only a hint of how the generalization looks; it does not bear on our problem, which stays one-dimensional.

---

# 4. Reducing it to our problem

We do not need the general model: no iterative computation, no partially ordered time, no antichains. Restricted to one-dimensional event time, the model reduces to work that may still produce events below the frontier, and eventually completes:

```text
Source work
    │
    ├── may still produce events < 100
    └── eventually completes
             │
             ▼
        frontier = 100
```

For multiple producers, the combined frontier is the minimum:

```text
worker A → frontier 100
worker B → frontier 80
worker C → frontier 120
```

$$
W=\min(100,80,120)=80
$$

That looks like the minimum of Flink watermarks. The difference is where the numbers come from: Flink reads them off the input streams as watermark markers; Differential derives them from what timestamped production remains outstanding.

On an unordered transport between two Flink jobs, the mapping is direct. The upstream system closes its work at `frontier = 100`; the downstream system must then prove that all elements of that work arrived before the frontier may be used:

```text
upstream system says:
    this work closes at frontier T

downstream system proves:
    all elements of this work have arrived

therefore:
    this work no longer contributes events < T
```

Our protocol does not need Differential's progress engine — only this one-dimensional specialization.

---

# 5. Materialize: a production proof at the Kafka boundary

Materialize is the production system built on the Differential Dataflow model. Inside Materialize, progress flows exactly as Differential Dataflow designs it — capabilities and frontiers, not watermark markers in an ordered stream.

Note that Materialize also does not fully support the bridge requirements on io boundaries. For example it's Kafka source reports progress as the greatest consumed offset per partition:

```text
partition 0 → offset 40,166,616
partition 1 → offset 40,781,940
partition 2 → offset 40,472,272
```

Offsets are committed once messages through them are durably recorded — a strong statement about **ingestion progress**. But it is not automatically event-time progress:

```text
Kafka offset 100 → event_time 10:00
Kafka offset 101 → event_time 10:05
Kafka offset 102 → event_time 08:00
```

Being through offset `101` does not imply that event time has progressed through `10:05`. Kafka offset progress does not supply the missing exact event-time bridge by itself.

---

# 6. Comparing the models

### Flink

```text
watermark
    ↓
ordered stream position
    ↓
per-input minimum
```

The progress marker is part of the stream protocol.

### Apache Beam / GCP Dataflow

```text
source estimated watermark
    +
knowledge of outstanding upstream node backlog
    ↓
estimated watermark
```

Progress can use source-specific knowledge of outstanding work, as in the Pub/Sub tracking subscription.

### Differential Dataflow

```text
timestamped work
    +
outstanding production capabilities
    ↓
frontier
```

Progress is an explicit property of the remaining computation.

For our bridge the third model is attractive because it separates data delivery order from knowledge about remaining event-time work.

---

# 7. What each approach leaves unresolved

**Flink** propagates watermarks well inside a job. What remains is the external boundary: how an unordered transport proves that all records covered by an upstream watermark have crossed before the downstream system emits it. FLIP-467 and DataStream V2 API generalize the progress signal, but they do not define a cross-transport completion protocol.

**Apache Beam / GCP Dataflow** cleanly separates source watermarks from runner processing and shows that backlog information improves the estimate. But its standard watermark is explicitly an estimate, not an exact cross-system certificate — GCP Dataflow demonstrates the right direction, tracking outstanding work, without delivering the watermark-preserving contract between two independently running jobs.

**Differential Dataflow** provides the cleanest abstraction: which timestamps are still possible somewhere in the remaining computation, without requiring FIFO ordering of data and progress markers. But the model is more general than our needs, and it still leaves the concrete engineering question open:

> What exact information must cross an unordered, at-least-once transport so that the downstream system can prove that an upstream-defined event-time boundary has crossed the transport?

---

# 8. The conclusion

The three models lead to the same observation:

> **An event-time watermark is ultimately knowledge about what events can still arrive.**

Flink expresses that knowledge as a watermark propagated through ordered stream inputs. Apache Beam / GCP Dataflow derives it using source-specific knowledge of the outstanding backlog. Differential Dataflow makes it a first-class concept of outstanding timestamped work and exposes the result as a frontier.

The lesson for an exact Flink-to-Flink bridge is not that one model should replace another. It is this:

```text
UPSTREAM SYSTEM KNOWLEDGE
"No future event < T"
        │
        ▼
TRANSPORT KNOWLEDGE
"No covered work remains in flight"
        │
        ▼
DOWNSTREAM SYSTEM KNOWLEDGE
"Watermark T is safe"
```

The first is a property of the upstream system. The second is a transport protocol property. The third is the watermark consumed by the downstream system.

Part Three takes this observation and derives the concrete protocol: work IDs, per-work element IDs, completion messages, at-least-once delivery, unordered transport, downstream-side reconciliation, and the conditions under which the resulting watermark is exact.