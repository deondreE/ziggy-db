## Fixes

1. Strict Type Storage & Retrieval (Complete!)

- Status: DONE! (Implemented ValueType and val_type in LogEntry.Set and deserialization). This was the most crucial architectural fix.

- [ ] Command reader for the TCP connections.

2. Robust List Implementation (Value-Aware Lists)

3. Transactional Itegrity(WAL & Crash Recovery)

- PROBLEM: Transactions (BEGIN, COMMIT, ROLLBACK) are not fully logged, making crash recovery of transaction state impossible. List operations are not transactional.

4. Value Type Enhancments (Value.Binary Refinement)

- Problem: Value.Binary is currently used for arbitrary binary data, but printValue and shell.get use {s} which implies UTF-8. If truly arbitrary binary data is stored, this could print garbage or error.

- [x] Fix platform dependent windows code.

## What You Have Now

Right now you have:

- A single-node, file-backed KV store (Database).

- WAL replication locally on disk (durable, replayable).

- A command shell and server infrastructure.

- Strong transactional semantics (begin/commit/rollback).

- JSON import/export for persistence.

Missing pieces for clustering:

- Network replication (data dissemination to peers)

- Cluster membership discovery

- Coordination (leader election, consistency control)

- Replication protocol (quorum writes / Raft / gossip)

- Health monitoring and failover (like Sentinel)

- Configuration and dynamic topol

2. Turning It Into a Cluster

Start by introducing node roles and cluster communication primitives.

B. Peer Connectivity

- Use std.net.StreamServer to listen for node-to-node messages.

- Each node maintains connections to other peers.

- Periodically send:
  - heartbeat

  - latest WAL offset

  - leader hint (if this node is a candidate leader)

  C. WAL Replication Path

When a node performs a SET or other mutating command:

1. It writes to WAL locally.

2. Sends an AppendEntry RPC to all followers.

3. Followers append, acknowledge receipt.

4. Once a quorum acknowledges, commit and apply to state machine.

This can piggyback your existing LogEntry.serialize/deserialize.

---

Summary Cheat Sheet

If you want to build ZiggyDB Cluster + Sentinel in Zig:

Add These Components:

1. cluster.zig — (Node identity, heartbeats, peer list)

2. sentinel.zig — (Election, monitoring, topology updates)

3. Replicated WAL — (AppendEntries RPC & leader commit index)

4. server.zig enhancement — accept cluster messages (PING/PONG, Append, Vote, etc.)

5. Configurable node roles (Leader, Follower, Sentinel)

6. Expose a simple REST/gRPC endpoint for health + state view.

🧠 3. Introducing Sentinels

Now, to your idea:

“What if the kv-store just host a relational Sentinel?”

This is very interesting — and makes sense if you want a distributed coordination layer that oversees “logical clusters” or even “relational services.”

Think of Redis Sentinel:

- It monitors nodes (ping/heartbeat).

- Performs leader election when the master fails.

- Reconfigures replicas.

- Updates clients with the new topology.

You can host Sentinel logic inside the KV stores (meaning: every node runs both KV and Sentinel—like a Redis node + sentinel process).

In ZiggyDB’s context:

- Each node runs your existing Database service.

- You embed a SentinelService alongside it.

- Sentinels coordinate via their own cluster gossip protocol.

This way the relational counterpart (if ever used) just “attaches” to the Sentinel network.

```
        ┌─────────────────────────────┐
        │         Clients             │
        └─────────────────────────────┘
                     │
              ┌──────┴────────────┐
              │ Ziggy Sentinel    │  ← Cluster + Failover Orchestrator
              │ (Leader Election) │
              └──────┬────────────┘
         ┌───────────┼───────────────┐
   ┌─────▼───────┐         ┌─────────▼──────────────┐
   │ ZiggyDB     │ (Hot)   │ Ziggy Cold    (Cold)   │
   │ KV Store    │         │ Relational / Columnar  │
   └─────────────┘         └────────────────────────┘
            │                         │
            │        WAL Sync         │
            └─────────────────────────┘
```

Phase Feature Description
1 Node Identifiers Each database identifies itself with an ID + network address.
2 Heartbeats Periodic PING/PONG across nodes to track liveness.
3 Simple Leader Election Most basic rule: lowest UUID or highest uptime. Then evolve to a Raft-like consensus.
4 WAL Propagation Leader pushes LogEntry updates to followers. Followers replay to local state.
5 Sentinels A dedicated coordination thread/module that detects failure, reassigns leadership, and keeps global view.
6 Consensus / Quorum Implement Raft or hybrid Paxos-lite for robustness.
7 Relational Sentinel Bridge Allow additional modules (like SQL layer, analytics) to register themselves under Sentinel-managed namespaces.

🧭 6. Developer Experience Benefit

With this design:

- Developers just talk to the cluster endpoint (DNS-level service entry).

- They don’t need to know which node is leader or follower.

- WAL + Sentinel manage syncing and availability internally.

That’s exactly the level of abstraction Redis Sentinel provides — you’d just be implementing it within ZiggyDB.

> ZiggyDB (hot, in-memory KV core)

> ⤷ Ziggy Sentinel (availability + coordination)

> ⤷ Ziggy Cold (durable, relational, historical backend)

## Ziggy Cold

1. Adaptive Schema Intelligence

- Infer types : Track confidence score of type inference.
- Example: a column may be detected as "95% int, 5% string anamolies" The system keeps a statistical awareness of data quality.
- Developers can query or visualize this to understand dataset consistency.

2. Auto-Normalization Suggestions

- When mutliple datasets seem to share columns (e.g., `user_id` appears everywhere) Ziggy cold suggests relationships.
- Think of it as a "dynamic relational map" that continuosly proposes joins.
- It discovers schemas, rather then requiring you to define them.

3. Query-Time Shape Morphing

- Shape projectors
  - Instead of forcing schema migration, can project data into a virtual schema
    defined at query-time:

  ```SQL
  SELECT * FROM user_events PROJECT (id, name, country TEXT DEFAULT 'Unknown');
  ```

  - If you think about like a live schema overlay layer -- like a join between data and metadata.

- Type Elasticity
  - Support safe automatic type coericions. For instance, if half you "age" column was a string, queries still work with runtime casts or fuzzy type matching -- without failing.

- Compute and Analytics

- Inline compute modules (WASM/Zig, ANY)
  - Example: custom compression, anomly detection, mini-ETL steps, all run within the engine.

- Deep Versioning / Lineage
- Git for Data
  - Ziggy Cold tracks lineage at transaction field level. You could `diff`

  ```bash
    ziggy diff orders@v112 orders@v113
  ```

- Time travel debugging
  - You could `replay` database state between timestamps and run `what-if` queries.\

  ```
  SELECT * FROM orders BETWEEN '2025-11-10' AND '2025-11-18';
  ```

- Causality Graphs

## Storage

- LSM Tree (write-heavy)

- Columnar (read-heavy)

- Object Store (immutable data)

- Unified

- Semantic Compression
  - u64
  - Domain structures: compress UUIDs differently than timestamps or JSON fields.

> “Structure emerges from observation.”
> Ziggy Cold doesn’t impose schema — it learns it.

```SQL

SELECT schema_of('user_events')
-- returns {id: int, name: text, age: int?, metadata: json}

ALTER TABLE user_events CONSOLIDATE SCHEMA;

```

(strict = fail on mismatched types, relaxed = best-effort).
