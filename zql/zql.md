# Core

ZQL should be:

1. Familiar: Reads like SQL — no cognitive tax for engineers.

2. Flexible: Works even if schemas are incomplete, inferred, or mixed across versions.

3. Temporal: Time is a first-class query dimension.

4. Relational + Semi-Structured: Can handle JSON-like or dynamic columns gracefully.

5. Composable & Extensible: Users can inline logic, user-defined functions, even WASM.

It’s SQL++ for evolving data, with temporal and lineage capabilities by design.

## Schema-Optional Querying

```SQL
SELECT name, age FROM users;
```

- Missing columns return `NULL`

You control tolerance

```SQL
SELECT name, age FROM users WITH tolerance = 'strict';
```

(strict = fail on mismatched types, relaxed = best-effort).

## Temporal awareness

```
SELECT * FROM orders AT VERSION 19;
```

```
SELECT *
FROM orders
BETWEEN '2025-01-01' AND '2025-11-18';
```

```
SELECT balance
FROM accounts AS OF '2025-09-30T12:00:00Z'
WHERE user_id = 42;
```

## Inline Struct json support

```
SELECT
  user.id,
  user.meta?.email,
  metrics->'clickRate' AS click_rate
FROM sessions;
```

The ?. operator navigates optional paths safely, returning NULL instead of errors.

The -> operator extracts from semi-structured objects (JSON, map literals).

## Temporal Joins and Historical Merges

```
SELECT o.order_id, o.amount, u.name
FROM orders AS OF '2025-03-05'
JOIN users AS OF '2025-03-05' ON o.user_id = u.id;
```

compare across time:

```
SELECT
  o.user_id,
  o.amount AS old_amount,
  n.amount AS new_amount
FROM orders AT '2025-01-01' AS o
JOIN orders AT '2025-06-01' AS n
ON o.order_id = n.order_id
WHERE o.amount <> n.amount;
```

This allows “diffing the past” without ETL.

## Computation and Inlining

ZQL supports UDFs and embedded modules (potentially in Zig or WASM).

```
SELECT
  user_id,
  ziggy.cold_udf('normalize_email', email)
FROM users
WHERE ziggy.is_suspicious(login_events) = true;
```

```
CREATE FUNCTION days_between(a TIMESTAMP, b TIMESTAMP)
RETURNS INT AS { return (b - a) / 84600; }
```

The { ... } block compiles to WASM for in-engine execution with sandboxing.

## Unified Hot Cold Quiries

```
SELECT *
FROM hot(user_sessions)
UNION ALL
SELECT *
FROM cold(user_history)
WHERE user_id = 42;
```

The engine pushes predicates appropriately: hot queries to in-memory KV, cold queries to frozen columnar segments.

## Analytical

ZQL includes first-class support for version-aware aggregates and schema evolution analytics.

```
SELECT
  COUNT(*)
  HISTOGRAM(schema_confidence['email'])
FROM USERS
BETWEEN VERSION 4 and VERSION 10
```

## Developer-First Diagnostics

Built-in commands for introspection:

```
EXPLAIN PLAN FOR SELECT * FROM orders WHERE amount > 100;
SHOW STORAGE MAP FOR orders;
SHOW VERSION TREE FOR users;
```

That’s a game-changer for engineers — gives visibility and control without leaving the query language.

## 10. Syntax Philosophy

- Whitespace and case-insensitive like SQL
- Extended keywords: AS OF, VERSION, LINEAGE, TOLERANCE, PROJECT
- Everything is consistent and composable, no ad-hoc JSON sugar
- Query engine understands temporal and structural dimensions natively
