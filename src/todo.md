## Fixes

1. Strict Type Storage & Retrieval (Complete!)

- Status: DONE! (Implemented ValueType and val_type in LogEntry.Set and deserialization). This was the most crucial architectural fix.

2. Robust List Implementation (Value-Aware Lists)

3. Transactional Itegrity(WAL & Crash Recovery)

- PROBLEM: Transactions (BEGIN, COMMIT, ROLLBACK) are not fully logged, making crash recovery of transaction state impossible. List operations are not transactional.

4. Value Type Enhancments (Value.Binary Refinement)

- Problem: Value.Binary is currently used for arbitrary binary data, but printValue and shell.get use {s} which implies UTF-8. If truly arbitrary binary data is stored, this could print garbage or error.


## Extended Data Types && Advanced Features

1. Snapshots / Compaction

- Problem: WAL file grows indefinitely, leading to slow recovery and high disk usage.

2. Concurrency / Locking (Thread/Process Safety)

- Problem: my_kv_store.log and in-memory structures are not protected from concurrent access, leading to data corruption if multiple threads/processes try to write.

3. Keys with Expiration (TTL)

- Problem: Keys persist forever.

Action:

- Add expires_at: ?u64 (Unix timestamp).

- Implement SETEXP.

- Implement a background routine that periodically scans and deletes expired keys from map and logs Delete operations for them.

4. Increment / Decrement Operations

- Problem: No native way to atomically increment/decrement integer values.


1. More Complex Data Types:

- Problem: Limited to simple strings, numbers, bools, and basic lists of strings.
- Action:
  - Timestamps/Dates: Add a ValueType.Timestamp and store u64 (Unix epoch time).

  - JSON/Structured Data: Add ValueType.JSON and store []const u8 (JSON string). Require parsing/validation.

  - Sets: Implement std.StringHashMap(std.AutoHashMap(Value)) (for unique Values) for sets. Add SADD, SREM, SMEMBERS commands.

  - Sorted Sets: More complex. Requires std.AutoHashMap where values have a score: f64 and member: Value. Add ZADD, ZRANGE, ZSCORE commands.

  - Hashes: Implement std.StringHashMap(std.StringHashMap(Value)) for nested key-value pairs. Add HSET, HGET, HGETALL commands.

2. Improved Error Handling & Robustness:

- Problem: Generic error handling (catch |err| ...) for WAL. No specific handling for "Disk Full" or "Corrupted WAL" outside of "InvalidWalFile" on header.
  -Action:

- Refine LogEntry.deserialize and Database.init to better distinguish between specific types of corruption.

- Add strategies for dealing with disk full errors (e.g., return specific error, potentially retry).

3. Monitoring & Metrics

- Problem: No insight into database performance or state.

- Action:
  - Expose internal metrics: number of keys, list lengths, memory usage, hit/miss ratio, transaction count/duration.

  - Could be accessed via a special INFO command in the shell.

### Phase 4: Shell Usability & Polish (Lower Priority)

1. History & Autocomplete

## Language Bindings (Phase 3: Expanding Reach)

- C API
  - ZIG -> C Types
  - return_types

### Priority

- Go/Rust: If you want to target other systems programming language users. Go's cgo and Rust's FFI are powerful.

- Node.js/JavaScript: For web development (server-side). Drizzle plugin

- C++: If you envision ZiggyDB being used in performance-critical C++ applications.
- Java: Large enterprise ecosystem, but JNI can be cumbersome.
