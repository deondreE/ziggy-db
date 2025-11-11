Persistent storage

- Append-only binary log (WAL). Every Set / Delete outside a transaction is serialized to disk.

- Crash-recovery: on startup the log is replayed to rebuild the in-memory map.

Key / value API

- set(key, value) store or update UTF-8 bytes

- get(key) → ?[]const u8 fetch value or null

- del(key) → bool remove key, returns true if it existed

Transactions

- beginTransaction() creates an in-memory snapshot (copy-on-write)

- set / del inside tx mutate the snapshot only

- commit() applies modified keys to the real map and WAL

- rollback() discards all snapshot changes

• Detects “transaction already active” and “no transaction” error cases

Durability & consistency

- Commit writes each changed key through the normal set() path, guaranteeing it is appended to the log before returning.

- On reopen the committed data survives; rolled-back data does not.

Connection string

- Parser supports “file=<path>;mode=read_write|read_only”

- Rejects invalid format, duplicates, or missing file path.

Access modes

- read_write (default) open or create file, allow mutations

- read_only open existing file read-only; log not appended

Memory management

- Uses std.StringHashMap for the working set.

- All keys / values are heap-duplicated; free’d on overwrite, delete, rollback, deinit.

- GeneralPurposeAllocator leak-checker runs at program exit.

CLI test driver (demo main)

- Exercises: set, get, del, begin, rollback, commit, durability-reopen.

- Prints human-readable tables for each logged operation (outside tx).

Utilities

- cleanupTestFile() helper to remove the WAL between test runs.

- LogEntry.printTable() pretty-prints a single Set / Delete record.

Build compatibility

- Compiles on Zig 0.15.2 (uses std.io.getStdHandle).

- No dependencies beyond Zig standard library.

Error handling

- Graceful handling of EndOfStream / malformed log entries during recovery.

- Distinct errors for transaction misuse (TransactionAlreadyActive, NoTransaction).
