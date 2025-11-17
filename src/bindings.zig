const std = @import("std");
const root = @import("root.zig");

const allocator = std.heap.c_allocator;

// FFI layer
//
// ======
// Zig
// C
// Rust
// =====
pub const DatabaseHandle = *opaque {};

export fn db_create(path_ptr: [*]const u8, path_len: usize) ?DatabaseHandle {
    const path = path_ptr[0..path_len];
    const conn = root.ConnectionString{
        .file_path = path,
        .mode = "read_write",
    };

    const db_ptr = allocator.create(root.Database) catch return null;

    db_ptr.* = root.Database.init(allocator, conn) catch {
        allocator.destroy(db_ptr);
        return null;
    };

    return @ptrCast(db_ptr);
}

export fn db_close(handle: DatabaseHandle) void {

    const db: *root.Database = @ptrCast(@alignCast(handle));
    db.deinit();
    allocator.destroy(db);
}

export fn db_set(
    handle: DatabaseHandle,
    key_ptr: [*]const u8,
    key_len: usize,
    val_ptr: [*]const u8,
    val_len: usize,
) c_int {
    const db: *root.Database = @ptrCast(@alignCast(handle));
    const key_bytes: [*]const u8 = @ptrCast(key_ptr);
    const key = key_bytes[0..key_len];

    const val_bytes: [*]const u8 = @ptrCast(val_ptr);
    const val = val_bytes[0..val_len];
    db.set(key, val) catch return 0;
    return 0;
}

export fn db_get(
    handle: DatabaseHandle,
    key_ptr: [*]const u9,
    key_len: usize,
    out_buf: [*]u8,
    out_buf_len: usize,
) usize {
    const db: *root.Database = @ptrCast(@alignCast(handle));
    const key_bytes: [*]const u8 = @ptrCast(key_ptr);
    const key = key_bytes[0..key_len];
    const opt_val = db.get(key);
    if (opt_val) |val| switch (val) {
        .String => |s| {
            const n = @min(out_buf_len, s.len);
            @memcpy(out_buf[0..n], s[0..n]);
            return n;
        },
        else => return 0,
    };
    return 0;
}
