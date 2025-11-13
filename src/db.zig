const std = @import("std");
const unicode = std.unicode;
const WAL_MAGIC_NUMBER = @import("wal.zig").WAL_MAGIC_NUMBER;
const WAL_VERSION = @import("wal.zig").WAL_VERSION;
pub const ConnectionString = @import("connection.zig").ConnectionString;
pub const LogEntry = @import("wal_log.zig").LogEntry;
pub const Value = @import("wal_log.zig").Value;
pub const ValueType = @import("wal_log.zig").ValueType;

pub const ValueWithMetadata = struct {
    value: Value,
    expiry_unix_s: ?u64,
};

pub const Database = struct {
    map: std.StringHashMap(ValueWithMetadata),
    lists: std.StringHashMap(std.array_list.Managed([]const u8)),
    allocator: std.mem.Allocator,
    log_file: std.fs.File,
    file_path: []const u8,
    in_tx: bool = false,
    tx_map: ?std.StringHashMap(ValueWithMetadata) = null,
    replaying: bool = false,

    fn writeWalHeader(file: std.fs.File) !void {
        var header: [6]u8 = undefined;
        std.mem.writeInt(u32, header[0..4], WAL_MAGIC_NUMBER, .little);
        std.mem.writeInt(u16, header[4..6], WAL_VERSION, .little);
        try file.writeAll(&header);
        try file.sync();
    }

    fn verifyWalHeader(file: std.fs.File) !void {
        var header: [6]u8 = undefined;
        const bytes_read = try file.readAll(&header);
        if (bytes_read < 6) {
            std.debug.print("Invalid WAL file: header too short ({d} bytes)\n", .{bytes_read});
            return error.InvalidWalFile;
        }

        const magic = std.mem.readInt(u32, header[0..4], .little);
        if (magic != WAL_MAGIC_NUMBER) {
            std.debug.print("Invalid WAL file: magic number mismatch. Expected 0x{X}, got 0x{X}\n", .{ WAL_MAGIC_NUMBER, magic });
            return error.InvalidWalFile;
        }

        const version = std.mem.readInt(u16, header[4..6], .little);
        if (version != WAL_VERSION) {
            std.debug.print("Unsupported WAL version: {d} (expected {d})\n", .{ version, WAL_VERSION });
            return error.UnsupportedWalVersion;
        }

        if (@import("builtin").os.tag == .windows) {
            _ = std.os.windows.kernel32.SetConsoleOutputCP(65001); // UTF-8
        }
        std.debug.print("{s} Valid WAL file (version {d})\n", .{ "✓", version });
    }

    pub fn init(allocator: std.mem.Allocator, conn_str: ConnectionString) !Database {
        const is_read_only = std.mem.eql(u8, conn_str.mode, "read_only");
        const file_mode: std.fs.File.OpenMode = if (is_read_only) .read_only else .read_write;

        var log_file = std.fs.cwd().openFile(conn_str.file_path, .{ .mode = file_mode }) catch |err| blk: {
            if (err == error.FileNotFound and !is_read_only) {
                const new_file = try std.fs.cwd().createFile(conn_str.file_path, .{ .read = true });
                try writeWalHeader(new_file);
                break :blk new_file;
            }
            return err;
        };
        errdefer log_file.close();

        var db = Database{
            .map = std.StringHashMap(ValueWithMetadata).init(allocator),
            .lists = std.StringHashMap(std.array_list.Managed([]const u8)).init(allocator),
            .allocator = allocator,
            .log_file = log_file,
            .file_path = try allocator.dupe(u8, conn_str.file_path),
        };
        errdefer allocator.free(db.file_path);

        // Load existing log entries
        const file_size = (try db.log_file.stat()).size;
        if (file_size >= 6) {
            try db.log_file.seekTo(0);
            try verifyWalHeader(db.log_file);

            while (true) {
                const entry = LogEntry.deserialize(db.log_file, db.allocator) catch |err| {
                    if (err == error.EndOfStream) break;
                    std.debug.print("Warning: Malformed log entry: {}. Stopping recovery.\n", .{err});
                    break;
                };

                switch (entry) {
                    .Set => |s_entry| {
                        const val: Value = switch (s_entry.val_type) {
                            .String => Value{ .String = try allocator.dupe(u8, s_entry.raw_value) },
                            .Integer => Value{ .Integer = std.mem.readInt(i64, s_entry.raw_value[0..8], .little) },
                            .Float => Value{ .Float = @bitCast(std.mem.readInt(u64, s_entry.raw_value[0..8], .little)) },
                            .Bool => Value{ .Bool = s_entry.raw_value[0] != 0 },
                            .Binary => Value{ .Binary = try allocator.dupe(u8, s_entry.raw_value) },
                            .Timestamp => Value{ .Timestamp = std.mem.readInt(u64, s_entry.raw_value[0..8], .little) },
                        };

                        const key_copy = try allocator.dupe(u8, s_entry.key);
                        const val_meta = ValueWithMetadata{
                            .value = val,
                            .expiry_unix_s = s_entry.expiry_unix_s,
                        };

                        if (db.map.fetchRemove(s_entry.key)) |old| {
                            db.allocator.free(old.key);
                            freeValueWithMetadata(db.allocator, old.value);
                        }

                        try db.map.put(key_copy, val_meta);
                        allocator.free(s_entry.key);
                        allocator.free(s_entry.raw_value);
                    },
                    .Delete => |d_entry| {
                        if (db.map.fetchRemove(d_entry.key)) |removed| {
                            db.allocator.free(removed.key);
                            freeValueWithMetadata(allocator, removed.value);
                        }
                        // Free the temporary key buffer since we don't need it
                        db.allocator.free(d_entry.key);
                    },
                    .ListPush => |lp_entry| {
                        var list = db.lists.getPtr(lp_entry.key) orelse blk: {
                            const new_list = std.array_list.Managed([]const u8).init(allocator);
                            try db.lists.put(
                                try db.allocator.dupe(u8, lp_entry.key),
                                new_list,
                            );
                            break :blk db.lists.getPtr(lp_entry.key).?;
                        };
                        const val_copy = try db.allocator.dupe(u8, lp_entry.value);
                        try list.insert(0, val_copy);
                        db.allocator.free(lp_entry.key);
                        db.allocator.free(lp_entry.value);
                    },
                    .ListPop => |lp_entry| {
                        if (db.lists.getPtr(lp_entry.key)) |list| {
                            if (list.items.len > 0) {
                                const removed = list.orderedRemove(0);
                                db.allocator.free(removed);
                            }
                        }
                        db.allocator.free(lp_entry.key);
                    },
                }
            } else if (file_size > 0) {
                std.debug.print("Warning: File too small to contain valid WAL header\n", .{});
                return error.InvalidWalFile;
            }
        }

        db.replaying = false;
        try db.log_file.seekTo(file_size);

        return db;
    }

    pub fn deinit(self: *Database) void {
        if (self.tx_map) |*tx| {
            var it = tx.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                freeValueWithMetadata(self.allocator, entry.value_ptr.*);
            }
            tx.deinit();
        }

        var lit = self.lists.iterator();
        while (lit.next()) |entry| {
            var arr = entry.value_ptr.*;
            for (arr.items) |item| {
                self.allocator.free(item);
            }
            arr.deinit();

            self.allocator.free(entry.key_ptr.*);
        }
        self.lists.deinit();

        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            freeValueWithMetadata(self.allocator, entry.value_ptr.*);
        }

        self.map.deinit();
        self.log_file.close();
        self.allocator.free(self.file_path);
    }

    pub fn setTyped(self: *Database, key: []const u8, v: Value, expiry_unix_s: ?u64) !void {
        var raw_buf: [8]u8 = undefined;
        const tmp: []const u8 = switch (v) {
            .String => v.String,
            .Binary => v.Binary,
            .Integer => blk: {
                std.mem.writeInt(i64, &raw_buf, v.Integer, .little);
                break :blk raw_buf[0..8];
            },
            .Float => blk: {
                const bits: u64 = @bitCast(v.Float);
                std.mem.writeInt(u64, &raw_buf, bits, .little);
                break :blk raw_buf[0..8];
            },
            .Bool => blk: {
                raw_buf[0] = if (v.Bool) 1 else 0;
                break :blk raw_buf[0..1];
            },
            .Timestamp => blk: {
                std.mem.writeInt(u64, &raw_buf, v.Timestamp, .little);
                break :blk raw_buf[0..8];
            },
        };

        const val_type = switch (v) {
            .String => ValueType.String,
            .Integer => ValueType.Integer,
            .Float => ValueType.Float,
            .Bool => ValueType.Bool,
            .Binary => ValueType.Binary,
            .Timestamp => ValueType.Timestamp,
        };

        var final_time: ?u64 = null;
        if (expiry_unix_s) |sec| {
            const current_time_s: u64 = @intCast(std.time.timestamp());
            final_time = current_time_s + sec;
        }

        const log_entry = LogEntry{
            .Set = .{
                .val_type = val_type,
                .key_len = @intCast(key.len),
                .key = key,
                .value_len = @intCast(tmp.len),
                .raw_value = tmp,
                .expiry_unix_s = final_time,
            },
        };

        if (!self.replaying and !self.in_tx) {
            try log_entry.serialize(self.log_file);
            try self.log_file.sync();
            log_entry.printTable();
        }

        if (self.map.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            freeValueWithMetadata(self.allocator, old.value);
        }
        const new_val = ValueWithMetadata{
            .value = try dupValue(self.allocator, v),
            .expiry_unix_s = expiry_unix_s,
        };
        try self.map.put(try self.allocator.dupe(u8, key), new_val);
    }

    pub fn lpush(self: *Database, key: []const u8, value: []const u8) !void {
        try self.setTyped(key, Value{ .Binary = value }, null);
    }

    pub fn lpop(self: *Database, key: []const u8) !?[]const u8 {
        const list_ptr = self.lists.getPtr(key) orelse return null;
        if (list_ptr.items.len == 0) return null;

        const popped = list_ptr.orderedRemove(0);

        const log_entry = LogEntry{
            .ListPop = .{
                .key_len = @intCast(key.len),
                .key = key,
            },
        };
        try log_entry.serialize(self.log_file);
        try self.log_file.sync();
        log_entry.printTable();

        return popped;
    }

    pub fn lrange(
        self: *Database,
        key: []const u8,
        start: usize,
        stop: usize,
    ) []const []const u8 {
        const list_ptr = self.lists.getPtr(key) orelse return &[_][]const u8{};
        const len = list_ptr.items.len;
        if (len == 0 or start >= len) return &[_][]const u8{};

        const s = start;
        const e = if (stop > len) len else stop;
        return list_ptr.items[s..e];
    }

    pub fn beginTransaction(self: *Database) !void {
        if (self.in_tx) return error.TransactionAlreadyActive;
        self.in_tx = true;
        self.tx_map = std.StringHashMap(ValueWithMetadata).init(self.allocator);

        var it = self.map.iterator();
        while (it.next()) |entry| {
            const key_copy = try self.allocator.dupe(u8, entry.key_ptr.*);
            const value_copy = try dupValueWithMetadata(self.allocator, entry.value_ptr.*);
            try self.tx_map.?.put(key_copy, value_copy);
        }

        std.debug.print("Transaction started.\n", .{});
    }

    pub fn commit(self: *Database) !void {
        if (!self.in_tx or self.tx_map == null) return error.NoTransaction;

        var tx_map = self.tx_map.?;
        self.tx_map = null;
        self.in_tx = false;

        var free_it = tx_map.iterator();
        while (free_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            freeValueWithMetadata(self.allocator, entry.value_ptr.*);
        }
        tx_map.deinit();

        var it = self.map.iterator();
        while (it.next()) |entry| {
            const v_meta = entry.value_ptr.*;
            const v = v_meta.value;
            const key = entry.key_ptr.*;
            const expiry_unix_s = v_meta.expiry_unix_s;

            var raw_buf: [8]u8 = undefined;
            const tmp: []const u8 = switch (v) {
                .String => v.String,
                .Binary => v.Binary,
                .Integer => blk: {
                    std.mem.writeInt(i64, &raw_buf, v.Integer, .little);
                    break :blk raw_buf[0..8];
                },
                .Float => blk: {
                    const bits: u64 = @bitCast(v.Float);
                    std.mem.writeInt(u64, &raw_buf, bits, .little);
                    break :blk raw_buf[0..8];
                },
                .Bool => blk: {
                    raw_buf[0] = if (v.Bool) 1 else 0;
                    break :blk raw_buf[0..1];
                },
                .Timestamp => blk: {
                    std.mem.writeInt(u64, &raw_buf, v.Timestamp, .little);
                    break :blk raw_buf[0..8];
                },
            };

            const val_type = switch (v) {
                .String => ValueType.String,
                .Integer => ValueType.Integer,
                .Float => ValueType.Float,
                .Bool => ValueType.Bool,
                .Binary => ValueType.Binary,
                .Timestamp => ValueType.Timestamp,
            };

            const log_entry = LogEntry{
                .Set = .{
                    .val_type = val_type,
                    .key_len = @intCast(key.len),
                    .key = key,
                    .value_len = @intCast(tmp.len),
                    .raw_value = tmp,
                    .expiry_unix_s = expiry_unix_s,
                },
            };

            try log_entry.serialize(self.log_file);
        }
        try self.log_file.sync();

        std.debug.print("Transaction Committed!\n", .{});
    }

    pub fn rollback(self: *Database) void {
        if (!self.in_tx or self.tx_map == null) return;

        var tx = self.tx_map.?;
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            freeValueWithMetadata(self.allocator, entry.value_ptr.*);
        }
        self.map.clearRetainingCapacity();

        var tx_it = tx.iterator();
        while (tx_it.next()) |entry| {
            const key_copy = self.allocator.dupe(u8, entry.key_ptr.*) catch unreachable;
            const val_copy = dupValueWithMetadata(self.allocator, entry.value_ptr.*) catch unreachable;
            self.map.put(key_copy, val_copy) catch unreachable;
        }

        var free_it = tx.iterator();
        while (free_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            freeValueWithMetadata(self.allocator, entry.value_ptr.*);
        }
        tx.deinit();

        self.tx_map = null;
        self.in_tx = false;

        std.debug.print("Transaction rolled back.\n", .{});
    }

    fn setInMemory(self: *Database, key: []const u8, value: []const u8) !void {
        if (self.map.fetchRemove(key)) |removed| {
            self.allocator.free(removed.key);
            freeValue(self.allocator, removed.value);
        }

        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);

        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);
        try self.map.put(owned_key, Value{ .String = owned_value });
    }

    fn deleteInMemory(self: *Database, key: []const u8) bool {
        if (self.map.fetchRemove(key)) |removed| {
            self.allocator.free(removed.key);
            freeValueWithMetadata(self.allocator, removed.value);
            return true;
        }
        return false;
    }

    pub fn set(self: *Database, key: []const u8, value: []const u8) !void {
        try self.setTyped(key, Value{ .String = value }, null);
    }

    pub fn get(self: *Database, key: []const u8) ?Value {
        if (self.map.get(key)) |val_meta| {
            if (val_meta.expiry_unix_s) |expiry| {
                const current_time_s: u64 = @intCast(std.time.timestamp());
                if (current_time_s >= expiry) {
                    if (!self.in_tx and !self.replaying) {
                        _ = self.del(key) catch {};
                    } else if (self.in_tx) {
                        _ = self.deleteInMemory(key);
                    }
                }
                return null;
            }
            return val_meta.value;
        }
        return null;
    }

    pub fn setString(self: *Database, key: []const u8, v: []const u8) !void {
        try self.setTyped(key, Value{ .String = v }, null);
    }

    pub fn setInt(self: *Database, key: []const u8, v: i64) !void {
        try self.setTyped(key, Value{ .Integer = v }, null);
    }

    pub fn setFloat(self: *Database, key: []const u8, v: f64) !void {
        try self.setTyped(key, Value{ .Float = v }, null);
    }

    pub fn setBool(self: *Database, key: []const u8, v: bool) !void {
        try self.setTyped(key, Value{ .Bool = v }, null);
    }

    pub fn setTimestamp(self: *Database, key: []const u8, v: u64) !void {
        try self.setTyped(key, Value{ .Timestamp = v }, null);
    }

    pub fn del(self: *Database, key: []const u8) !bool {
        if (!self.in_tx) {
            const log_entry = LogEntry{
                .Delete = .{
                    .key_len = @intCast(key.len),
                    .key = key,
                },
            };
            try log_entry.serialize(self.log_file);
            try self.log_file.sync();
            log_entry.printTable();
        }

        return self.deleteInMemory(key);
    }

    pub fn dupValue(a: std.mem.Allocator, v: Value) !Value {
        return switch (v) {
            .String => |s| Value{ .String = try a.dupe(u8, s) },
            .Binary => |b| Value{ .Binary = try a.dupe(u8, b) },
            .Integer => Value{ .Integer = v.Integer },
            .Float => Value{ .Float = v.Float },
            .Bool => Value{ .Bool = v.Bool },
            .Timestamp => Value{ .Timestamp = v.Timestamp },
        };
    }

    pub fn freeValue(a: std.mem.Allocator, v: Value) void {
        switch (v) {
            .String => |s| a.free(s),
            .Binary => |b| a.free(b),
            else => {},
        }
    }

    pub fn dupValueWithMetadata(a: std.mem.Allocator, vwm: ValueWithMetadata) !ValueWithMetadata {
        return ValueWithMetadata{
            .value = try dupValue(a, vwm.value),
            .expiry_unix_s = vwm.expiry_unix_s,
        };
    }

    pub fn freeValueWithMetadata(a: std.mem.Allocator, vwm: ValueWithMetadata) void {
        freeValue(a, vwm.value);
    }
};

pub fn printValue(value: Value) void {
    const YELLOW = "\x1b[33m"; // Define colors here for printValue's specific usage
    const MAGENTA = "\x1b[35m";
    const BLUE = "\x1b[34m";
    const CYAN = "\x1b[36m";
    const RESET = "\x1b[0m";

    switch (value) {
        .String => |s| std.debug.print("{s}{s}{s}", .{ BLUE, s, RESET }),
        .Integer => |i| std.debug.print("{s}{d}{s}", .{ YELLOW, i, RESET }),
        .Float => |f| std.debug.print("{s}{d:.2}{s}", .{ YELLOW, f, RESET }),
        .Bool => |b| std.debug.print("{s}{}{s}", .{ MAGENTA, b, RESET }),
        .Binary => |b| {
            std.debug.print("{s}Binary[{d} bytes]: {s}", .{ CYAN, b.len, RESET });
            // Print a hex representation for binary data, more robust than just '.'
            // Or if you prefer printable characters and '.' for others
            // for (b) |byte| {
            //     if (byte >= 32 and byte < 127) {
            //         std.debug.print("{c}", .{byte});
            //     } else {
            //         std.debug.print(".", .{});
            //     }
            // }
            // For true binary, hex dump is generally better
            for (b) |byte| {
                std.debug.print("\\x{x:0>2}", .{byte});
            }
        },
        .Timestamp => |ts| {
            // Re-using the robust timestamp formatting logic
            var buf: [64]u8 = undefined; // Buffer for formatted timestamp string

            const current_year: u64 = 1970; // Unix epoch year
            const SECONDS_IN_YEAR: u64 = 31536000;
            const SECONDS_IN_DAY: u64 = 86400;
            const SECONDS_IN_HOUR: u64 = 3600;
            const SECONDS_IN_MINUTE: u64 = 60;

            var remaining_seconds = ts;

            const years_since_epoch = remaining_seconds / SECONDS_IN_YEAR;
            const year = current_year + years_since_epoch;
            remaining_seconds %= SECONDS_IN_YEAR;

            const days_since_year_start = remaining_seconds / SECONDS_IN_DAY;
            const month_approx = (days_since_year_start / 30) + 1; // Very rough month
            const day_approx = (days_since_year_start % 30) + 1; // Very rough day
            remaining_seconds %= SECONDS_IN_DAY;

            const hours = remaining_seconds / SECONDS_IN_HOUR;
            remaining_seconds %= SECONDS_IN_HOUR;

            const minutes = remaining_seconds / SECONDS_IN_MINUTE;
            remaining_seconds %= SECONDS_IN_MINUTE;

            const seconds = remaining_seconds;

            const formatted_ts = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2} UTC", .{ year, month_approx, day_approx, hours, minutes, seconds }) catch "ERR_TS_FORMAT";

            std.debug.print("{s}{s}{s} {s}(Unix: {d}){s}", .{ MAGENTA, formatted_ts, RESET, BLUE, ts, RESET });
        },
    }
}
