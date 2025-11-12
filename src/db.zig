const std = @import("std");
const unicode = std.unicode;
const WAL_MAGIC_NUMBER = @import("wal.zig").WAL_MAGIC_NUMBER;
const WAL_VERSION = @import("wal.zig").WAL_VERSION;

// Define the types of operations we can log.
const OperationType = enum(u8) {
    Set,
    Delete,
    ListPush,
    ListPop,
};

pub const ValueType = enum(u8) {
    String,
    Integer,
    Float,
    Bool,
    Binary,
};

pub fn printValue(value: Value) void {
    switch (value) {
        .String => |s| std.debug.print("{s}", .{s}),
        .Integer => |i| std.debug.print("{d}", .{i}),
        .Float => |f| std.debug.print("{d}", .{f}),
        .Bool => |b| std.debug.print("{}", .{b}),
        .Binary => |b| {
            std.debug.print("Binary[{d} bytes]: ", .{b.len});
            for (b) |byte| {
                if (byte >= 32 and byte < 127) {
                    std.debug.print("{c}", .{byte});
                } else {
                    std.debug.print("\\x{x:0>2}", .{byte});
                }
            }
        },
    }
}

const Value = union(ValueType) {
    String: []const u8,
    Integer: i64,
    Float: f64,
    Bool: bool,
    Binary: []const u8,
};

// Define the structure of a single log entry.
const LogEntry = union(OperationType) {
    Set: struct { val_type: ValueType, key_len: u32, key: []const u8, value_len: u32, raw_value: []const u8 },
    Delete: struct {
        key_len: u32,
        key: []const u8,
    },
    ListPush: struct {
        key_len: u32,
        key: []const u8,
        value_len: u32,
        value: []const u8,
    },
    ListPop: struct {
        key_len: u32,
        key: []const u8,
    },

    pub fn serialize(self: LogEntry, file: std.fs.File) !void {
        var tag: [1]u8 = .{@intFromEnum(self)};
        try file.writeAll(&tag);

        var buf4: [4]u8 = undefined;

        switch (self) {
            .Set => |s_entry| {
                var vt: [1]u8 = .{@intFromEnum(s_entry.val_type)};
                try file.writeAll(&vt);

                std.mem.writeInt(u32, &buf4, s_entry.key_len, .little);
                try file.writeAll(&buf4);
                std.mem.writeInt(u32, &buf4, s_entry.value_len, .little);
                try file.writeAll(&buf4);
                try file.writeAll(s_entry.key);
                try file.writeAll(s_entry.raw_value);
            },
            .Delete => |d_entry| {
                std.mem.writeInt(u32, &buf4, d_entry.key_len, .little);
                try file.writeAll(&buf4);
                try file.writeAll(d_entry.key);
            },
            .ListPush => |lp| {
                std.mem.writeInt(u32, &buf4, lp.key_len, .little);
                try file.writeAll(&buf4);
                try file.writeAll(lp.key);

                std.mem.writeInt(u32, &buf4, lp.value_len, .little);
                try file.writeAll(&buf4);
                try file.writeAll(lp.value);
            },
            .ListPop => |lp| {
                std.mem.writeInt(u32, &buf4, lp.key_len, .little);
                try file.writeAll(&buf4);
                try file.writeAll(lp.key);
            },
        }
    }

    pub fn deserialize(file: std.fs.File, allocator: std.mem.Allocator) !LogEntry {
        var tag: [1]u8 = undefined;
        const br = try file.readAll(&tag);
        if (br == 0) return error.EndOfStream;
        const op = @as(OperationType, @enumFromInt(tag[0]));

        var buf4: [4]u8 = undefined;

        switch (op) {
            .Set => {
                var vt_buf: [1]u8 = undefined;
                _ = try file.readAll(&vt_buf);
                const vt = @as(ValueType, @enumFromInt(vt_buf[0]));

                _ = try file.readAll(&buf4);
                const key_len = std.mem.readInt(u32, &buf4, .little);
                _ = try file.readAll(&buf4);
                const val_len = std.mem.readInt(u32, &buf4, .little);

                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                if (try file.readAll(key) < key_len) return error.EndOfStream;

                const val = try allocator.alloc(u8, val_len);
                errdefer allocator.free(val);
                if (try file.readAll(val) < val_len) return error.EndOfStream;

                return .{
                    .Set = .{
                        .val_type = vt,
                        .key_len = key_len,
                        .key = key,
                        .value_len = val_len,
                        .raw_value = val,
                    },
                };
            },
            .Delete => {
                _ = try file.readAll(&buf4);
                const key_len = std.mem.readInt(u32, &buf4, .little);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                if (try file.readAll(key) < key_len) return error.EndOfStream;

                return LogEntry{
                    .Delete = .{
                        .key_len = key_len,
                        .key = key,
                    },
                };
            },
            .ListPush => {
                _ = try file.readAll(&buf4);
                const key_len = std.mem.readInt(u32, &buf4, .little);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                if (try file.readAll(&buf4) < key_len) return error.EndOfStream;

                _ = try file.readAll(&buf4);
                const val_len = std.mem.readInt(u32, &buf4, .little);
                const val = try allocator.alloc(u8, val_len);
                errdefer allocator.free(val);
                if (try file.readAll(val) < val_len) return error.EndOfStream;

                return LogEntry{ .ListPush = .{
                    .key_len = key_len,
                    .key = key,
                    .value_len = val_len,
                    .value = val,
                } };
            },
            .ListPop => {
                _ = try file.readAll(&buf4);
                const key_len = std.mem.readInt(u32, &buf4, .little);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                if (try file.readAll(key) < key_len) return error.EndOfStream;

                return LogEntry{
                    .ListPop = .{
                        .key_len = key_len,
                        .key = key,
                    },
                };
            },
        }
    }

    pub fn printTable(self: LogEntry) void {
        std.debug.print("+-----------+---------------+-----------------------+\n", .{});
        std.debug.print("| Operation | Key           | Value                 |\n", .{});
        std.debug.print("+-----------+---------------+-----------------------+\n", .{});

        switch (self) {
            .Set => |s_entry| {
                std.debug.print("| {s:9} | {s:13} | ", .{
                    "Set",
                    s_entry.key,
                });

                var printed = false;

                if (s_entry.raw_value.len == 8) {
                    const bits = std.mem.readInt(u64, s_entry.raw_value[0..8], .little);
                    const float_val: f64 = @bitCast(bits);
                    const int_val: i64 = @bitCast(bits);

                    // Heuristic: if float interpretation gives a value < 1e-100 or > 1e100,
                    // or is NaN/Inf, it's probably an integer
                    const abs_float = @abs(float_val);
                    if (std.math.isNan(float_val) or std.math.isInf(float_val) or
                        abs_float < 1e-100 or abs_float > 1e100)
                    {
                        // Treat as integer
                        std.debug.print("{d:<21} |\n", .{int_val});
                        printed = true;
                    } else {
                        // Treat as float
                        std.debug.print("{d:<21.2} |\n", .{float_val});
                        printed = true;
                    }
                } else if (s_entry.raw_value.len == 1) {
                    const bool_val = s_entry.raw_value[0] != 0;
                    std.debug.print("{s:<21} |\n", .{if (bool_val) "true" else "false"});
                    printed = true;
                }

                if (!printed) {
                    for (s_entry.raw_value) |b| {
                        if (b >= 32 and b < 127) {
                            std.debug.print("{c}", .{b});
                        } else {
                            std.debug.print(".", .{});
                        }
                    }

                    const val_len = s_entry.raw_value.len;
                    if (val_len < 21) {
                        var i: usize = 0;
                        while (i < (21 - val_len)) : (i += 1) {
                            std.debug.print(" ", .{});
                        }
                    }
                    std.debug.print(" |\n", .{});
                }
            },
            .Delete => |d_entry| {
                std.debug.print("| {s:9} | {s:13} | {s:21} |\n", .{
                    "Delete",
                    d_entry.key,
                    "-",
                });
            },
            .ListPush => |lp| {
                std.debug.print("| {s:9} | {s:13} | {s:21} |\n", .{
                    "ListPush",
                    lp.key,
                    lp.value,
                });
            },
            .ListPop => |lp| {
                std.debug.print("| {s:9} | {s:13} | {s:21} |\n", .{
                    "ListPop",
                    lp.key,
                    "-",
                });
            },
        }
        std.debug.print("+-----------+---------------+-----------------------+\n", .{});
    }
};

pub const ConnectionString = struct {
    file_path: []const u8,
    mode: []const u8,

    pub const ParseError = error{
        InvalidFormat,
        MissingFilePath,
        DuplicateParameter,
    };

    // Parses a Connection string like "file=my_kv_store.log;mode=read_write"
    pub fn parse(allocator: std.mem.Allocator, connectionStr: []const u8) !ConnectionString {
        var parsed_file_path: ?[]const u8 = null;
        var parsed_mode: ?[]const u8 = null;

        var tokens = std.mem.tokenizeAny(u8, connectionStr, ";");
        while (tokens.next()) |token| {
            var parts = std.mem.tokenizeAny(u8, token, "=");
            const key = parts.next() orelse return ParseError.InvalidFormat;
            const value = parts.next() orelse return ParseError.InvalidFormat;

            if (parts.next() != null) return ParseError.InvalidFormat;

            if (std.mem.eql(u8, key, "file")) {
                if (parsed_file_path != null) return ParseError.DuplicateParameter;
                parsed_file_path = try allocator.dupe(u8, value);
            } else if (std.mem.eql(u8, key, "mode")) {
                if (parsed_mode != null) return ParseError.DuplicateParameter;
                parsed_mode = try allocator.dupe(u8, value);
            } else {
                std.debug.print("Unknown parameter: {s}\n", .{key});
            }
        }

        if (parsed_file_path == null) return ParseError.MissingFilePath;

        // Default to read_write mode if not specified
        const mode = if (parsed_mode) |m| m else try allocator.dupe(u8, "read_write");

        return ConnectionString{
            .file_path = parsed_file_path.?,
            .mode = mode,
        };
    }

    pub fn deinit(self: ConnectionString, allocator: std.mem.Allocator) void {
        allocator.free(self.file_path);
        allocator.free(self.mode);
    }
};

pub const Database = struct {
    map: std.StringHashMap(Value),
    lists: std.StringHashMap(std.array_list.Managed([]const u8)),
    allocator: std.mem.Allocator,
    log_file: std.fs.File,
    file_path: []const u8,
    in_tx: bool = false,
    tx_map: ?std.StringHashMap(Value) = null,
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
        _ = std.os.windows.kernel32.SetConsoleOutputCP(65001); // UTF-8
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
            .map = std.StringHashMap(Value).init(allocator),
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
                        };

                        if (db.map.fetchRemove(s_entry.key)) |old| {
                            db.allocator.free(old.key);
                            freeValue(db.allocator, old.value);
                        }

                        try db.map.put(try allocator.dupe(u8, s_entry.key), val);
                        allocator.free(s_entry.key);
                        allocator.free(s_entry.raw_value);
                    },
                    .Delete => |d_entry| {
                        if (db.map.fetchRemove(d_entry.key)) |removed| {
                            db.allocator.free(removed.key);
                            freeValue(allocator, removed.value);
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
                freeValue(self.allocator, entry.value_ptr.*);
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
            freeValue(self.allocator, entry.value_ptr.*);
        }

        self.map.deinit();
        self.log_file.close();
        self.allocator.free(self.file_path);
    }

    pub fn setTyped(self: *Database, key: []const u8, v: Value) !void {
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
        };

        const val_type = switch (v) {
            .String => ValueType.String,
            .Integer => ValueType.Integer,
            .Float => ValueType.Float,
            .Bool => ValueType.Bool,
            .Binary => ValueType.Binary,
        };

        const log_entry = LogEntry{
            .Set = .{
                .val_type = val_type,
                .key_len = @intCast(key.len),
                .key = key,
                .value_len = @intCast(tmp.len),
                .raw_value = tmp,
            },
        };

        if (!self.replaying and !self.in_tx) {
            try log_entry.serialize(self.log_file);
            try self.log_file.sync();
            log_entry.printTable();
        }

        if (self.map.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            freeValue(self.allocator, old.value);
        }
        const new_val = try dupValue(self.allocator, v);
        try self.map.put(try self.allocator.dupe(u8, key), new_val);
    }

    pub fn lpush(self: *Database, key: []const u8, value: []const u8) !void {
        try self.setTyped(key, Value{ .Binary = value });
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
        self.tx_map = std.StringHashMap(Value).init(self.allocator);

        var it = self.map.iterator();
        while (it.next()) |entry| {
            const key_copy = try self.allocator.dupe(u8, entry.key_ptr.*);
            const value_copy = try dupValue(self.allocator, entry.value_ptr.*);
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
            freeValue(self.allocator, entry.value_ptr.*);
        }
        tx_map.deinit();

        var it = self.map.iterator();
        while (it.next()) |entry| {
            const v = entry.value_ptr.*;
            const key = entry.key_ptr.*;

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
            };

            const val_type = switch (v) {
                .String => ValueType.String,
                .Integer => ValueType.Integer,
                .Float => ValueType.Float,
                .Bool => ValueType.Bool,
                .Binary => ValueType.Binary,
            };

            const log_entry = LogEntry{
                .Set = .{
                    .val_type = val_type,
                    .key_len = @intCast(key.len),
                    .key = key,
                    .value_len = @intCast(tmp.len),
                    .raw_value = tmp,
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
            freeValue(self.allocator, entry.value_ptr.*);
        }
        self.map.clearRetainingCapacity();

        var tx_it = tx.iterator();
        while (tx_it.next()) |entry| {
            const key_copy = self.allocator.dupe(u8, entry.key_ptr.*) catch unreachable;
            const val_copy = dupValue(self.allocator, entry.value_ptr.*) catch unreachable;
            self.map.put(key_copy, val_copy) catch unreachable;
        }

        var free_it = tx.iterator();
        while (free_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            freeValue(self.allocator, entry.value_ptr.*);
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
            freeValue(self.allocator, removed.value);
            return true;
        }
        return false;
    }

    pub fn set(self: *Database, key: []const u8, value: []const u8) !void {
        try self.setTyped(key, Value{ .String = value });
    }

    pub fn get(self: *Database, key: []const u8) ?Value {
        return self.map.get(key);
    }

    pub fn setString(self: *Database, key: []const u8, v: []const u8) !void {
        try self.setTyped(key, Value{ .String = v });
    }

    pub fn setInt(self: *Database, key: []const u8, v: i64) !void {
        try self.setTyped(key, Value{ .Integer = v });
    }

    pub fn setFloat(self: *Database, key: []const u8, v: f64) !void {
        try self.setTyped(key, Value{ .Float = v });
    }

    pub fn setBool(self: *Database, key: []const u8, v: bool) !void {
        try self.setTyped(key, Value{ .Bool = v });
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
        };
    }

    pub fn freeValue(a: std.mem.Allocator, v: Value) void {
        switch (v) {
            .String => |s| a.free(s),
            .Binary => |b| a.free(b),
            else => {},
        }
    }
};
