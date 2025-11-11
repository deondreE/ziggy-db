const std = @import("std");
const WAL_MAGIC_NUMBER = @import("wal.zig").WAL_MAGIC_NUMBER;
const WAL_VERSION = @import("wal.zig").WAL_VERSION;

// Define the types of operations we can log.
const OperationType = enum(u8) {
    Set,
    Delete,
    ListPush,
    ListPop,
};

// Define the structure of a single log entry.
const LogEntry = union(OperationType) {
    Set: struct {
        key_len: u32,
        key: []const u8,
        value_len: u32,
        value: []const u8,
    },
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
        var buf: [1]u8 = undefined;
        buf[0] = @intFromEnum(self);
        try file.writeAll(&buf);

        switch (self) {
            .Set => |s_entry| {
                var int_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &int_buf, s_entry.key_len, .little);
                try file.writeAll(&int_buf);
                try file.writeAll(s_entry.key);

                std.mem.writeInt(u32, &int_buf, s_entry.value_len, .little);
                try file.writeAll(&int_buf);
                try file.writeAll(s_entry.value);
            },
            .Delete => |d_entry| {
                var int_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &int_buf, d_entry.key_len, .little);
                try file.writeAll(&int_buf);
                try file.writeAll(d_entry.key);
            },
            .ListPush => |lp| {
                var int_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &int_buf, lp.key_len, .little);
                try file.writeAll(&int_buf);
                try file.writeAll(lp.key);

                std.mem.writeInt(u32, &int_buf, lp.value_len, .little);
                try file.writeAll(&int_buf);
                try file.writeAll(lp.value);
            },
            .ListPop => |lp| {
                var int_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &int_buf, lp.key_len, .little);
                try file.writeAll(&int_buf);
                try file.writeAll(lp.key);
            },
        }
    }

    pub fn deserialize(file: std.fs.File, allocator: std.mem.Allocator) !LogEntry {
        var buf: [1]u8 = undefined;
        const bytes_read = try file.readAll(&buf);
        if (bytes_read == 0) return error.EndOfStream;

        const op_type_byte = buf[0];
        const op_type = @as(OperationType, @enumFromInt(op_type_byte));

        switch (op_type) {
            .Set => {
                var int_buf: [4]u8 = undefined;
                const len_bytes_read = try file.readAll(&int_buf);
                if (len_bytes_read < 4) return error.EndOfStream;
                const key_len = std.mem.readInt(u32, &int_buf, .little);

                const key_buf = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key_buf);
                const key_bytes_read = try file.readAll(key_buf);
                if (key_bytes_read < key_len) return error.EndOfStream;

                const value_len_bytes_read = try file.readAll(&int_buf);
                if (value_len_bytes_read < 4) return error.EndOfStream;
                const value_len = std.mem.readInt(u32, &int_buf, .little);

                const value_buf = try allocator.alloc(u8, value_len);
                errdefer allocator.free(value_buf);
                const value_bytes_read = try file.readAll(value_buf);
                if (value_bytes_read < value_len) return error.EndOfStream;

                return LogEntry{
                    .Set = .{
                        .key_len = key_len,
                        .key = key_buf,
                        .value_len = value_len,
                        .value = value_buf,
                    },
                };
            },
            .Delete => {
                var int_buf: [4]u8 = undefined;
                const len_bytes_read = try file.readAll(&int_buf);
                if (len_bytes_read < 4) return error.EndOfStream;
                const key_len = std.mem.readInt(u32, &int_buf, .little);

                const key_buf = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key_buf);
                const key_bytes_read = try file.readAll(key_buf);
                if (key_bytes_read < key_len) return error.EndOfStream;

                return LogEntry{
                    .Delete = .{
                        .key_len = key_len,
                        .key = key_buf,
                    },
                };
            },
            .ListPush => {
                var int_buf: [4]u8 = undefined;
                _ = try file.readAll(&int_buf);
                const key_len = std.mem.readInt(u32, &int_buf, .little);
                const key_buf = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key_buf);
                _ = try file.readAll(key_buf);

                _ = try file.readAll(&int_buf);
                const val_len = std.mem.readInt(u32, &int_buf, .little);
                const val_buf = try allocator.alloc(u8, val_len);
                errdefer allocator.free(val_buf);
                _ = try file.readAll(val_buf);

                return LogEntry{ .ListPush = .{
                    .key_len = key_len,
                    .key = key_buf,
                    .value_len = val_len,
                    .value = val_buf,
                } };
            },
            .ListPop => {
                var int_buf: [4]u8 = undefined;
                _ = try file.readAll(&int_buf);
                const key_len = std.mem.readInt(u32, &int_buf, .little);
                const key_buf = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key_buf);
                _ = try file.readAll(key_buf);

                return LogEntry{
                    .ListPop = .{
                        .key_len = key_len,
                        .key = key_buf,
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
                std.debug.print("| {s:9} | {s:13} | {s:21} |\n", .{
                    "Set",
                    s_entry.key,
                    s_entry.value,
                });
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
    map: std.StringHashMap([]const u8),
    lists: std.StringHashMap(std.array_list.Managed([]const u8)),
    allocator: std.mem.Allocator,
    log_file: std.fs.File,
    file_path: []const u8,
    in_tx: bool = false,
    tx_map: ?std.StringHashMap([]const u8) = null,

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

        std.debug.print("✓ Valid WAL file (version {d})\n", .{version});
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
            .map = std.StringHashMap([]const u8).init(allocator),
            .lists = std.StringHashMap(std.array_list.Managed([]const u8)).init(allocator),
            .allocator = allocator,
            .log_file = log_file,
            .file_path = try allocator.dupe(u8, conn_str.file_path),
        };
        errdefer allocator.free(db.file_path);

        // Load existing log entries
        const file_size = (try db.log_file.stat()).size;
        if (file_size > 0) {
            try db.log_file.seekTo(0);

            if (file_size >= 6) {
                try verifyWalHeader(db.log_file);
            } else {
                std.debug.print("Warning: File too small to contain valid WAL header\n", .{});
                return error.InvalidWalFile;
            }

            while (true) {
                const entry = LogEntry.deserialize(db.log_file, db.allocator) catch |err| {
                    if (err == error.EndOfStream) break;
                    std.debug.print("Warning: Malformed log entry: {}. Stopping recovery.\n", .{err});
                    break;
                };

                switch (entry) {
                    .Set => |s_entry| {
                        // Remove existing entry if it exists
                        if (db.map.fetchRemove(s_entry.key)) |removed| {
                            db.allocator.free(removed.key);
                            db.allocator.free(removed.value);
                        }

                        const owned_key = try db.allocator.dupe(u8, s_entry.key);
                        const owned_value = try db.allocator.dupe(u8, s_entry.value);
                        try db.map.put(owned_key, owned_value);

                        db.allocator.free(s_entry.key);
                        db.allocator.free(s_entry.value);
                    },
                    .Delete => |d_entry| {
                        if (db.map.fetchRemove(d_entry.key)) |removed| {
                            db.allocator.free(removed.key);
                            db.allocator.free(removed.value);
                        }
                        // Free the temporary key buffer since we don't need it
                        db.allocator.free(d_entry.key);
                    },
                    .ListPush => |lp_entry| {
                        var list = db.lists.getPtr(lp_entry.key) orelse blk: {
                            const new_list = std.array_list.Managed([]const u8).init(db.allocator);
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
            }
        }

        // Position at end of file for appending new entries (only if not read-only)
        if (!is_read_only) {
            try db.log_file.seekTo(file_size);
        }

        return db;
    }

    pub fn deinit(self: *Database) void {
        if (self.tx_map) |*tx| {
            var it = tx.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
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
            self.allocator.free(entry.value_ptr.*);
        }

        self.map.deinit();
        self.log_file.close();
        self.allocator.free(self.file_path);
    }

    pub fn lpush(self: *Database, key: []const u8, value: []const u8) !void {
        const log_entry = LogEntry{
            .ListPush = .{
                .key_len = @intCast(key.len),
                .key = key,
                .value_len = @intCast(value.len),
                .value = value,
            },
        };

        try log_entry.serialize(self.log_file);
        try self.log_file.sync();

        var list = self.lists.getPtr(key) orelse blk: {
            const new_list = std.array_list.Managed([]const u8).init(self.allocator);
            try self.lists.put(try self.allocator.dupe(u8, key), new_list);
            break :blk self.lists.getPtr(key).?;
        };

        const val_copy = try self.allocator.dupe(u8, value);
        try list.insert(0, val_copy);
        log_entry.printTable();
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

        const s = if (start < len) start else len;
        const e = if (stop < len) stop else len;
        return list_ptr.items[s..e];
    }

    pub fn beginTransaction(self: *Database) !void {
        if (self.in_tx) return error.TransactionAlreadyActive;
        self.in_tx = true;
        self.tx_map = std.StringHashMap([]const u8).init(self.allocator);

        var it = self.map.iterator();
        while (it.next()) |entry| {
            const key_copy = try self.allocator.dupe(u8, entry.key_ptr.*);
            const value_copy = try self.allocator.dupe(u8, entry.value_ptr.*);
            try self.tx_map.?.put(key_copy, value_copy);
        }

        std.debug.print("Transaction started.\n", .{});
    }

    pub fn commit(self: *Database) !void {
        if (!self.in_tx or self.tx_map == null) return error.NoTransaction;

        // disable txn flag first so set()/del() become durable
        self.in_tx = false;

        var tx_map = self.tx_map.?;

        // apply changes
        var it = tx_map.iterator();
        while (it.next()) |entry| {
            const need_write =
                self.map.get(entry.key_ptr.*) == null or
                !std.mem.eql(u8, self.map.get(entry.key_ptr.*).?, entry.value_ptr.*);

            if (need_write) try self.set(entry.key_ptr.*, entry.value_ptr.*);
        }

        // ---- free the snapshot buffers (leak fix) ----
        var free_it = tx_map.iterator();
        while (free_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        tx_map.deinit();

        self.tx_map = null;
        std.debug.print("Transaction Committed!\n", .{});
    }

    pub fn rollback(self: *Database) void {
        if (!self.in_tx or self.tx_map == null) return;
        var tx_map = self.tx_map.?;

        var it = tx_map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }

        tx_map.deinit();
        self.tx_map = null;
        self.in_tx = false;
        std.debug.print("Transaction rolled back.\n", .{});
    }

    fn setInMemory(self: *Database, key: []const u8, value: []const u8) !void {
        // Remove existing entry if it exists to free old memory
        if (self.map.fetchRemove(key)) |removed| {
            self.allocator.free(removed.key);
            self.allocator.free(removed.value);
        }

        // Add new entry
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);
        try self.map.put(owned_key, owned_value);
    }

    fn deleteInMemory(self: *Database, key: []const u8) bool {
        if (self.map.fetchRemove(key)) |removed| {
            self.allocator.free(removed.key);
            self.allocator.free(removed.value);
            return true;
        }
        return false;
    }

    pub fn set(self: *Database, key: []const u8, value: []const u8) !void {
        // Write to log first
        const log_entry = LogEntry{
            .Set = .{
                .key_len = @intCast(key.len),
                .key = key,
                .value_len = @intCast(value.len),
                .value = value,
            },
        };

        if (!self.in_tx) {
            try log_entry.serialize(self.log_file);
            try self.log_file.sync();
        }

        // Update in-memory map
        try self.setInMemory(key, value);
        if (!self.in_tx) log_entry.printTable();
    }

    pub fn get(self: *Database, key: []const u8) ?[]const u8 {
        return self.map.get(key);
    }

    pub fn del(self: *Database, key: []const u8) !bool {
        // Write to log first
        const log_entry = LogEntry{
            .Delete = .{
                .key_len = @intCast(key.len),
                .key = key,
            },
        };

        if (!self.in_tx) {
            try log_entry.serialize(self.log_file);
            try self.log_file.sync();

            log_entry.printTable();
        }
        // Update in-memory map
        return self.deleteInMemory(key);
    }
};
