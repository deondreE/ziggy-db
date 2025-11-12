const std = @import("std");

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

pub const Value = union(ValueType) {
    String: []const u8,
    Integer: i64,
    Float: f64,
    Bool: bool,
    Binary: []const u8,
};

pub const LogEntry = union(OperationType) {
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
        const CYAN = "\x1b[36m";
        const GREEN = "\x1b[32m";
        const YELLOW = "\x1b[33m";
        const RED = "\x1b[31m";
        const BLUE = "\x1b[34m";
        const MAGENTA = "\x1b[35m";
        const BOLD = "\x1b[1m";
        const RESET = "\x1b[0m";
        const DIM = "\x1b[2m";

        std.debug.print("{s}╭───────────┬───────────────┬───────────────────────╮{s}\n", .{ DIM, RESET });
        std.debug.print("{s}│{s} {s}Operation{s} {s}│{s} {s}Key{s}           {s}│{s} {s}Value{s}                 {s}│{s}\n", .{ DIM, RESET, BOLD, RESET, DIM, RESET, BOLD, RESET, DIM, RESET, BOLD, RESET, DIM, RESET });
        std.debug.print("{s}├───────────┼───────────────┼───────────────────────┤{s}\n", .{ DIM, RESET });

        switch (self) {
            .Set => |s_entry| {
                std.debug.print("{s}│{s} {s}{s:9}{s} {s}│{s} {s}{s:13}{s} {s}│{s} ", .{ DIM, RESET, GREEN, "Set", RESET, DIM, RESET, CYAN, s_entry.key, RESET, DIM, RESET });

                var printed = false;

                if (s_entry.raw_value.len == 8) {
                    const bits = std.mem.readInt(u64, s_entry.raw_value[0..8], .little);
                    const float_val: f64 = @bitCast(bits);
                    const int_val: i64 = @bitCast(bits);

                    const abs_float = @abs(float_val);
                    if (std.math.isNan(float_val) or std.math.isInf(float_val) or
                        abs_float < 1e-100 or abs_float > 1e100)
                        {
                            std.debug.print("{s}{d:<21}{s} {s}│{s}\n", .{ YELLOW, int_val, RESET, DIM, RESET });
                            printed = true;
                        } else {
                        std.debug.print("{s}{d:<21.2}{s} {s}│{s}\n", .{ YELLOW, float_val, RESET, DIM, RESET });
                        printed = true;
                    }
                } else if (s_entry.raw_value.len == 1) {
                    const bool_val = s_entry.raw_value[0] != 0;
                    const bool_str = if (bool_val) "true" else "false";
                    std.debug.print("{s}{s:<21}{s} {s}│{s}\n", .{ MAGENTA, bool_str, RESET, DIM, RESET });
                    printed = true;
                }

                if (!printed) {
                    std.debug.print("{s}", .{BLUE});
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
                    std.debug.print("{s} {s}│{s}\n", .{ RESET, DIM, RESET });
                }
            },
            .Delete => |d_entry| {
                std.debug.print("{s}│{s} {s}{s:9}{s} {s}│{s} {s}{s:13}{s} {s}│{s} {s}{s:21}{s} {s}│{s}\n", .{ DIM, RESET, RED, "Delete", RESET, DIM, RESET, CYAN, d_entry.key, RESET, DIM, RESET, DIM, "-", RESET, DIM, RESET });
            },
            .ListPush => |lp| {
                std.debug.print("{s}│{s} {s}{s:9}{s} {s}│{s} {s}{s:13}{s} {s}│{s} {s}{s:21}{s} {s}│{s}\n", .{ DIM, RESET, GREEN, "ListPush", RESET, DIM, RESET, CYAN, lp.key, RESET, DIM, RESET, BLUE, lp.value, RESET, DIM, RESET });
            },
            .ListPop => |lp| {
                std.debug.print("{s}│{s} {s}{s:9}{s} {s}│{s} {s}{s:13}{s} {s}│{s} {s}{s:21}{s} {s}│{s}\n", .{ DIM, RESET, RED, "ListPop", RESET, DIM, RESET, CYAN, lp.key, RESET, DIM, RESET, DIM, "-", RESET, DIM, RESET });
            },
        }

        std.debug.print("{s}╰───────────┴───────────────┴───────────────────────╯{s}\n", .{ DIM, RESET });
    }
};