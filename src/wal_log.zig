const std = @import("std");

const OperationType = enum(u8) {
    Set,
    Delete,
    ListPush,
    ListPop,
    BitmapSetBit,
    BitFeildSet,
};

pub const ValueType = enum(u8) {
    String,
    Integer,
    Float,
    Bool,
    Binary,
    Timestamp,
    Bitmap,
    BitFeild,
};

pub const Value = union(ValueType) {
    String: []const u8,
    Integer: i64,
    Float: f64,
    Bool: bool,
    Binary: []const u8,
    Timestamp: u64,
    Bitmap: []const u8,
    BitFeild: []const u8,
};

pub const LogEntry = union(OperationType) {
    Set: struct {
        val_type: ValueType,
        key_len: u32,
        key: []const u8,
        value_len: u32,
        raw_value: []const u8,
        expiry_unix_s: ?u64,
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
    BitmapSetBit: struct {
        key_len: u32,
        key: []const u8,
        offset: u32,
        value: bool,
    },
    BitFeildSet: struct {
        key_len: u32,
        key: []const u8,
        offset_bytes: u32,
        value_len_bytes: u32,
        value_bytes: []const u8,
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

                // Expiry
                var has_expiry_byte: [1]u8 = undefined;
                if (s_entry.expiry_unix_s) |expiry| {
                    has_expiry_byte[0] = 1;
                    try file.writeAll(&has_expiry_byte);
                    var buf8: [8]u8 = undefined;
                    std.mem.writeInt(u64, &buf8, expiry, .little);
                    try file.writeAll(&buf8);
                } else {
                    has_expiry_byte[0] = 0;
                    try file.writeAll(&has_expiry_byte);
                }
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
            .BitmapSetBit => |bsb_entry| {
                var buf1: [1]u8 = undefined;
                std.mem.writeInt(u32, &buf4, bsb_entry.key_len, .little);
                try file.writeAll(&buf4);
                try file.writeAll(bsb_entry.key);

                std.mem.writeInt(u32, &buf4, bsb_entry.offset, .little);
                try file.writeAll(&buf4);

                buf1[0] = if (bsb_entry.value) 1 else 0;
                try file.writeAll(&buf1);
            },
            .BitFeildSet => |bsf_entry| {
                std.mem.writeInt(u32, &buf4, bsf_entry.key_len, .little);
                try file.writeAll(&buf4);
                try file.writeAll(bsf_entry.key);

                std.mem.writeInt(u32, &buf4, bsf_entry.offset_bytes, .little);
                try file.writeAll(&buf4);
                std.mem.writeInt(u32, &buf4, bsf_entry.value_len_bytes, .little);
                try file.writeAll(&buf4);
                try file.writeAll(bsf_entry.value_bytes);
            },
        }
    }

    pub fn deserialize(file: std.fs.File, allocator: std.mem.Allocator) !LogEntry {
        var tag: [1]u8 = undefined;
        const br = try file.readAll(&tag);
        if (br == 0) return error.EndOfStream;
        const op = @as(OperationType, @enumFromInt(tag[0]));

        var buf4: [4]u8 = undefined;
        var buf1: [1]u8 = undefined;

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

                var has_expiry_byte: [1]u8 = undefined;
                if (try file.readAll(&has_expiry_byte) < 1) return error.EndOfStream;
                var expiry_unix_s: ?u64 = null;
                if (has_expiry_byte[0] == 1) {
                    var buf8: [8]u8 = undefined;
                    if (try file.readAll(&buf8) < 1) return error.EndOfStream;
                    expiry_unix_s = std.mem.readInt(u64, &buf8, .little);
                }

                return .{
                    .Set = .{
                        .val_type = vt,
                        .key_len = key_len,
                        .key = key,
                        .value_len = val_len,
                        .raw_value = val,
                        .expiry_unix_s = expiry_unix_s,
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
            .BitmapSetBit => {
                _ = try file.readAll(&buf4);
                const key_len = std.mem.readInt(u32, &buf4, .little);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                if (try file.readAll(key) < key_len) return error.EndOfStream;

                _ = try file.readAll(&buf4);
                const offset = std.mem.readInt(u32, &buf4, .little);

                _ = try file.readAll(&buf1);
                const value = buf1[0] != 0;

                return LogEntry{
                    .BitmapSetBit = .{
                        .key_len = key_len,
                        .key = key,
                        .offset = offset,
                        .value = value,
                    },
                };
            },
            .BitFeildSet => {
                _ = try file.readAll(&buf4);
                const key_len = std.mem.readInt(u32, &buf4, .little);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                if (try file.readAll(key) < key_len) return error.EndOfStream;

                _ = try file.readAll(&buf4);
                const offset_bytes = std.mem.readInt(u32, &buf4, .little);
                _ = try file.readAll(&buf4);
                const value_len_bytes = std.mem.readInt(u32, &buf4, .little);

                const value_bytes = try allocator.alloc(u8, value_len_bytes);
                errdefer allocator.free(value_bytes);
                if (try file.readAll(value_bytes) < value_len_bytes) return error.EndOfStream;

                return LogEntry{
                    .BitFeildSet = .{
                        .key_len = key_len,
                        .key = key,
                        .offset_bytes = offset_bytes,
                        .value_len_bytes = value_len_bytes,
                        .value_bytes = value_bytes,
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
        const GREY = "\x1b[90m";

        std.debug.print("{s}╭───────────┬───────────────┬───────────────────────╮{s}\n", .{ DIM, RESET });
        std.debug.print("{s}│{s} {s}Operation{s} {s}│{s} {s}Key{s}           {s}│{s} {s}Value{s}                 {s}│{s}\n", .{
            DIM,
            RESET,
            BOLD,
            RESET,
            DIM,
            RESET,
            BOLD,
            RESET,
            DIM,
            RESET,
            BOLD,
            RESET,
            DIM,
            RESET,
        });
        std.debug.print("{s}├───────────┼───────────────┼───────────────────────┤{s}\n", .{ DIM, RESET });

        var actual_content_len: usize = 0;
        const value_column_width: usize = 21;
        switch (self) {
            .Set => |s_entry| {
                std.debug.print("{s}│{s} {s}{s:9}{s} {s}│{s} {s}{s:13}{s} {s}│{s} ", .{ DIM, RESET, GREEN, "Set", RESET, DIM, RESET, CYAN, s_entry.key, RESET, DIM, RESET });

                switch (s_entry.val_type) {
                    .String => {
                        std.debug.print("{s}{s}{s}", .{ BLUE, s_entry.raw_value, RESET });
                        actual_content_len = s_entry.raw_value.len;
                    },
                    .Integer => {
                        const int_val = std.mem.readInt(i64, s_entry.raw_value[0..8], .little);
                        var buf: [32]u8 = undefined;
                        const formatted = std.fmt.bufPrint(&buf, "{d}", .{int_val}) catch "ERR";
                        std.debug.print("{s}{s}{s}", .{ YELLOW, formatted, RESET });
                        actual_content_len = formatted.len;
                    },
                    .Float => {
                        const float_val: f64 = @bitCast(std.mem.readInt(u64, s_entry.raw_value[0..8], .little));
                        var buf: [32]u8 = undefined;
                        const formatted = std.fmt.bufPrint(&buf, "{d:.2}", .{float_val}) catch "ERR";
                        std.debug.print("{s}{s}{s}", .{ YELLOW, formatted, RESET });
                        actual_content_len = formatted.len;
                    },
                    .Bool => {
                        const bool_val = s_entry.raw_value[0] != 0;
                        const bool_str = if (bool_val) "true" else "false";
                        std.debug.print("{s}{s}{s}", .{ MAGENTA, bool_str, RESET });
                        actual_content_len = bool_str.len;
                    },
                    .Binary => {
                        std.debug.print("{s}", .{BLUE});
                        var count_visible: usize = 0;
                        for (s_entry.raw_value) |b| {
                            if (b >= 32 and b < 127) {
                                std.debug.print("{c}", .{b});
                                count_visible += 1;
                            } else {
                                std.debug.print(".", .{});
                                count_visible += 1;
                            }
                        }
                        std.debug.print("{s}", .{RESET});
                        actual_content_len = count_visible;
                    },
                    .Bitmap => {
                        std.debug.print("{s}Bitmap[{d} bytes]: ", .{ CYAN, s_entry.raw_value.len });
                        var current_len: usize = "Bitmap[] bytes: ".len;
                        for (s_entry.raw_value) |byte| {
                            if (current_len + 9 > value_column_width) { // 8 bytes + 1 space
                                std.debug.print("...", .{}); // too long truncate.
                                current_len += 3;
                                break;
                            }

                            for (0..8) |i| {
                                const shift_val: u3 = @intCast(7 - i);
                                if (((byte >> shift_val) & 1) != 0) {
                                    std.debug.print("1", .{});
                                } else {
                                    std.debug.print("0", .{});
                                }
                            }
                            std.debug.print(" ", .{});
                            current_len += 9;
                        }
                        std.debug.print("{s}", .{RESET});
                        actual_content_len += current_len;
                    },
                    .BitFeild => {
                        std.debug.print("{s}BitFeild[{d} bytes]: ", .{ CYAN, s_entry.raw_value.len });
                        var current_len: usize = "Bitfield[] bytes: ".len;
                        for (s_entry.raw_value) |byte| {
                            if (current_len + 4 > value_column_width) {
                                std.debug.print("...", .{});
                                current_len += 3;
                                break;
                            }
                            std.debug.print("\\x{x:0>2}", .{byte});
                            current_len += 4;
                        }
                        actual_content_len = current_len;
                    },
                    .Timestamp => {
                        const ts_val = std.mem.readInt(u64, s_entry.raw_value[0..8], .little);
                        var buf: [64]u8 = undefined;

                        const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = ts_val };
                        const epoch_day = epoch_seconds.getEpochDay();
                        const day_seconds = epoch_seconds.getDaySeconds();
                        const year_day = epoch_day.calculateYearDay();
                        const month_day = year_day.calculateMonthDay();

                        const year = year_day.year;
                        const month = @intFromEnum(month_day.month);
                        const day = month_day.day_index + 1;
                        const hours = day_seconds.getHoursIntoDay();
                        const minutes = day_seconds.getMinutesIntoHour();
                        const seconds = day_seconds.getSecondsIntoMinute();

                        const formatted_ts = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2} UTC", .{ year, month, day, hours, minutes, seconds }) catch "ERR_TS_FORMAT";
                        std.debug.print("{s}{s}{s}", .{ MAGENTA, formatted_ts, RESET });
                        actual_content_len = formatted_ts.len;
                    },
                }

                if (actual_content_len < value_column_width) {
                    var i: usize = 0;
                    while (i < (value_column_width - actual_content_len)) : (i += 1) {
                        std.debug.print(" ", .{});
                    }
                }
                std.debug.print("{s}│{s}\n", .{ DIM, RESET });

                if (s_entry.expiry_unix_s) |expiry| {
                    const current_time_s: u64 = @intCast(std.time.timestamp());

                    std.debug.print("{s}│{s}           {s}│{s}               {s}│{s} ", .{ DIM, RESET, DIM, RESET, DIM, RESET });

                    var expiry_message_buf: [64]u8 = undefined;
                    var expiry_content_len: usize = 0;

                    if (current_time_s >= expiry) {
                        const msg = std.fmt.bufPrint(&expiry_message_buf, "EXPIRED @ {d}", .{expiry}) catch "ERR";
                        std.debug.print("{s}{s}{s}", .{ RED, msg, RESET });
                        expiry_content_len = msg.len;
                    } else {
                        const msg = std.fmt.bufPrint(&expiry_message_buf, "Expires @ {d}", .{expiry}) catch "ERR";
                        std.debug.print("{s}{s}{s}", .{ GREY, msg, RESET });
                        expiry_content_len = msg.len;
                    }

                    if (expiry_content_len < value_column_width) {
                        var i: usize = 0;
                        while (i < (value_column_width - expiry_content_len)) : (i += 1) {
                            std.debug.print(" ", .{});
                        }
                    }
                    std.debug.print("{s}│{s}\n", .{ DIM, RESET });
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
            .BitmapSetBit => |bsb_entry| {
                var value_str: [8]u8 = undefined;
                const len = std.fmt.bufPrint(&value_str, "Bit {d}={d}", .{ bsb_entry.offset, if (bsb_entry.value) @as(u8, 1) else @as(u8, 0) }) catch "ERR";
                std.debug.print("{s}│{s} {s}{s:9}{s} {s}│{s} {s}{s:13}{s} {s}│{s} {s}{s:21}{s} {s}│{s}\n", .{ DIM, RESET, GREEN, "SetBit", RESET, DIM, RESET, CYAN, bsb_entry.key, RESET, DIM, RESET, YELLOW, len, RESET, DIM, RESET });
            },
            .BitFeildSet => |bfs_entry| {
                std.debug.print("{s}│{s} {s}{s:9}{s} {s}│{s} {s}{s:13}{s} {s}│{s} ", .{ DIM, RESET, GREEN, "Bitfield", RESET, DIM, RESET, CYAN, bfs_entry.key, RESET, DIM, RESET });
                var value_repr_buf: [64]u8 = undefined;
                var current_len: usize = 0;
                const formatted = std.fmt.bufPrint(&value_repr_buf, "Off {d}, Len {d}, Val ", .{ bfs_entry.offset_bytes, bfs_entry.value_len_bytes }) catch "ERR";
                current_len = formatted.len;
                std.debug.print("{s}{s}", .{ YELLOW, formatted });

                for (bfs_entry.value_bytes) |byte| {
                    if (current_len + 4 > value_column_width) {
                        std.debug.print("...", .{});
                        current_len += 3;
                        break;
                    }
                    std.debug.print("\\x{x:0>2}", .{byte});
                    current_len += 4;
                }
                if (current_len < value_column_width) {
                    var i: usize = 0;
                    while (i < (value_column_width - current_len)) : (i += 1) {
                        std.debug.print(" ", .{});
                    }
                }
                std.debug.print("{s}│{s}{s}\n", .{ RESET, DIM, RESET });
            },
        }

        std.debug.print("{s}╰───────────┴───────────────┴───────────────────────╯{s}\n", .{ DIM, RESET });
    }
};
