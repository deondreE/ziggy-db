const std = @import("std");
const dbmod = @import("db.zig");

const Command = enum {
    help,
    set,
    get,
    del,
    lpush,
    lpop,
    lrange,
    setbit,
    getbit,
    bitfeild,
    begin,
    commit,
    rollback,
    exit,
    unknown,
};

fn parseCommand(tok: []const u8) Command {
    if (std.ascii.eqlIgnoreCase(tok, "HELP")) return .help;
    if (std.ascii.eqlIgnoreCase(tok, "SET")) return .set;
    if (std.ascii.eqlIgnoreCase(tok, "GET")) return .get;
    if (std.ascii.eqlIgnoreCase(tok, "DEL")) return .del;
    if (std.ascii.eqlIgnoreCase(tok, "LPUSH")) return .lpush;
    if (std.ascii.eqlIgnoreCase(tok, "LPOP")) return .lpop;
    if (std.ascii.eqlIgnoreCase(tok, "LRANGE")) return .lrange;
    if (std.ascii.eqlIgnoreCase(tok, "BEGIN")) return .begin;
    if (std.ascii.eqlIgnoreCase(tok, "SETBIT")) return .setbit;
    if (std.ascii.eqlIgnoreCase(tok, "GETBIT")) return .getbit;
    if (std.ascii.eqlIgnoreCase(tok, "BITFEILD")) return .bitfeild;
    if (std.ascii.eqlIgnoreCase(tok, "COMMIT")) return .commit;
    if (std.ascii.eqlIgnoreCase(tok, "ROLLBACK")) return .rollback;
    if (std.ascii.eqlIgnoreCase(tok, "EXIT") or std.ascii.eqlIgnoreCase(tok, "QUIT"))
        return .exit;
    return .unknown;
}

fn enableWindowsAnsiColors() void {
    if (@import("builtin").os.tag == .windows) {
        const windows = std.os.windows;
        const ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004;
        const STD_OUTPUT_HANDLE: windows.DWORD = @bitCast(@as(i32, -11));

        const handle = windows.kernel32.GetStdHandle(STD_OUTPUT_HANDLE) orelse return;
        if (handle == windows.INVALID_HANDLE_VALUE) return;

        var mode: windows.DWORD = 0;
        if (windows.kernel32.GetConsoleMode(handle, &mode) == 0) return;

        mode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
        _ = windows.kernel32.SetConsoleMode(handle, mode);
    }
}

pub fn main() !void {
    enableWindowsAnsiColors();
    var stdout_buffer: [1024]u8 = undefined;
    var stdin_buffer: [1024]u8 = undefined;

    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    var stdin_reader = std.fs.File.stdin().reader(&stdin_buffer);
    const io_reader = &stdin_reader.interface;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const conn_str =
        try dbmod.ConnectionString.parse(alloc, "file=my_kv_store.log;mode=read_write");
    defer conn_str.deinit(alloc);

    var db = try dbmod.Database.init(alloc, conn_str);
    defer db.deinit();

    const YELLOW = "\x1b[33m";
    const RESET = "\x1b[0m";
    const CYAN = "\x1b[36m";

    try stdout.print(
        \\
        \\  ╔══════════════════════════════════════════════╗
        \\  ║                                              ║
        \\  ║     {s}███████╗██╗ ██████╗  ██████╗██╗   ██╗{s}    ║
        \\  ║     {s}╚══███╔╝██║██╔════╝ ██╔════╝╚██╗ ██╔╝{s}    ║
        \\  ║     {s}  ███╔╝ ██║██║  ███╗██║  ███╗╚████╔╝{s}     ║
        \\  ║     {s} ███╔╝  ██║██║   ██║██║   ██║ ╚██╔╝{s}      ║
        \\  ║     {s}███████╗██║╚██████╔╝╚██████╔╝  ██║{s}       ║
        \\  ║     {s}╚══════╝╚═╝ ╚═════╝  ╚═════╝   ╚═╝{s}       ║
        \\  ║                                      {s}DB{s}      ║
        \\  ║  {s}────────────────────────────────────{s}        ║
        \\  ║  Fast. Reliable. Written in Zig.             ║
        \\  ║                                              ║
        \\  ╚══════════════════════════════════════════════╝
        \\
        \\  Welcome to ZiggyDB shell (v1.0.0 • Zig 0.15.2)
        \\  Type HELP for commands • EXIT to quit
        \\
        \\
    , .{ YELLOW, RESET, YELLOW, RESET, YELLOW, RESET, YELLOW, RESET, YELLOW, RESET, YELLOW, RESET, CYAN, RESET, YELLOW, RESET });

    shell_loop: while (true) { // FIX: Labeled while loop
        try stdout.print("ziggy> ", .{});
        try stdout.flush();

        const maybe_line = io_reader.takeDelimiter('\n') catch |err| switch (err) {
            error.ReadFailed => return err,
            error.StreamTooLong => {
                try stdout.print("Line too long.\n", .{});
                continue :shell_loop;
            },
        };

        const line = maybe_line orelse break;

        const trimmed = std.mem.trim(u8, line, " \r\t");
        if (trimmed.len == 0) continue :shell_loop;

        var toks = std.mem.tokenizeAny(u8, trimmed, " ");
        const cmd_tok = toks.next() orelse continue :shell_loop;
        const command = parseCommand(cmd_tok);

        switch (command) {
            .help => {
                try stdout.print(
                    \\Commands:
                    \\  SET key value [EXP seconds] [TYPE <STRING|INT|FLOAT|BOOL|BINARY|TIMESTAMP|BITMAP|BITFEILD>]
                    \\  GET key
                    \\  DEL key
                    \\  LPUSH key value [value ...]
                    \\  LPOP key
                    \\  LRANGE key start stop
                    \\  SETBIT key offset value (0 or 1)
                    \\  GETBIT key offset
                    \\  BITFEILD key [SET <type> <offset_bytes> <value_str>] [GET <type> <offset_bytes>]
                    \\      Types: u8, u16, u32, u64
                    \\  BEGIN / COMMIT / ROLLBACK
                    \\  HELP / EXIT
                    \\-------------------------------------------
                    \\
                , .{});
            },
            .set => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue :shell_loop;
                };

                var val_str: ?[]const u8 = null;
                var expiry_seconds_from_now: ?u64 = null;
                var value_type: dbmod.ValueType = dbmod.ValueType.String; // String by default;

                var args_temp_alloc = std.array_list.Managed([]const u8).init(alloc);
                defer args_temp_alloc.deinit();

                while (toks.next()) |tok| {
                    try args_temp_alloc.append(tok);
                }

                var i: usize = 0;
                while (i < args_temp_alloc.items.len) : (i += 1) {
                    const arg = args_temp_alloc.items[i];
                    if (std.ascii.eqlIgnoreCase(arg, "EXP")) {
                        if (i + 1 >= args_temp_alloc.items.len) {
                            try stdout.print("Missing seconds for EXP option (must be u64 integer)\n", .{});
                            continue :shell_loop;
                        }
                        const seconds_str = args_temp_alloc.items[i + 1];
                        expiry_seconds_from_now = std.fmt.parseInt(u64, seconds_str, 10) catch {
                            try stdout.print("Invalid seconds for EXP option (must be u64 integer)\n", .{});
                            continue :shell_loop;
                        };
                        i += 1;
                    } else if (std.ascii.eqlIgnoreCase(arg, "TYPE")) {
                        if (i + 1 >= args_temp_alloc.items.len) {
                            try stdout.print("Missing type for TYPE option (STRING, INT, FLOAT, BOOL, BINARY, TIMESTAMP, BITMAP, BITFEILD)\n", .{});
                            continue :shell_loop;
                        }
                        const type_str = args_temp_alloc.items[i + 1];
                        if (std.ascii.eqlIgnoreCase(type_str, "STRING")) {
                            value_type = dbmod.ValueType.String;
                        } else if (std.ascii.eqlIgnoreCase(type_str, "INT")) {
                            value_type = dbmod.ValueType.Integer;
                        } else if (std.ascii.eqlIgnoreCase(type_str, "FLOAT")) {
                            value_type = dbmod.ValueType.Float;
                        } else if (std.ascii.eqlIgnoreCase(type_str, "BOOL")) {
                            value_type = dbmod.ValueType.Bool;
                        } else if (std.ascii.eqlIgnoreCase(type_str, "BINARY")) {
                            value_type = dbmod.ValueType.Binary;
                        } else if (std.ascii.eqlIgnoreCase(type_str, "TIMESTAMP")) {
                            value_type = dbmod.ValueType.Timestamp;
                        } else if (std.ascii.eqlIgnoreCase(type_str, "BITMAP")) {
                            value_type = dbmod.ValueType.Bitmap;
                        } else if (std.ascii.eqlIgnoreCase(type_str, "BITFEILD")) {
                            value_type = dbmod.ValueType.BitFeild;
                        } else {
                            try stdout.print("Unknown type: {s} (expected STRING, INT, FLOAT, BOOL, BINARY, TIMESTAMP)\n", .{type_str});
                            continue :shell_loop;
                        }
                        i += 1; // Consume the type argument
                    } else if (val_str == null) {
                        // This is the actual value argument
                        val_str = arg;
                    } else {
                        try stdout.print("Too many values or unexpected argument: {s}\n", .{arg});
                        continue :shell_loop;
                    }
                }

                if (val_str == null) {
                    try stdout.print("Missing value\n", .{});
                } else {
                    const value_to_set: dbmod.Value = switch (value_type) {
                        .String => dbmod.Value{ .String = val_str.? },
                        .Integer => dbmod.Value{ .Integer = try std.fmt.parseInt(i64, val_str.?, 10) },
                        .Float => dbmod.Value{ .Float = try std.fmt.parseFloat(f64, val_str.?) },
                        .Bool => if (std.ascii.eqlIgnoreCase(val_str.?, "true")) dbmod.Value{ .Bool = true } else dbmod.Value{ .Bool = false },
                        .Binary => dbmod.Value{ .Binary = val_str.? },
                        .Timestamp => dbmod.Value{ .Timestamp = try std.fmt.parseInt(u64, val_str.?, 10) },
                        .Bitmap => dbmod.Value{ .Bitmap = val_str.? },
                        .BitFeild => dbmod.Value{ .Bitmap = val_str.? },
                    };

                    var final_expiry_unix_s: ?u64 = null;
                    if (expiry_seconds_from_now) |sec| {
                        const current_time: u64 = @intCast(std.time.timestamp());
                        final_expiry_unix_s = current_time + sec;
                    }

                    try db.setTyped(key, value_to_set, final_expiry_unix_s);
                    try stdout.print("OK\n", .{});
                }
            },
            .get => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue :shell_loop;
                };
                if (db.get(key)) |v| {
                    dbmod.printValue(v);
                    try stdout.print("\n", .{});
                } else try stdout.print("(nil)\n", .{});
            },
            .del => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue :shell_loop;
                };
                const ok = try db.del(key);
                if (ok) try stdout.print("OK\n", .{}) else try stdout.print("(nil)\n", .{});
            },
            .lpush => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue :shell_loop;
                };
                var n: usize = 0;
                while (toks.next()) |v| {
                    try db.lpush(key, v);
                    n += 1;
                }
                try stdout.print("({d}) pushed\n", .{n});
            },
            .lpop => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue :shell_loop;
                };
                const popped = try db.lpop(key);
                if (popped) |p| {
                    try stdout.print("{s}\n", .{p});
                    alloc.free(p);
                } else try stdout.print("(nil)\n", .{});
            },
            .lrange => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue :shell_loop;
                };
                const s_str = toks.next() orelse "0";
                const e_str = toks.next() orelse "10";
                const s = std.fmt.parseInt(usize, s_str, 10) catch 0;
                const e = std.fmt.parseInt(usize, e_str, 10) catch 10;
                const vals = db.lrange(key, s, e);
                if (vals.len == 0) {
                    try stdout.print("(empty list)\n", .{});
                } else {
                    for (vals, 0..) |v, i| {
                        try stdout.print("{d}) {s}\n", .{ i + 1, v });
                    }
                }
            },
            .setbit => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue :shell_loop;
                };
                const offset_str = toks.next() orelse {
                    try stdout.print("SETBIT Missing offset\n", .{});
                    continue :shell_loop;
                };
                const value_str = toks.next() orelse {
                    try stdout.print("SETBIT Missing value (0 or 1)\n", .{});
                    continue :shell_loop;
                };

                const offset = std.fmt.parseInt(u32, offset_str, 10) catch {
                    try stdout.print("SETBIT Invalid offset (must be u32 integer)\n", .{});
                    continue :shell_loop;
                };

                const value = if (std.ascii.eqlIgnoreCase(value_str, "1")) true else if (std.ascii.eqlIgnoreCase(value_str, "0")) false else {
                    try stdout.print("SETBIT Invalid value (must be 0 or 1)\n", .{});
                    continue :shell_loop;
                };

                try db.setBit(key, offset, value); 
            },
            .getbit => {
                const key = toks.next() orelse {
                    try stdout.print("GETBIT Missing key\n", .{});
                    continue :shell_loop;
                };
                const offset_str = toks.next() orelse {
                    try stdout.print("GETBIT Missing offset\n", .{});
                    continue :shell_loop;
                };
                const offset = std.fmt.parseInt(u32, offset_str, 10) catch {
                    try stdout.print("SETBIT Invalid offset (must be u32 integer)\n", .{});
                    continue :shell_loop;
                };

                if (try db.getBit(key, offset)) |bit_val| {
                    try stdout.print("{d}\n", .{if (bit_val) @as(u8, 0) else @as(u8, 1)});
                } else {
                    try stdout.print("(nil)\n", .{});
                }
            },
            .bitfeild => {
                const key = toks.next() orelse {
                    try stdout.print("BITFIELD Missing key\n", .{});
                    continue :shell_loop;
                };

                const subcommand_tok = toks.next() orelse {
                    try stdout.print("BITFIELD Missing subcommand (SET or GET)\n", .{});
                    continue :shell_loop;
                };

                if (std.ascii.eqlIgnoreCase(subcommand_tok, "SET")) {
                    const type_str = toks.next() orelse {
                        try stdout.print("BITFIELD SET Missing type (u8, u16, u32, u64)\n", .{});
                        continue :shell_loop;
                    };
                    const offset_str = toks.next() orelse {
                        try stdout.print("BITFIELD SET Missing offset_bytes\n", .{});
                        continue :shell_loop;
                    };
                    const value_str = toks.next() orelse {
                        try stdout.print("BITFIELD SET Missing value\n", .{});
                        continue :shell_loop;
                    };

                    const offset_bytes = std.fmt.parseInt(u32, offset_str, 10) catch {
                        try stdout.print("BITFIELD SET Invalid offset_bytes (must be u32 integer)\n", .{});
                        continue :shell_loop;
                    };

                    var value_bytes_arr: [8]u8 = undefined;
                    var value_len_bytes: u32 = 0;

                    if (std.ascii.eqlIgnoreCase(type_str, "u8")) {
                        const val = std.fmt.parseInt(u8, value_str, 10) catch {
                            try stdout.print("BITFIELD SET Invalid u8 value\n", .{});
                            continue :shell_loop;
                        };
                        std.mem.writeInt(u8, value_bytes_arr[0..1], val, .little);
                        value_len_bytes = 1;
                    } else if (std.ascii.eqlIgnoreCase(type_str, "u16")) {
                        const val = std.fmt.parseInt(u16, value_str, 10) catch {
                            try stdout.print("BITFIELD SET Invalid u16 value\n", .{});
                            continue :shell_loop;
                        };
                        std.mem.writeInt(u16, value_bytes_arr[0..2], val, .little);
                        value_len_bytes = 2;
                    } else if (std.ascii.eqlIgnoreCase(type_str, "u32")) {
                        const val = std.fmt.parseInt(u32, value_str, 10) catch {
                            try stdout.print("BITFIELD SET Invalid u32 value\n", .{});
                            continue :shell_loop;
                        };
                        std.mem.writeInt(u32, value_bytes_arr[0..4], val, .little);
                        value_len_bytes = 4;
                    } else if (std.ascii.eqlIgnoreCase(type_str, "u64")) {
                        const val = std.fmt.parseInt(u64, value_str, 10) catch {
                            try stdout.print("BITFIELD SET Invalid u64 value\n", .{});
                            continue :shell_loop;
                        };
                        std.mem.writeInt(u64, value_bytes_arr[0..8], val, .little);
                        value_len_bytes = 8;
                    } else {
                        try stdout.print("BITFIELD SET Unknown type: {s} (expected u8, u16, u32, u64)\n", .{type_str});
                        continue :shell_loop;
                    }

                    try db.bitfieldSet(key, offset_bytes, value_len_bytes, value_bytes_arr[0..value_len_bytes]);
                    try stdout.print("OK\n", .{});
                } else if (std.ascii.eqlIgnoreCase(subcommand_tok, "GET")) {
                    const type_str = toks.next() orelse {
                        try stdout.print("BITFIELD GET Missing type (u8, u16, u32, u64)\n", .{});
                        continue :shell_loop;
                    };
                    const offset_str = toks.next() orelse {
                        try stdout.print("BITFIELD GET Missing offset_bytes\n", .{});
                        continue :shell_loop;
                    };

                    const offset_bytes = std.fmt.parseInt(u32, offset_str, 10) catch {
                        try stdout.print("BITFIELD GET Invalid offset_bytes (must be u32 integer)\n", .{});
                        continue :shell_loop;
                    };

                    var value_len_bytes: u32 = 0;
                    if (std.ascii.eqlIgnoreCase(type_str, "u8")) {
                        value_len_bytes = 1;
                    } else if (std.ascii.eqlIgnoreCase(type_str, "u16")) {
                        value_len_bytes = 2;
                    } else if (std.ascii.eqlIgnoreCase(type_str, "u32")) {
                        value_len_bytes = 4;
                    } else if (std.ascii.eqlIgnoreCase(type_str, "u64")) {
                        value_len_bytes = 8;
                    } else {
                        try stdout.print("BITFIELD GET Unknown type: {s} (expected u8, u16, u32, u64)\n", .{type_str});
                        continue :shell_loop;
                    }

                    if (try db.bitfieldGet(key, offset_bytes, value_len_bytes, alloc)) |retrieved_bytes| {
                        defer alloc.free(retrieved_bytes);

                        if (std.ascii.eqlIgnoreCase(type_str, "u8")) {
                            const val = std.mem.readInt(u8, &retrieved_bytes[0..1].*, .little);
                            try stdout.print("{d}\n", .{val});
                        } else if (std.ascii.eqlIgnoreCase(type_str, "u16")) {
                            const val = std.mem.readInt(u16, &retrieved_bytes[0..2].*, .little);
                            try stdout.print("{d}\n", .{val});
                        } else if (std.ascii.eqlIgnoreCase(type_str, "u32")) {
                            const val = std.mem.readInt(u32, &retrieved_bytes[0..4].*, .little);
                            try stdout.print("{d}\n", .{val});
                        } else if (std.ascii.eqlIgnoreCase(type_str, "u64")) {
                            const val = std.mem.readInt(u64, &retrieved_bytes[0..8].*, .little);
                            try stdout.print("{d}\n", .{val});
                        }
                    } else {
                        try stdout.print("(nil)\n", .{});
                    }
                } else {
                    try stdout.print("BITFIELD Unknown subcommand: {s} (expected SET or GET)\n", .{subcommand_tok});
                }
            },
            .begin => {
                try db.beginTransaction();
                try stdout.print("Transaction started.\n", .{});
            },
            .commit => {
                try db.commit();
                try stdout.print("Committed.\n", .{});
            },
            .rollback => {
                db.rollback();
                try stdout.print("Rolled back.\n", .{});
            },
            .exit => {
                try stdout.print("Goodbye!\n", .{});
                break;
            },

            .unknown => {
                try stdout.print("Unknown command.\n", .{});
            },
        }
        try stdout.flush();
    }

    try stdout.flush();
}
