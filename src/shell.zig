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
                    \\  SET key value [EXP seconds] [TYPE <STRING|INT|FLOAT|BOOL|BINARY|TIMESTAMP>]
                    \\  GET key
                    \\  DEL key
                    \\  LPUSH key value [value ...]
                    \\  LPOP key
                    \\  LRANGE key start stop
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
                            try stdout.print("Missing type for TYPE option (STRING, INT, FLOAT, BOOL, BINARY, TIMESTAMP)\n", .{});
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
