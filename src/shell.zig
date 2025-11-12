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
    while (true) {
        try stdout.print("ziggy> ", .{});
        try stdout.flush();

        const maybe_line = io_reader.takeDelimiter('\n') catch |err| switch (err) {
            error.ReadFailed => return err,
            error.StreamTooLong => {
                try stdout.print("Line too long.\n", .{});
                continue;
            },
        };

        const line = maybe_line orelse break;

        const trimmed = std.mem.trim(u8, line, " \r\t");
        if (trimmed.len == 0) continue;

        var toks = std.mem.tokenizeAny(u8, trimmed, " ");
        const cmd_tok = toks.next() orelse continue;
        const command = parseCommand(cmd_tok);

        switch (command) {
            .help => {
                try stdout.print(
                    \\Commands:
                    \\  SET key value
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
                    continue;
                };
                const val = toks.rest();
                if (val.len == 0) {
                    try stdout.print("Missing value\n", .{});
                } else {
                    try db.set(key, val);
                    try stdout.print("OK\n", .{});
                }
            },
            .get => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue;
                };
                if (db.get(key)) |v| {
                    switch (v) {
                        .String => |s| try stdout.print("{s}\n", .{s}),
                        .Integer => |i| try stdout.print("{d}\n", .{i}),
                        .Float => |f| try stdout.print("{d}\n", .{f}),
                        .Bool => |b| try stdout.print("{}\n", .{b}),
                        .Binary => |b| try stdout.print("{s}\n", .{b}),
                    }
                } else try stdout.print("(nil)\n", .{});
            },
            .del => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue;
                };
                const ok = try db.del(key);
                if (ok) try stdout.print("OK\n", .{}) else try stdout.print("(nil)\n", .{});
            },
            .lpush => {
                const key = toks.next() orelse {
                    try stdout.print("Missing key\n", .{});
                    continue;
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
                    continue;
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
                    continue;
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
