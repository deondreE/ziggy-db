const std = @import("std");
const root = @import("root.zig");

const CommandResult = struct {
    msg: []const u8,
    owns: bool = false,
};

pub fn startTcpServer(
    db: *root.Database,
    port: u16,
    allocator: std.mem.Allocator,
) !void {
    const address = try std.net.Address.parseIp("0.0.0.0", port);
    var listener = try address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    std.debug.print("ZiggyDB TCP server listening on {any}\n", .{address.in});

    while (true) {
        var conn = listener.accept() catch |err| {
            std.debug.print("Error accepting client: {s}\n", .{@errorName(err)});
            continue;
        };

        const moved_stream = conn.stream;
        const builtin = @import("builtin");
        conn.stream = std.net.Stream{
            .handle = if (builtin.os.tag == .windows)
                std.os.windows.ws2_32.INVALID_SOCKET
            else
                -1,
        };

        std.debug.print("Accepted client on socket {}\n", .{moved_stream.handle});

        const thread_db_ptr = db;
        const thread_allocator = allocator;

        _ = std.Thread.spawn(.{}, struct {
            fn run(
                stream: std.net.Stream,
                database: *root.Database,
                alloc: std.mem.Allocator,
            ) void {
                handleClient(stream, database, alloc);
            }
        }.run, .{ moved_stream, thread_db_ptr, thread_allocator }) catch |err| {
            std.debug.print("Thread spawn failed: {s}\n", .{@errorName(err)});
            moved_stream.close();
        };
    }
}

fn handleClient(
    stream: std.net.Stream,
    db: *root.Database,
    allocator: std.mem.Allocator,
) void {
    std.debug.print("Starting client handler (socket {})\n", .{stream.handle});

    defer {
        std.debug.print("Closing socket {}\n", .{stream.handle});
        stream.close();
    }

    var read_buf: [1024]u8 = undefined;
    var write_buf: [1024]u8 = undefined;

    var reader = stream.reader(&read_buf);
    var writer = stream.writer(&write_buf);

    _ = writer.interface.writeAll("Welcome to ZiggyDB!\n") catch |err| {
        std.debug.print("Greeting write error: {s}\n", .{@errorName(err)});
        return;
    };

    const MAX_CMD = 1024;
    var cmd_buf: [MAX_CMD]u8 = undefined;

    while (true) {
        const bytes_read = reader.net_stream.read(&cmd_buf) catch |err| switch (err) {
            error.ConnectionResetByPeer, error.SocketNotConnected, error.BrokenPipe => {
                std.debug.print("Client disconnected.\n", .{});
                return;
            },
            else => {
                std.debug.print("Read error: {s}\n", .{@errorName(err)});
                return;
            },
        };
        if (bytes_read == 0) {
            std.debug.print("Received EOF from client.\n", .{});
            break;
        }

        const cmd_line = std.mem.trim(u8, cmd_buf[0..bytes_read], " \r\n\t");
        if (cmd_line.len == 0) continue;

        std.debug.print("Received command: '{s}'\n", .{cmd_line});
        if (std.ascii.eqlIgnoreCase(cmd_line, "exit")) break;

        const result = executeCommand(db, allocator, cmd_line) catch |err| blk: {
            const msg = @errorName(err);
            std.debug.print("Execution failed: {s}\n", .{msg});
            break :blk CommandResult{ .msg = msg, .owns = false };
        };

        _ = writer.interface.writeAll(result.msg) catch |err| {
            std.debug.print("Write failed: {s}\n", .{@errorName(err)});
            return;
        };

        if (result.owns) allocator.free(result.msg);
    }

    std.debug.print("Handler thread done for socket {}\n", .{stream.handle});
}

fn executeCommand(
    db: *root.Database,
    allocator: std.mem.Allocator,
    line: []const u8,
) !CommandResult {
    root.fuzzAssert(line.len < 4096, "Input command too large");

    var toks = std.mem.tokenizeAny(u8, line, " ");
    const cmd_tok = toks.next() orelse return .{ .msg = "ERR empty\n" };
    const command = root.parseCommand(cmd_tok);

    switch (command) {
        .get => {
            const key = toks.next() orelse return .{ .msg = "ERR missing key\n" };
            if (db.get(key)) |v| {
                var buf = std.array_list.Managed(u8).init(allocator);
                defer buf.deinit();
                const writer = buf.writer();
                try writer.print("{s}: ", .{key});
                root.printValue(v);
                try writer.print("\n", .{});
                const slice = try buf.toOwnedSlice();
                return .{ .msg = slice, .owns = true };
            } else return .{ .msg = "(nil)\n" };
        },
        .set => {
            const key = toks.next() orelse return .{ .msg = "ERR missing key\n" };
            const val = toks.next() orelse return .{ .msg = "ERR missing value\n" };
            try db.setString(key, val);
            return .{ .msg = "OK\n" };
        },
        .del => {
            const key = toks.next() orelse return .{ .msg = "ERR missing key\n" };
            if (try db.del(key)) return .{ .msg = "OK\n" } else return .{ .msg = "(nil)\n" };
        },
        .begin => {
            try db.beginTransaction();
            return .{ .msg = "BEGIN\n" };
        },
        .commit => {
            try db.commit();
            return .{ .msg = "COMMIT\n" };
        },
        .rollback => {
            db.rollback();
            return .{ .msg = "ROLLBACK\n" };
        },
        .help => {
            return .{ .msg = "Commands: GET SET DEL BEGIN COMMIT ROLLBACK EXIT\n" };
        },
        else => return .{ .msg = "ERR unknown command\n" },
    }
}
