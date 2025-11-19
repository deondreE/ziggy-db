const std = @import("std");
const root = @import("root.zig");
const cluster = @import("cluster.zig");

const CommandResult = struct {
    msg: []const u8,
    owns: bool = false,
};

const ClientHandler = struct {
    client_stream: std.net.Stream,
    db: *root.Database,
    cluster: ?cluster.ClusterState,
    allocator: std.mem.Allocator,

    fn handleClient(self: ClientHandler) void {
        var client_out = self.client_stream.writer();
        var client_in = self.client_stream.reader();

        _ = client_out.writeAll("Testing this connection") catch |err| {
            std.debug.print("{s}", .{@errorName(err)});
        };

        _ = client_out.flush() catch |err| {
            std.debug.print("{s}", .{@errorName(err)});
        };

        // Read some data
        var data_buf: [256]u8 = undefined;
        const bytes_read = client_in.readAll(&data_buf) catch |err| {
            std.debug.print("read error: {s}\n", .{@errorName(err)});
            return;
        };

        root.fuzzAssert(bytes_read > 0, "Expected to read some data from client");

        std.debug.print("Received command: {s}\n", .{data_buf[0..bytes_read]});

        _ = client_out.writeAll("Command received\n") catch |err| {
            std.debug.print("write error: {s}\n", .{@errorName(err)});
            return;
        };
        _ = client_out.flush() catch |err| {
            std.debug.print("flush error: {s}\n", .{@errorName(err)});
            return;
        };

        while (true) {
            std.Thread.sleep(1000 * std.time.ns_per_ms);
        }

        // defer self.client_stream.close();
    }
};

const ClusterConnectionHandler = struct {
    conn: std.net.Server,
    cluster: *cluster.ClusterState,
    db: root.Database,
    allocator: std.mem.Allocator,

    fn run(self: ClusterConnectionHandler) void {
        _ = self;
    }
};

pub fn startTcpServer(db: *root.Database, port: u16, allocator: std.mem.Allocator) !void {
    const address = try std.net.Address.parseIp("0.0.0.0", port);
    var listener = try address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    std.debug.print("ZiggyDB TCP server listening on {any}\n", .{address.in});
    std.debug.print("Waiting for clients to connect...\n", .{});

    while (true) {
        const conn = listener.accept() catch |err| {
            std.debug.print("Error accepting client: {any}\n", .{@errorName(err)});
            continue;
        };

        std.debug.print("accepting new connection.\n", .{});

        handleClient(conn.stream, db, allocator);

        std.debug.print("Client connection handled and closed\n", .{});
    }
}

fn handleClient(
    stream_ptr: std.net.Stream,
    db: *root.Database,
    allocator: std.mem.Allocator,
) void {
    defer {
        stream_ptr.close();
        std.debug.print("client stream closed.\n", .{});
    }

    var line_buf = std.array_list.Managed(u8).init(allocator);
    defer line_buf.deinit();

    var reader_buf: [4096]u8 = undefined;
    var writer_buf: [4096]u8 = undefined;

    var reader = stream_ptr.reader(&reader_buf);
    var writer = stream_ptr.writer(&writer_buf).interface;

    while (true) {
        line_buf.clearRetainingCapacity();
        std.Thread.sleep(1 * std.time.ns_per_ms);

        _ = reader.interface_state.streamDelimiter(&writer, '\n') catch |err| switch (err) {
            error.EndOfStream => {
                std.debug.print("handleClient: Client disconneced gracefully.{s}\n", .{@errorName(err)});
                break;
            },
            else => {
                std.debug.print("handleClient: Read error in loop: {s}\n", .{@errorName(err)});
                return;
            },
        };

        const cmd_line = std.mem.trim(u8, line_buf.items, " \r\t");
        root.fuzzAssert(cmd_line.len < 1024, "Cmd too large");
        if (cmd_line.len == 0 or std.ascii.eqlIgnoreCase(cmd_line, "exit")) break;

        const res = executeCommand(db, allocator, cmd_line) catch |err| blk: {
            const msg = @errorName(err);
            root.fuzzAssert(msg.len > 0, "Error message should not be empty");
            break :blk CommandResult{ .msg = msg, .owns = true };
        };

        std.Thread.sleep(1 * std.time.ns_per_ms);

        _ = writer.writeAll(res.msg) catch |err| {
            std.debug.print("handleClient: Result write error: {s}\n", .{@errorName(err)});
            if (res.owns) allocator.free(res.msg);
        };

        std.Thread.sleep(1 * std.time.ns_per_ms);

        _ = writer.flush() catch |err| {
            std.debug.print("handleClient: Result flush error: {s}\n", .{@errorName(err)});
            break;
        };
    }
}

fn executeCommand(
    db: *root.Database,
    allocator: std.mem.Allocator,
    line: []const u8,
) !CommandResult {
    root.fuzzAssert(line.len < 4096, "Input line command is too large");

    var toks = std.mem.tokenizeAny(u8, line, " ");
    const cmd_tok = toks.next() orelse return .{ .msg = "ERR empty\n" };
    const command = root.parseCommand(cmd_tok);

    switch (command) {
        .get => {
            const key = toks.next() orelse return .{ .msg = "ERR missing key\n" };
            if (db.get(key)) |v| {
                var out_buf = std.array_list.Managed(u8).init(allocator);
                defer out_buf.deinit();

                const writer = out_buf.writer();
                try writer.print("{s}: ", .{key});
                root.printValue(v);
                try writer.print("\n", .{});
                const slice = try out_buf.toOwnedSlice();
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
