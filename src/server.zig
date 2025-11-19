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

        const thread_db_ptr = db;
        const thread_allocator = allocator;
        const client_stream_to_move = conn.stream;

        _ = std.Thread.spawn(.{}, struct {
            fn run(
                moved_client_stream: std.net.Stream,
                db_ptr: *root.Database,
                current_allocator: std.mem.Allocator,
            ) void {
                handleClient(moved_client_stream, db_ptr, current_allocator);
            }
        }.run, .{ client_stream_to_move, thread_db_ptr, thread_allocator }) catch |err| {
            std.debug.print("ERORR: failed to spin new process. {any} \n", .{@errorName(err)});
            client_stream_to_move.close();
        };

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

    // var reader_buf: [4096]u8 = undefined;
    var writer_buf: [4096]u8 = undefined;

    var writer = stream_ptr.writer(&writer_buf).interface;

    const MAX_COMMAND_LENGTH = 1024;

    writer.writeAll("Welcome to ZiggyDB!\n") catch |err| {
        std.debug.print("handleClient for {any}: Error writing greeting: {s}\n", .{ "", @errorName(err) });
    };
    writer.flush() catch |err| {
        std.debug.print("handleClient for {any}: Error flushing greeting: {s}\n", .{ "", @errorName(err) });
    };

    var recived_data: [MAX_COMMAND_LENGTH]u8 = undefined;
    while (true) {
        _ = stream_ptr.read(recived_data[0..]) catch |err| switch (err) {
            else => {
                std.debug.print("handleClient: Read error in loop: {s}\n", .{@errorName(err)});
                return;
            },
        };
        const bytes_read = recived_data.len;
        if (bytes_read == 0) {
            std.debug.print("DEBUG: Zig server read 0 bytes. Client likely disconnected.\n", .{});
            break;
        }

        std.debug.print("DEBUG: Zig server read {} bytes. Raw content: {any}\n", .{ bytes_read, recived_data });

        const command_slice = recived_data[0..bytes_read];
        const cmd_line = std.mem.trim(u8, command_slice, " \r\n\t");
        std.debug.print("handleclient: received command: '{s}'\n", .{cmd_line});
        root.fuzzAssert(cmd_line.len < 1024, "cmd too large");
        if (cmd_line.len == 0 or std.ascii.eqlIgnoreCase(cmd_line, "exit")) break;

        const res = executeCommand(db, allocator, cmd_line) catch |err| blk: {
            std.debug.print("handleClient: Error executing command '{s}': '{s}'\n", .{ cmd_line, @errorName(err) });
            const msg = @errorName(err);
            root.fuzzAssert(msg.len > 0, "Error message should not be empty");
            break :blk CommandResult{ .msg = msg, .owns = true };
        };

        _ = writer.writeAll(res.msg) catch |err| {
            std.debug.print("handleClient: Result write error: {s}\n", .{@errorName(err)});
            if (res.owns) allocator.free(res.msg);
            break;
        };

        _ = writer.flush() catch |err| {
            std.debug.print("handleClient: Result flush error: {s}\n", .{@errorName(err)});
            break;
        };
        if (res.owns) allocator.free(res.msg);
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
            std.debug.print("executeCommand: SET successful\n", .{});
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
