const std = @import("std");
const root = @import("root.zig");

const ClientHandler = struct {
    client_stream: std.net.Stream,
    db: root.Database,
    allocator: std.mem.Allocator,

    fn handleClient(self: ClientHandler) void {
        var read_buf: [1024]u8 = undefined;
        var write_buf: [1024]u8 = undefined;
        var client_out = self.client_stream.writer(&write_buf);
        var client_in = self.client_stream.reader(&read_buf);

        // _ = client_in;
        _ = client_out.interface.write("Testing this connection") catch |err| {
            std.debug.print("{s}", .{@errorName(err)});
        };

        // User: SET <Key> <Value>, GET <Key> <Value>
        _ = client_out.interface.flush() catch |err| {
            std.debug.print("{s}", .{@errorName(err)});
        };

        const data = client_in.interface_state.buffered();

        std.debug.print("Received command: {any}\n", .{data.ptr});

        _ = client_out.interface.write("Command received\n") catch |err| {
            std.debug.print("write error: {s}\n", .{@errorName(err)});
        };
        _ = client_out.interface.flush() catch |err| {
            std.debug.print("flush error: {s}\n", .{@errorName(err)});
        };

        while (true) {
            std.Thread.sleep(1000);
        }

        self.client_stream.close();
    }
};

pub fn startTcpServer(db: root.Database, port: u16, allocator: std.mem.Allocator) !void {
    const address = try std.net.Address.parseIp("0.0.0.0", port);
    var listener = address.listen(.{ .reuse_address = true }) catch |err| {
        std.debug.print("Error listening on port {d}: {any}\n", .{ port, @errorName(err) });
        return err;
    };
    defer listener.deinit();

    std.debug.print("ZiggyDB TCP server listening on {any}\n", .{address.in});
    std.debug.print("Waiting for clients to connect...\n", .{});

    var threads = std.array_list.Managed(std.Thread).init(allocator);
    defer {
        for (threads.items) |thread| {
            thread.join();
            threads.deinit();
        }
    }

    while (true) {
        const client_stream = listener.accept() catch |err| {
            std.debug.print("Error accepting client: {any}\n", .{@errorName(err)});
            continue;
        };

        // std.debug.print("Client connected from {any} \n", .{client_stream.address.in});

        const handler = ClientHandler{
            .client_stream = client_stream.stream,
            .db = db,
            .allocator = allocator,
        };

        const thread = try std.Thread.spawn(.{}, ClientHandler.handleClient, .{handler});
        try threads.append(thread);
    }
}
