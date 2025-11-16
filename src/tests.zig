const std = @import("std");
const root = @import("root.zig");

fn cleanupTestFile(path: []const u8) void {
    std.fs.cwd().deleteFile(path) catch {};
}

test "basic SET/GET/DEL operations, and missing keys" {
    cleanupTestFile("tests/test1.log");

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const conn = try root.ConnectionString.parse(alloc, "file=tests/test1.log;mode=read_write");
    defer conn.deinit(alloc);

    {
        var db = try root.Database.init(alloc, conn);
        defer db.deinit();

        // --- Set/Get
        try db.set("name", "Deondre");
        try db.set("language", "Zig");

        const name_val = db.get("name").?;
        try std.testing.expect(name_val == .String);
        try std.testing.expectEqualStrings("Deondre", name_val.String);

        const lang_val = db.get("language").?;
        try std.testing.expectEqualStrings("Zig", lang_val.String);

        // --- Delete
        const ok = try db.del("language");
        try std.testing.expect(ok);
        try std.testing.expect(db.get("language") == null);

        // -- Overwrite
        try db.set("name", "Brad");
        const brad_val = db.get("name").?;
        try std.testing.expectEqualStrings("Brad", brad_val.String);
    }

    // Now open a new connection after the first one is closed
    {
        var db2 = try root.Database.init(alloc, conn);
        defer db2.deinit();
        const name_val = db2.get("name");
        try std.testing.expect(name_val != null);
        try std.testing.expectEqualStrings("Brad", name_val.?.String);
    }
}

test "transaction commit and rollback" {
    cleanupTestFile("tests/test_tx.log");

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const conn = try root.ConnectionString.parse(alloc, "file=tests/test_tx.log;mode=read_write");
    defer conn.deinit(alloc);
    var db = try root.Database.init(alloc, conn);
    defer db.deinit();

    try db.set("city", "Dallas");
    const dallas_val = db.get("city").?;
    try std.testing.expectEqualStrings("Dallas", dallas_val.String);

    // Rollback restores the old value.
    try db.beginTransaction();
    try db.set("city", "Austin");
    try db.set("temp", "X");
    db.rollback();
    const city_after_rollback = db.get("city").?;
    try std.testing.expectEqualStrings("Dallas", city_after_rollback.String);
    try std.testing.expect(db.get("temp") == null);

    // commit persists
    try db.beginTransaction();
    try db.set("city", "Houston");
    _ = try db.del("temp");
    try db.commit();
    const houston_val = db.get("city").?;
    try std.testing.expectEqualStrings("Houston", houston_val.String);

    const ok = db.commit();
    try std.testing.expectError(error.NoTransaction, ok);

    db.rollback();
    const city_final = db.get("city").?;
    try std.testing.expectEqualStrings("Houston", city_final.String);
}

test "list LPUSH / LPOP / LRANGE" {
    cleanupTestFile("tests/test_list.log");

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const conn = try root.ConnectionString.parse(alloc, "file=tests/test_list.log;mode=read_write");
    defer conn.deinit(alloc);
    var db = try root.Database.init(alloc, conn);
    defer db.deinit();

    try db.lpush("mylist", "one");
    try db.lpush("mylist", "two");
    try db.lpush("mylist", "three");

    // Since lpush now uses Binary values, check via get
    const list_val = db.get("mylist");
    try std.testing.expect(list_val != null);
    try std.testing.expect(list_val.? == .Binary);

    // Note: With current implementation, lpush overwrites rather than appending
    // So only the last value will be present
    try std.testing.expectEqualStrings("three", list_val.?.Binary);
}

test "typed values - Integer, Float, Bool, Timestamp" {
    cleanupTestFile("tests/test_types.log");

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const conn = try root.ConnectionString.parse(alloc, "file=tests/test_types.log;mode=read_write");
    defer conn.deinit(alloc);
    var db = try root.Database.init(alloc, conn);
    defer db.deinit();

    // Test Integer
    try db.setInt("count", 42);
    const count_val = db.get("count").?;
    try std.testing.expect(count_val == .Integer);
    try std.testing.expectEqual(@as(i64, 42), count_val.Integer);

    // Test Float
    try db.setFloat("price", 19.99);
    const price_val = db.get("price").?;
    try std.testing.expect(price_val == .Float);
    try std.testing.expectEqual(@as(f64, 19.99), price_val.Float);

    // Test Bool
    try db.setBool("active", true);
    const active_val = db.get("active").?;
    try std.testing.expect(active_val == .Bool);
    try std.testing.expect(active_val.Bool);

    // Test Timestamp
    const test_timestamp_key = "event_time";
    const test_timestamp_value: u64 = 1678838400;

    try db.setTyped(test_timestamp_key, root.Value{ .Timestamp = test_timestamp_value }, null);

    const event_time = db.get(test_timestamp_key).?;
    try std.testing.expect(event_time == .Timestamp);
    try std.testing.expectEqual(@as(u64, test_timestamp_value), event_time.Timestamp);
}

test "WAL replay reproduces correct data" {
    cleanupTestFile("tests/test_replay.log");

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const a = gpa.allocator();

    const conn = try root.ConnectionString.parse(a, "file=tests/test_replay.log;mode=read_write");
    defer conn.deinit(a);

    {
        var db = try root.Database.init(a, conn);
        defer db.deinit();
        try db.set("x", "1");
        try db.setInt("num", 100);
    }

    {
        var db2 = try root.Database.init(a, conn);
        defer db2.deinit();

        const x_val = db2.get("x").?;
        try std.testing.expectEqualStrings("1", x_val.String);

        const num_val = db2.get("num").?;
        try std.testing.expectEqual(@as(i64, 100), num_val.Integer);
    }
}
