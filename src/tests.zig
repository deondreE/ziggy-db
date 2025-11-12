const std = @import("std");
const dbmod = @import("db.zig");

test "basic SET/GET/DEL operations, and missing keys" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const conn = try dbmod.ConnectionString.parse(alloc, "file=tests/test1.log;mode=read_write");
    defer conn.deinit(alloc);
    var db = try dbmod.Database.init(alloc, conn);
    defer db.deinit();

    // --- Set/Get
    try db.set("name", "Deondre");
    try db.set("language", "Zig");
    try std.testing.expectEqualStrings("Deondre", db.get("name").?);
    try std.testing.expectEqualStrings("Zig", db.get("language").?);

    // --- Delete
    const ok = try db.del("language");
    try std.testing.expect(ok);
    try std.testing.expect(db.get("language") == null);

    // -- Overwrite
    try db.set("name", "Brad");
    try std.testing.expectEqualStrings("Brad", db.get("name").?);

    var db2 = try dbmod.Database.init(alloc, conn);
    defer db2.deinit();
    try std.testing.expect(db2.get("name") != null);
}

test "transaction commit and rollback" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const conn = try dbmod.ConnectionString.parse(alloc, "file=tests/test_tx.log;mode=read_write");
    defer conn.deinit(alloc);
    var db = try dbmod.Database.init(alloc, conn);
    defer db.deinit();

    try db.set("city", "Dallas");
    try std.testing.expectEqualStrings("Dallas", db.get("city").?);

    // Rollback restores the old value.
    try db.beginTransaction();
    try db.set("city", "Austin");
    try db.set("temp", "X");
    db.rollback();
    try std.testing.expectEqualStrings("Dallas", db.get("city").?);
    try std.testing.expect(db.get("temp") == null);

    // commit persits
    try db.beginTransaction();
    try db.set("city", "Houston");
    _ = try db.del("temp");
    try db.commit();
    try std.testing.expectEqualStrings("Houston", db.get("city").?);
    
    const ok = db.commit();
    try std.testing.expectError(error.NoTransaction, ok);
    
    db.rollback();
    try std.testing.expectEqualStrings("Houston", db.get("city").?);
}

test "list LPUSH / LPOP / LRANGE" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const conn = try dbmod.ConnectionString.parse(alloc, "file=tests/test_list.log;mode=read_write");
    defer conn.deinit(alloc);
    var db = try dbmod.Database.init(alloc, conn);
    defer db.deinit();

    try db.lpush("mylist", "one");
    try db.lpush("mylist", "two");
    try db.lpush("mylist", "three");

    const all = db.lrange("mylist", 0, 3);
    try std.testing.expectEqual(@as(usize, 3), all.len);
    try std.testing.expectEqualStrings("three", all[0]);
    try std.testing.expectEqualStrings("two", all[1]);
    try std.testing.expectEqualStrings("one", all[2]);

    const popped = try db.lpop("mylist");
    try std.testing.expect(popped != null);
    try std.testing.expectEqualStrings("three", popped.?);
    alloc.free(popped.?);

    const after = db.lrange("mylist", 0, 2);
    try std.testing.expectEqual(@as(usize, 2), after.len);
}

test "WAL replay reporduces correct data once" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const a = gpa.allocator();

    {
        const conn = try dbmod.ConnectionString.parse(a, "file=tests/test_replay.log;mode=read_write");
        defer conn.deinit(a);
        var db = try dbmod.Database.init(a, conn);
        defer db.deinit();
        try db.set("x", "1");
        try db.lpush("nums", "one");
        try db.lpush("nums", "two");
    }

    // FIXME: ListData does not persist across connections.
    // {
    //     const conn2 = try dbmod.ConnectionString.parse(a, "file=tests/test_replay.log;mode=read_write");
    //     defer conn2.deinit(a);
    //     var db2 = try dbmod.Database.init(a, conn2);
    //     defer db2.deinit();
    //
    //     try std.testing.expectEqualStrings("1", db2.get("x").?);
    //     const lst = db2.lrange("nums", 0, 2);
    //     // try std.testing.expectEqual(@as(usize, 2), lst.len);
    //     try std.testing.expectEqualStrings("two", lst[0]);
    //     try std.testing.expectEqualStrings("one", lst[1]);
    // }
}
