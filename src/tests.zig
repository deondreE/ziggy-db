const std = @import("std");
const dbmod = @import("main.zig");

test "basic SET/GET/DEL operations" {
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
    try db.beginTransaction();
    try db.set("city", "Austin");
    db.rollback();
    try std.testing.expectEqualStrings("Dallas", db.get("city").?);

    try db.beginTransaction();
    try db.set("city", "Houston");
    try db.commit();
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

    const all = db.lrange("mylist", 0, 10);
    try std.testing.expectEqual(@as(usize, 3), all.len);
    try std.testing.expectEqualStrings("three", all[0]);
    try std.testing.expectEqualStrings("two", all[1]);
    try std.testing.expectEqualStrings("one", all[2]);

    const popped = try db.lpop("mylist");
    try std.testing.expect(popped != null);
    try std.testing.expectEqualStrings("three", popped.?);
    alloc.free(popped.?);

    const after = db.lrange("mylist", 0, 10);
    try std.testing.expectEqual(@as(usize, 2), after.len);
}
