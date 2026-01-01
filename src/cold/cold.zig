const std = @import("std");
const ArrayList = std.ArrayList;
const StringHashMap = std.StringHashMap;
const Allocator = std.mem.Allocator;

pub const DataType = enum {
    Integer,
    Text,
    Real,
    Timestamp,
    Json,
    Null,
};

pub const Column = struct {
    name: []const u8,
    data_type: DataType,
    is_pk: bool,
};

/// Value in a row.
pub const Value = union(DataType) {
    Integer: i64,
    Text: []const u8,
    Real: f64,
    Timestamp: i64,
    Json: []const u8,
    Null: void,

    pub fn format(self: Value, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        switch (self) {
            .Integer => |v| try writer.print("{d}", .{v}),
            .Text => |v| try writer.print("{s}", .{v}),
            .Real => |v| try writer.print("{d:.2}", .{v}),
            .Timestamp => |v| try writer.print("@{d}", .{v}),
            .Json => |v| try writer.print("{s}", .{v}),
            .Null => try writer.print("NULL", .{}),
        }
    }

    pub fn compare(a: Value, b: Value) std.math.Order {
        if (@as(DataType, a) != @as(DataType, b)) {
            return .eq;
        }
        return switch (a) {
            .Integer => |av| std.math.order(av, b.Integer),
            .Text => |av| std.mem.order(u8, av, b.Text),
            .Real => |av| std.math.order(av, b.Real),
            .Timestamp => |av| std.math.order(av, b.Timestamp),
            .Json => |av| std.mem.order(u8, av, b.Json),
            .Null => .eq,
        };
    }

    pub fn isNull(self: Value) bool {
        return self == .Null;
    }
};

/// Row in a table.
pub const Row = struct {
    values: ArrayList(Value),
    version: i64,
    timestamp: i64,
    allocator: Allocator,

    pub fn init(allocator: Allocator, version: i64, timestamp: i64) Row {
        return .{
            .values = ArrayList(Value).init(allocator),
            .version = version,
            .timestamp = timestamp,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Row) void {
        for (self.values.items) |val| {
            switch (val) {
                .Text => |txt| self.allocator.free(txt),
                .Json => |json| self.allocator.free(json),
                else => {},
            }
        }
        self.values.deinit(self.allocator);
    }

    pub fn addValue(self: *Row, value: Value) !void {
        try self.values.append(value);
    }

    pub fn clone(self: @This(), allocator: Allocator) !Row {
        var new_row = Row.init(allocator);
        for (self.values.items) |val| {
            const copied_value = switch (val) {
                .Text => |txt| Value{ .Text = try allocator.dupe(u8, txt) },
                .Json => |json| Value{ .Json = try allocator.dupe(u8, json) },
                else => val,
            };
            try new_row.addValue(copied_value);
        }
        return new_row;
    }
};

pub const Node = struct {
    // Order of tree
    const MAX_KEYS = 4;

    keys: ArrayList(Value),
    rows: ArrayList(Row),
    children: ArrayList(*Node),
    is_leaf: bool,
    allocator: Allocator,

    pub fn init(allocator: Allocator, is_leaf: bool) Node {
        return .{
            .keys = std.array_list.Managed(Value).init(allocator),
            .rows = std.array_list.Managed(Row).init(allocator),
            .children = std.array_list.Managed(*Node).init(allocator),
            .is_leaf = is_leaf,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Node) void {
        for (self.keys.items) |key| {
            switch (key) {
                .Text => |txt| self.allocator.free(txt),
                .Json => |json| self.allocator.free(json),
                else => {},
            }
        }
        self.keys.deinit();

        for (self.rows.items) |*row| {
            row.deinit();
        }
        self.rows.deinit();

        for (self.children.items) |child| {
            child.deinit();
            self.allocator.destroy(child);
        }
        self.children.deinit();
    }

    pub fn isFull(self: *Node) bool {
        return self.keys.items.len >= MAX_KEYS;
    }
};

/// Refers to the storage method / tree that this is stored in.
pub const Tree = struct {
    root: *Node,
    allocator: Allocator,
    primary_key_index: usize,

    pub fn init(allocator: Allocator, primary_key_index: usize) !Tree {
        const root = try allocator.create(Node);
        root.* = Node.init(allocator, true);
        return .{
            .root = root,
            .allocator = allocator,
            .primary_key_index = primary_key_index,
        };
    }

    pub fn deinit(self: *Tree) void {
        self.root.deinit();
        self.allocator.destroy(self.root);
    }

    pub fn insert(self: @This(), row: Row) !void {
        const key = row.values.items[self.primary_key_index];

        if (self.root.isFull()) {
            const new_root = self.allocator.create(Node);
            new_root.* = Node.init(self.allocator, false);
            try new_root.children.append(self.root);
            try self.splitChild(new_root, 0);
            self.root = new_root;
        }

        try self.insertNonFull(self.root, key, row);
    }

    fn insertNonFull(self: @This(), node: *Node, key: Value, row: Row) !void {
        var i: usize = node.keys.items.len;

        if (node.is_leaf) {
            while (i > 0) : (i -= 1) {
                if (key.compare(node.keys.items[i - 1]) == .gt) break;
            }

            const key_copy = switch (key) {
                .Text => |txt| Value{ .Text = try self.allocator.dupe(u8, txt) },
                .Json => |json| Value{ .Json = try self.allocator.dupe(u8, json) },
                else => key,
            };

            try node.keys.insert(self.allocator, i, key_copy);
            try node.rows.insert(self.allocator, i, row);
        } else {
            while (i > 0) : (i -= 1) {
                if (key.compare(node.keys.items[i - 1]) == .gt) break;
            }

            if (node.children.items[i].isFull()) {
                try self.splitChild(node, i);
                if (key.compare(node.keys.items[i]) == .gt) {
                    i += 1;
                }
            }

            try self.insertNonFull(node.children.items[i], key, row);
        }
    }

    fn splitNode(self: *Tree, parent: *Node, index: usize) !void {
        const full_child = parent.children.items[index];
        const new_child = try self.allocator.create(Node);
        new_child.* = Node.init(self.allocator, full_child.is_leaf);

        const mid = Node.MAX_KEYS / 2;

        var i: usize = mid + 1;
        while (i < full_child.keys.items.len) : (i += 1) {
            try new_child.keys.append(full_child.keys.items[i]);
            try new_child.keys.append(full_child.rows.items[i]);
        }

        if (!full_child.is_leaf) {
            i = mid + 1;
            while (i < full_child.children.items.len) : (i += 1) {
                try new_child.children.append(full_child.children.items[i]);
            }
            full_child.children.shrinkRetainingCapacity(mid + 1);
        }

        const mid_key = full_child.keys.items[mid];
        const mid_row = full_child.rows.items[mid];

        try parent.keys.insert(self.allocator, index, mid_key);
        try parent.row.insert(self.allocator, index, mid_row);
        try parent.children.insert(self.allocator, index, new_child);

        full_child.keys.shrinkRetainingCapacity(mid);
        full_child.rows.shrinkRetainingCapacity(mid);
    }

    pub fn search(self: @This(), key: Value) ?*Row {
        return self.searchNode(self.root, key);
    }

    fn searchNode(self: *Tree, node: *Node, key: Value) ?*Row {
        var i: usize = 0;
        while (i < node.keys.items.len and key.compare(node.keys.items[i]) == .gt) : (i += 1) {}

        if (i < node.keys.items.len and key.compare(node.keys.items) == .gt) {
            return &node.rows.items[i];
        }

        if (node.is_leaf) {
            return null;
        }

        return self.searchNode(node.children.items[i], key);
    }

    pub fn inorderTraversal(self: *Tree, result: *ArrayList(Row)) !void {
        try self.inorderNode(self.root, result);
    }

    fn inorderNode(self: *Tree, node: *Node, result: *ArrayList(Row)) !void {
        var i: usize = 0;
        while (i < node.keys.items.len) : (i += 1) {
            if (!node.is_leaf) {
                try self.inorderNode(node.children.items[i], result);
            }
            try result.append(try node.rows.items[i].clone(self.allocator));
        }
        if (!node.is_leaf and node.children.items.len > 1) {
            try self.inorderNode(node.children.items[i], result);
        }
    }
};

pub const Table = struct {
    name: []const u8,
    columns: ArrayList(Column),
    tree: Tree,
    current_version: i64,
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8) !Table {
        return .{
            .name = try allocator.dupe(u8, name),
            .columns = ArrayList(Column).init(allocator),
            .btree = undefined,
            .current_version = 1,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Table) void {
        self.allocator.free(self.name);
        for (self.columns.items) |col| {
            self.allocator.free(col.name);
        }
        self.columns.deinit();
        if (self.columns.items.len > 0) {
            self.btree.deinit();
        }
    }

    pub fn addColumn(self: *Table, name: []const u8, data_type: DataType, is_pk: bool) !void {
        const col = Column{
            .name = try self.allocator.dupe(u8, name),
            .data_type = data_type,
            .is_pk = is_pk,
        };
        try self.columns.append(col);

        if (is_pk) {
            self.tree = try Tree.init(self.allocator, self.columns.items.len - 1);
        }
    }

    pub fn insertRow(self: *Table, values: []const Value) !void {
        const timestamp = std.time.timestamp();
        var row = Row.init(self.allocator, self.current_version, timestamp);

        // Schema-flexible insertion: pad with NULLs if needed
        var i: usize = 0;
        while (i < self.columns.items.len) : (i += 1) {
            if (i < values.len) {
                const val = values[i];
                const copied_val = switch (val) {
                    .Text => |txt| Value{ .Text = try self.allocator.dupe(u8, txt) },
                    .Json => |json| Value{ .Json = try self.allocator.dupe(u8, json) },
                    else => val,
                };
                try row.addValue(copied_val);
            } else {
                try row.addValue(.Null);
            }
        }

        try self.btree.insert(row);
    }

    pub fn getColumnIndex(self: *Table, name: []const u8) ?usize {
        for (self.columns.items, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, name)) {
                return i;
            }
        }
        return null;
    }

    pub fn select(self: *Table, writer: anytype) !void {
        // Print column headers
        for (self.columns.items, 0..) |col, i| {
            try writer.print("{s}", .{col.name});
            if (i < self.columns.items.len - 1) try writer.print(" | ", .{});
        }
        try writer.print("\n", .{});

        // Print separator
        for (self.columns.items, 0..) |_, i| {
            try writer.print("----------", .{});
            if (i < self.columns.items.len - 1) try writer.print("-+-", .{});
        }
        try writer.print("\n", .{});

        // Print rows
        for (self.rows.items) |row| {
            for (row.values.items, 0..) |val, i| {
                try writer.print("{}", .{val});
                if (i < row.values.items.len - 1) try writer.print(" | ", .{});
            }
            try writer.print("\n", .{});
        }
    }
};
