const std = @import("std");
const ArrayList = std.ArrayList;
const StringHashMap = std.StringHashMap;
const Allocator = std.mem.Allocator;

pub const DataType = enum {
    Integer,
    Text,
    Real,
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

    pub fn format(self: Value, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        switch (self) {
            .Integer => |v| try writer.print("{d}", .{v}),
            .Text => |v| try writer.print("{s}", .{v}),
            .Real => |v| try writer.print("{d:.2}", .{v}),
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
        };
    }
};

/// Row in a table.
pub const Row = struct {
    values: ArrayList(Value),
    allocator: Allocator,

    pub fn init(allocator: Allocator) Row {
        return .{
            .values = std.array_list.Managed(Value).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Row) void {
        for (self.values.items) |val| {
            if (val == .Text) {
                self.allocator.free(val.Text);
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
            if (key == .Text) {
                self.allocator.free(key.Text);
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

pub const Tree = struct {
    root: *Node,
    allocator: Allocator,
    pk: usize,

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
};

pub const Table = struct {
    name: []const u8,
    columns: ArrayList(Column),
    rows: ArrayList(Row),
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8) !Table {
        return .{
            .name = try allocator.dupe(u8, name),
            .columns = ArrayList(Column).init(allocator),
            .rows = ArrayList(Row).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Table) void {
        self.allocator.free(self.name);
        for (self.columns.items) |col| {
            self.allocator.free(col.name);
        }
        self.columns.deinit();
        for (self.rows.items) |row| {
            row.deinit();
        }
        self.rows.deinit();
    }

    pub fn addColumn(self: *Table, name: []const u8, data_type: DataType, is_pk: bool) !void {
        const col = Column{
            .name = try self.allocator.dupe(u8, name),
            .data_type = data_type,
            .is_pk = is_pk,
        };
        try self.columns.append(col);
    }

    pub fn insertRow(self: *Table, values: []const Value) !void {
        if (values.len != self.columns.items.len) {
            return error.ColumnCountMismatch;
        }

        var row = Row.init(self.allocator);
        for (values) |val| {
            const copied_val = switch (val) {
                .Text => |txt| Value{ .Text = try self.allocator.dupe(u8, txt) },
                else => val,
            };
            try row.addValue(copied_val);
        }
        try self.rows.append(row);
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
