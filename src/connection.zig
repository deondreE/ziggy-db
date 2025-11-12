const std = @import("std");

pub const ConnectionString = struct {
    file_path: []const u8,
    mode: []const u8,

    pub const ParseError = error{
        InvalidFormat,
        MissingFilePath,
        DuplicateParameter,
    };

    // Parses a Connection string like "file=my_kv_store.log;mode=read_write"
    pub fn parse(allocator: std.mem.Allocator, connectionStr: []const u8) !ConnectionString {
        var parsed_file_path: ?[]const u8 = null;
        var parsed_mode: ?[]const u8 = null;

        var tokens = std.mem.tokenizeAny(u8, connectionStr, ";");
        while (tokens.next()) |token| {
            var parts = std.mem.tokenizeAny(u8, token, "=");
            const key = parts.next() orelse return ParseError.InvalidFormat;
            const value = parts.next() orelse return ParseError.InvalidFormat;

            if (parts.next() != null) return ParseError.InvalidFormat;

            if (std.mem.eql(u8, key, "file")) {
                if (parsed_file_path != null) return ParseError.DuplicateParameter;
                parsed_file_path = try allocator.dupe(u8, value);
            } else if (std.mem.eql(u8, key, "mode")) {
                if (parsed_mode != null) return ParseError.DuplicateParameter;
                parsed_mode = try allocator.dupe(u8, value);
            } else {
                std.debug.print("Unknown parameter: {s}\n", .{key});
            }
        }

        if (parsed_file_path == null) return ParseError.MissingFilePath;

        // Default to read_write mode if not specified
        const mode = if (parsed_mode) |m| m else try allocator.dupe(u8, "read_write");

        return ConnectionString{
            .file_path = parsed_file_path.?,
            .mode = mode,
        };
    }

    pub fn deinit(self: ConnectionString, allocator: std.mem.Allocator) void {
        allocator.free(self.file_path);
        allocator.free(self.mode);
    }
};
