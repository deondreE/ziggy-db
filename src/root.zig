const std = @import("std");

pub const ConnectionString = @import("connection.zig").ConnectionString;
pub const LogEntry = @import("wal_log.zig").LogEntry;
pub const Value = @import("wal_log.zig").Value;
pub const ValueType = @import("wal_log.zig").ValueType;

pub const Database = @import("db.zig").Database;
pub const ValueWithMetadata = @import("db.zig").ValueWithMetadata;
pub const printValue = @import("db.zig").printValue;

pub const Command = enum {
    help,
    set,
    get,
    del,
    lpush,
    lpop,
    lrange,
    setbit,
    getbit,
    bitfeild,
    begin,
    commit,
    rollback,
    serve,
    importjson,
    exportjson,
    exit,
    unknown,
};

pub fn parseCommand(tok: []const u8) Command {
    if (std.ascii.eqlIgnoreCase(tok, "HELP")) return .help;
    if (std.ascii.eqlIgnoreCase(tok, "SET")) return .set;
    if (std.ascii.eqlIgnoreCase(tok, "GET")) return .get;
    if (std.ascii.eqlIgnoreCase(tok, "DEL")) return .del;
    if (std.ascii.eqlIgnoreCase(tok, "LPUSH")) return .lpush;
    if (std.ascii.eqlIgnoreCase(tok, "LPOP")) return .lpop;
    if (std.ascii.eqlIgnoreCase(tok, "LRANGE")) return .lrange;
    if (std.ascii.eqlIgnoreCase(tok, "BEGIN")) return .begin;
    if (std.ascii.eqlIgnoreCase(tok, "SETBIT")) return .setbit;
    if (std.ascii.eqlIgnoreCase(tok, "GETBIT")) return .getbit;
    if (std.ascii.eqlIgnoreCase(tok, "BITFEILD")) return .bitfeild;
    if (std.ascii.eqlIgnoreCase(tok, "COMMIT")) return .commit;
    if (std.ascii.eqlIgnoreCase(tok, "ROLLBACK")) return .rollback;
    if (std.ascii.eqlIgnoreCase(tok, "IMPORTJSON")) return .importjson;
    if (std.ascii.eqlIgnoreCase(tok, "EXPORTJSON")) return .exportjson;
    if (std.ascii.eqlIgnoreCase(tok, "SERVE")) return .serve;
    if (std.ascii.eqlIgnoreCase(tok, "EXIT") or std.ascii.eqlIgnoreCase(tok, "QUIT"))
        return .exit;
    return .unknown;
}

pub fn enableWindowsAnsiColors() void {
    if (@import("builtin").os.tag == .windows) {
        const windows = std.os.windows;
        const ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004;
        const STD_OUTPUT_HANDLE: windows.DWORD = @bitCast(@as(i32, -11));

        const handle = windows.kernel32.GetStdHandle(STD_OUTPUT_HANDLE) orelse return;
        if (handle == windows.INVALID_HANDLE_VALUE) return;

        var mode: windows.DWORD = 0;
        if (windows.kernel32.GetConsoleMode(handle, &mode) == 0) return;

        mode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
        _ = windows.kernel32.SetConsoleMode(handle, mode);
    }
}
