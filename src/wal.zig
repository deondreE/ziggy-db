const std = @import("std");

pub const WAL_MAGIC_NUMBER: u32 = 0x5A494747; // "ZIGG" in hex
pub const WAL_VERSION: u16 = 1;
