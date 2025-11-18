const std = @import("std");
const root = @import("root.zig");
const tls = std.crypto.tls;

pub const GOSSIP_PORT: u16 = 9333;
pub const CLUSTER_DEFAULT_PORT: u16 = 6001;

pub const NodeRole = enum {
    Leader,
    Follower,
    Canidate,
};

pub const NodeInfo = struct {
    id: []const u8,
    addr: []const u8, // "ip:port"
    last_heartbeat: i64,
    online: bool,
};

pub const PersistentState = struct {
    current_term: u64 = 0,
    voted_for: ?[]const u8 = null,
    commit_index: u64 = 0,
    last_applied: u64 = 0,

    /// Save persistent data to an intermediate file, for reading by the node thread.
    pub fn save(self: *PersistentState, dir: std.fs.Dir, allocator: std.mem.Allocator) !void {
        var file = try dir.createFile("cluster_meta.json", .{ .truncate = true });
        defer file.close();

        const encoded = try std.json.Stringify.valueAlloc(allocator, self, .{});
        defer allocator.free(encoded);
        try file.writeAll(encoded);
        try file.sync();
    }

    pub fn load(allocator: std.mem.Allocator, dir: std.fs.Dir) !PersistentState {
        var file = try dir.openFile("cluster_meta.json", .{}) catch |err| switch (err) {
            error.FileNotFound => return PersistentState{},
            else => return err,
        };
        defer file.close();

        const data = try file.readToEndAlloc(allocator, std.math.maxInt(usize));
        defer allocator.free(data);
        const parsed = try std.json.parseFromSlice(PersistentState, allocator, data, .{});
        return parsed.value;
    }
};

/// RPC request and response.
pub const AppendEntriesReq = struct {
    term: u64,
    leader_id: []const u8,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: []const u8,
    leader_commit: u64,
};

pub const AppendEntriesResp = struct {
    term: u64,
    success: bool,
    match_index: u64,
    follower_id: []const u8,
};

pub const ClusterState = struct {
    allocator: std.mem.Allocator,
    dir: std.fs.Dir,

    peers: std.StringHashMap(NodeInfo),
    self_node: NodeInfo,

    role: NodeRole = .Follower,
    term: u64 = 0,
    voted_for: ?[]const u8 = null,
    commit_index: u64 = 0,
    last_applied: u64 = 0,
    leader_id: ?[]const u8 = null,

    cluster_meta: PersistentState,

    pub fn init(allocator: std.mem.Allocator, base_dur: []const u8, self_addr: []const u8) !ClusterState {
        var cwd = std.fs.cwd();
        const cluster_dir = try cwd.makeOpenPath(base_dur, .{});
        const meta = try PersistentState.load(allocator, cluster_dir);

        var id_buf: [16]u8 = undefined;
        try std.crypto.random(&id_buf);
        const id = std.fmt.allocPrint(allocator, "{x:0>32}", .{id_buf});

        return ClusterState{
            .allocator = allocator,
            .dir = cluster_dir,
            .peers = std.StringHashMap(NodeInfo).init(allocator),
            .self_node = NodeInfo{
                .id = id,
                .addr = try allocator.dupe(u8, self_addr),
                .online = true,
                .last_heartbeat = @intCast(std.time.timestamp()),
            },
            .term = meta.current_term,
            .voted_for = meta.voted_for,
            .commit_index = meta.commit_index,
            .last_applied = meta.last_applied,
            .cluster_meta = meta,
        };
    }

    pub fn deinit(self: *ClusterState) void {
        self.peers.deinit();
        self.allocator.free(self.self_node.id);
        self.allocator.free(self.self_node.addr);
        self.dir.close();
    }

    /// Add peer manually.
    pub fn addPeer(self: *ClusterState, addr: []const u8) !void {
        if (std.mem.eql(u8, addr, self.self_node.addr)) return;
        const addr_copy = try self.allocator.dupe(u8, addr);

        var id_buf: [16]u8 = undefined;
        try std.crypto.random(&id_buf);
        const id = try std.fmt.allocPrint(self.allocator, "{x:0>32}", .{id_buf});

        try self.peers.put(addr_copy, NodeInfo{
            .id = id,
            .addr = addr_copy,
            .last_heartbeat = 0,
            .online = false,
        });
    }

    /// Try load peers from config file; fallback to gossip outdated.
    pub fn discover(self: *ClusterState, path: []const u8) !void {
        if (std.fs.cwd().access(path, .{}) catch |err| err == error.FileNotFound) {
            std.debug.print("[CLUSTER] Config file not found - using gossip autodiscovery...");
            _ = std.Thread.spawn(.{}, startGossipDiscovery, .{self}) catch |err| {
                std.debug.print("[GOSSIP] Failed to spawn discovery thread: {any}\n", .{err});
            };
            return;
        }

        var file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        var buf: [256]u8 = undefined;
        while (try file.readAll(&buf)) |line| {
            const trimmed = std.mem.trim(&buf, line, "\r\t");
            if (trimmed.len == 0 or trimmed[0] == '#') continue;
            if (std.mem.startsWith(u8, trimmed, "peer")) {
                const addr = std.mem.trim(u8, trimmed[4..], " =");
                try self.addPeer(addr);
            }
        }

        std.debug.print("[CLUSTER] Config discovery complete ({d} peers)", .{self.peers.count()});
    }

    /// Gossip discovery (runs in background)
    fn startGossipDiscovery(self: *ClusterState) !void {
        _ = self;
    }

    /// Load a cluster configuration from a file.
    pub fn loadConfig(allocator: std.mem.Allocator, config_path: []const u8) !ClusterState {
        var file = try std.fs.cwd().openFile(config_path, .{});
        defer file.close();

        var write_buffer: [1024]u8 = undefined;
        var buffer: [1024]u8 = undefined;
        var reader = file.reader(&buffer);
        var writer = file.writer(&write_buffer);

        var self_addr: []const u8 = "";
        var peers = std.ArrayList([]const u8);
        defer peers.deinit();

        while (try reader.interface.streamDelimiter(&writer, "\n")) |line| {
            const trimmed = std.mem.trim(u8, line, "\t\r");
            if (trimmed.len == 0) continue;

            if (std.mem.startsWith(u8, trimmed, "#")) continue;
            if (std.mem.startsWith(u8, trimmed, "node_addr")) {
                self_addr = std.mem.trim(u8, trimmed[10..], " =");
            } else if (std.mem.startsWith(u8, trimmed, "node_id")) {
                const p = std.mem.trim(u8, trimmed[4..], " =");
                try peers.append(p);
            }
        }

        var cluster = try ClusterState.init(allocator, self_addr);

        for (peers.items) |peer| {
            try cluster.addPeer(peer);
        }

        return cluster;
    }

    fn runElection(self: *ClusterState) !void {
        if (self.role == .Leader) return;

        self.term += 1;
        self.role = .Canidate;
        self.voted_for = self.self_node.id;

        try self.cluster_meta.save(self.dir, self.allocator);

        std.debug.print("[CLUSTER] Starting election for term {d} \n", .{self.term});

        var highest_id = self.self_node.id;
        const it = self.peers.iterator();
        while (it.next()) |entry| {
            const peer = entry.value_ptr.*;
            if (peer.online and std.mem.lessThan(u8, peer.id, highest_id)) {
                highest_id = peer.id;
            }
        }

        if (std.mem.eql(u8, highest_id, self.self_node.id)) {
            std.debug.print("[RAFT] Node {s} became leader\n", .{self.self_node.id});
            self.role = .Leader;
            self.leader_id = self.self_node.id;
            return;
        }
    }

    /// Runs the heartbeat loop for the cluster.
    fn heartbeat(self: *ClusterState) !void {
        if (self.role != .Leader) return;
        var it = self.peers.iterator();
        while (it.next()) |entry| {
            const peer = entry.value_ptr.*;
            if (!peer.online) continue;
            _ = sendAppendEntries(self, peer.addr, null) catch |err| {
                std.debug.print("[WARN] heartbeat to {s} failed: {any} \n", .{ peer.addr, err });
            };
        }
    }

    fn sendAppendEntries(
        cluster: *ClusterState,
        addr: []const u8,
        opt_entry: ?root.LogEntry,
    ) !void {
        var conn = try std.net.tcpConnectToHost(addr, "test", 3_000_000_000);
        defer conn.close();
        try conn.writer().interface.print("APPEND\n", .{});

        var body = std.ArrayList(u8);
        defer body.deinit(cluster.allocator);
        const writer = body.writer();

        var entry_buf: []const u8 = "";
        if (opt_entry) |entry| {
            entry.serialize(writer) catch {};
            entry_buf = body.items;
        }

        const hdr = AppendEntriesReq{
            .term = cluster.term,
            .leader_id = cluster.self_node.id,
            .prev_log_index = cluster.commit_index,
            .prev_log_term = cluster.term,
            .entries = entry_buf,
            .leader_commit = cluster.commit_index,
        };

        const encoded = try std.json.Stringify.valueAlloc(cluster.allocator, hdr, .{});
        defer cluster.allocator.free(encoded);
        try conn.writer().interface.writeAll(encoded);
    }

    pub fn handleAppendEntries(
        self: *ClusterState,
        db: *root.Database,
        req: AppendEntriesReq,
        allocator: std.mem.Allocator,
    ) !AppendEntriesResp {
        var response = AppendEntriesResp{
            .term = self.term,
            .success = false,
            .match_index = self.commit_index,
            .follower_id = self.self_node.id,
        };
        if (req.term < self.term) return response;

        if (req.entries.len > 0) {
            const entry = try root.LogEntry.deserialize(req.entries, allocator);
            try db.applyFromLeader(entry);
            self.commit_index += 1;
            self.last_applied = self.commit_index;
        }

        if (req.leader_commit > self.commit_index) {
            self.commit_index = req.leader_commit;
        }

        self.role = .Follower;
        self.term = req.term;
        self.leader_id = req.leader_id;
        self.cluster_meta.current_term = self.term;
        self.cluster_meta.commit_index = self.commit_index;
        self.cluster_meta.last_applied = self.last_applied;
        try self.cluster_meta.save(self.dir, self.allocator);

        response.success = true;
        response.match_index = self.commit_index;
        return response;
    }

    pub fn quromAdvance(self: *ClusterState) void {
        var match_indexes = std.ArrayList(u8);
        defer match_indexes.deinit(self.allocator);

        var it = self.match_index.iterator();
        while (it.next()) |entry| {
            try match_indexes.append(entry.value_ptr.*);
        }
        std.sort(u64, match_indexes.items, {}, std.sort.asc(u64));

        const mid = match_indexes.items.len / 2;
        const canidate = match_indexes.items[mid];
        if (canidate > self.commit_index) {
            self.commit_index = canidate;
            std.debug.print("[RAFT] Commit index advanced to {d}\n", .{canidate});
        }
    }

    pub fn createSnapshot(self: *ClusterState, db: *root.Database) !void {
        var mem_buf = std.ArrayList(u8).empty;
        defer mem_buf.deinit();
        const writer = mem_buf.writer();

        try db.jsonStringify(std.json.Stringify{
            .writer = writer,
            .options = .{},
        });

        var snapshot_file = try self.dir.createFile("snapshot.json", .{ .truncate = true });
        defer snapshot_file.close();
        try snapshot_file.writeAll(mem_buf.items);
        try snapshot_file.sync();

        std.debug.print("[RAFT] Snapshot saved. index={d}\n", .{self.commit_index});
    }

    pub fn installSnapshot(self: *ClusterState, db: *root.Database) !void {
        const f = self.dir.openDir("snapshot.json", .{}) catch |err| {
            std.debug.print("[RAFT] No Snapshot to install: {any}\n", .{err});
            return;
        };
        defer f.close();

        const bytes = try f.readFileAlloc(self.allocator, "snapshot.json", std.math.maxInt(usize));
        defer self.allocator.free(bytes);
        try db.importFromJson(bytes);
    }

    /// Generates a new node ID.
    fn generateNodeId(allocator: std.mem.Allocator) ![]const u8 {
        var buf: [16]u8 = undefined;
        try std.crypto.random(&buf);
        return try std.fmt.allocPrint(allocator, "{x:0>32}", .{buf});
    }
};
