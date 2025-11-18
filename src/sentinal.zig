const std = @import("std");
const root = @import("root.zig");
const cluster = @import("cluster.zig");

pub const Sentinel = struct {
    allocator: std.mem.Allocator,
    db: *root.Database,
    cluster: *cluster.ClusterState,
    running: bool = true,
    gossip_feq_ns: u64 = 2 * std.time.ns_per_s,
    election_timeout_ns: u64 = 5 * std.time.ns_per_s,

    pub fn init(allocator: std.mem.Allocator, db: *root.Database, cluster_state: *cluster.ClusterState) !Sentinel {
        return Sentinel{
            .allocator = allocator,
            .db = db,
            .cluster = cluster_state,
        };
    }

    /// Bootstraps cluster Leadership election.
    pub fn run(self: *Sentinel) !void {
        std.debug.print("[SENTINAL] Starting orchestration loop.\n", .{});

        var gossip_thread = try std.Thread.spawn(.{}, cluster.startGossipDiscovery, .{self.cluster});
        defer gossip_thread.join();

        var heartbeat_timer = try std.time.Timer.start();
        var election_timer = try std.time.Timer.start();

        while (self.running) {
            if (election_timer.read() >= self.election_timeout_ns) {
                self.triggerElection() catch |err| {
                    std.debug.print("[SENTINAL] election erorr: {any}\n", .{@errorName(err)});
                };
                election_timer.reset();
            }

            if (heartbeat_timer.read >= self.gossip_feq_ns and self.cluster.role == .Leader) {
                self.sendHeartbeats() catch {};
                heartbeat_timer.reset();
            }

            self.checkPeerHealth();
            std.Thread.sleep(std.time.ms_per_s * 500);
        }

        std.debug.print("[SENTINAL] Shutdown complete.\n");
    }

    fn triggerElection(self: *Sentinel) !void {
        // No explicit votes - use Lowest UUID (simple fallback election)
        var cluster_ref = self.cluster;
        var lowest = cluster_ref.self_node.id;
        var it = cluster_ref.peers.iterator();
        var online = 0;
        while (it.next()) |entry| {
            const p = entry.value_ptr.*;
            if (p.online) online += 1;
            if (p.online and std.mem.lessThan(u8, p.id, lowest))
                lowest = p.id;
        }

        if (std.mem.eql(u8, lowest, cluster_ref.self_node.id)) {
            cluster_ref.role = .Leader;
            cluster_ref.leader_id = cluster_ref.self_node.id;
            std.debug.print("[SENTINAL] Promoted as leader (term {d})\n", .{cluster_ref.term});
        } else {
            cluster_ref.role = .Follower;
            cluster_ref.leader_id = lowest;
            std.debug.print("[SENTINAL] Following Node {s}.\n", .{lowest});
        }
    }

    fn sendHeartbeat(self: *Sentinel) !void {
        var cluster_ref = self.cluster;

        var it = cluster_ref.peers.iterator();
        while (it.next()) |entry| {
            const ninfo = entry.value_ptr.*;

            _ = cluster_ref.sendAppendEntries(
                ninfo.addr,
                null,
            ) catch |err| {
                std.debug.print("[SENTINAL] Heartbeat to {s} failed: {any}\n", .{ ninfo.addr, err });
            };
        }
    }

    fn checkPeerHealth(self: *Sentinel) void {
        const now = std.time.timestamp();
        var unhealthy_list = std.ArrayList([]const u8);
        defer unhealthy_list.deinit();

        var it = self.cluster.peers.iterator();
        while (it.next()) |entry| {
            const peer = entry.value_ptr.*;
            if (peer.online and (now - peer.last_heartbeat) > 10) {
                peer.online = false;
                _ = unhealthy_list.append(peer.addr) catch {};
            }
        }

        for (unhealthy_list.items) |p| {
            std.debug.print("[SENTINEL] Peer {s} timed out.\n", .{p});
        }
    }

    /// Stop orchestrator safely
    pub fn stop(self: *Sentinel) void {
        self.running = false;
    }
};
