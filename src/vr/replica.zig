const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vr);

const config = @import("../config.zig");

const Time = @import("../time.zig").Time;
const MessageBus = @import("../message_bus.zig").MessageBusReplica;
const Message = @import("../message_bus.zig").Message;
const StateMachine = @import("../state_machine.zig").StateMachine;

const vr = @import("../vr.zig");
const Header = vr.Header;
const Clock = vr.Clock;
const Journal = vr.Journal;
const Timeout = vr.Timeout;
const Command = vr.Command;

pub const Status = enum {
    normal,
    view_change,
    recovering,
};

const ClientTable = std.AutoHashMap(u128, ClientTableEntry);

/// We found two bugs in the VRR paper relating to the client table:
///
/// 1. a correctness bug, where successive client crashes may cause request numbers to collide for
/// different request payloads, resulting in requests receiving the wrong reply, and
///
/// 2. a liveness bug, where if the client table is updated for `.request` and `.prepare` messages
/// with the client's latest request number, then the client may be locked out from the cluster if
/// the request is ever reordered through a view change.
///
/// We therefore take a different approach with the implementation of our client table, to:
///
/// 1. register client sessions explicitly through the state machine to ensure that client session
/// numbers always increase, and
///
/// 2. make a more careful distinction between uncommitted and committed request numbers,
/// considering that uncommitted requests may not survive a view change.
const ClientTableEntry = struct {
    /// The client's session number as committed to the cluster by a .register request.
    session: u64,

    /// The reply sent to the client's latest committed request.
    reply: *Message,
};

pub const Replica = struct {
    allocator: *Allocator,

    /// The number of the cluster to which this replica belongs:
    cluster: u32,

    /// The number of replicas in the cluster:
    replica_count: u8,

    /// The index of this replica's address in the configuration array held by the MessageBus:
    replica: u8,

    /// The maximum number of replicas that may be faulty:
    f: u8,

    /// A distributed fault-tolerant clock to provide lower/upper bounds on the leader's wall clock:
    clock: Clock,

    /// The persistent log of hash-chained journal entries:
    journal: *Journal,

    /// An abstraction to send messages from the replica to itself or another replica or client.
    /// The recipient replica or client may be a local in-memory pointer or network-addressable.
    /// The message bus will also deliver messages to this replica by calling Replica.on_message().
    message_bus: *MessageBus,

    /// For executing service up-calls after an operation has been committed:
    state_machine: *StateMachine,

    /// The client table records for each client the latest session and the latest committed reply.
    client_table: ClientTable,

    /// The current view, initially 0:
    view: u32,

    /// Whether we have experienced a view jump:
    /// If this is true then we must request a start_view message from the leader before committing.
    /// This prevents us from committing ops that may have been reordered through a view change.
    view_jump_barrier: bool = false,

    /// The current status, either normal, view_change, or recovering:
    /// TODO Don't default to normal, set the starting status according to the journal's health.
    status: Status = .normal,

    /// The op number assigned to the most recently prepared operation:
    op: u64,

    /// The op number of the latest committed and executed operation (according to the replica):
    /// The replica may have to wait for repairs to complete before commit_min reaches commit_max.
    commit_min: u64,

    /// The op number of the latest committed operation (according to the cluster):
    /// This is the commit number in terms of the VRR paper.
    commit_max: u64,

    /// The current request's checksum (used for now to enforce one-at-a-time request processing):
    request_checksum: ?u128 = null,

    /// The current prepare message (used to cross-check prepare_ok messages, and for resending):
    prepare_message: ?*Message = null,
    prepare_attempt: u64 = 0,

    appending: bool = false,
    appending_frame: @Frame(write_to_journal) = undefined,

    repairing: bool = false,
    repairing_frame: @Frame(write_to_journal) = undefined,

    committing: bool = false,

    sending_prepare: bool = false,
    sending_prepare_frame: @Frame(send_prepare_to_replica) = undefined,

    /// TODO Size repair_queue_max according to a reasonable bandwidth-delay product:
    repair_queue: ?*Message = null,
    repair_queue_len: usize = 0,
    repair_queue_max: usize = 3,

    /// Unique prepare_ok messages for the same view, op number and checksum from ALL replicas:
    prepare_ok_from_all_replicas: []?*Message,

    /// Unique start_view_change messages for the same view from OTHER replicas (excluding ourself):
    start_view_change_from_other_replicas: []?*Message,

    /// Unique do_view_change messages for the same view from ALL replicas (including ourself):
    do_view_change_from_all_replicas: []?*Message,

    /// Unique nack_prepare messages for the same view from OTHER replicas (excluding ourself):
    nack_prepare_from_other_replicas: []?*Message,

    /// Whether a replica has received a quorum of start_view_change messages for the view change:
    start_view_change_quorum: bool = false,

    /// Whether the leader has received a quorum of do_view_change messages for the view change:
    /// Determines whether the leader may effect repairs according to the CTRL protocol.
    do_view_change_quorum: bool = false,

    /// Whether the leader is expecting to receive a nack_prepare and for which op:
    nack_prepare_op: ?u64 = null,

    /// The number of ticks before a leader or follower broadcasts a ping to the other replicas:
    /// TODO Explain why we need this (MessageBus handshaking, leapfrogging faulty replicas,
    /// deciding whether starting a view change would be detrimental under some network partitions).
    ping_timeout: Timeout,

    /// The number of ticks without enough prepare_ok's before the leader resends a prepare:
    /// TODO Adjust this dynamically to match sliding window EWMA of recent network latencies.
    prepare_timeout: Timeout,

    /// The number of ticks before the leader sends a commit heartbeat:
    /// The leader always sends a commit heartbeat irrespective of when it last sent a prepare.
    /// This improves liveness when prepare messages cannot be replicated fully due to partitions.
    commit_timeout: Timeout,

    /// The number of ticks without hearing from the leader before a follower starts a view change:
    /// This transitions from .normal status to .view_change status.
    normal_timeout: Timeout,

    /// The number of ticks before a view change is timed out:
    /// This transitions from `view_change` status to `view_change` status but for a newer view.
    view_change_timeout: Timeout,

    /// The number of ticks before resending a `start_view_change` or `do_view_change` message:
    view_change_message_timeout: Timeout,

    /// The number of ticks before repairing missing/disconnected headers and/or dirty entries:
    repair_timeout: Timeout,

    /// Used to provide deterministic entropy to `choose_any_other_replica()`.
    /// Incremented whenever `choose_any_other_replica()` is called.
    choose_any_other_replica_ticks: u64 = 0,

    pub fn init(
        allocator: *Allocator,
        cluster: u32,
        replica_count: u8,
        replica: u8,
        time: *Time,
        // TODO We should actually provide Storage here, a Replica will always use the same Journal:
        journal: *Journal,
        message_bus: *MessageBus,
        state_machine: *StateMachine,
    ) !Replica {
        // The smallest f such that 2f + 1 is less than or equal to the number of replicas.
        const f = (replica_count - 1) / 2;

        assert(replica_count > 0);
        assert(replica_count > f);
        assert(replica < replica_count);
        assert(f > 0 or replica_count <= 2);

        var client_table = ClientTable.init(allocator);
        errdefer client_table.deinit();
        try client_table.ensureCapacity(@intCast(u32, config.clients_max));

        var prepare_ok = try allocator.alloc(?*Message, replica_count);
        errdefer allocator.free(prepare_ok);
        std.mem.set(?*Message, prepare_ok, null);

        var start_view_change = try allocator.alloc(?*Message, replica_count);
        errdefer allocator.free(start_view_change);
        std.mem.set(?*Message, start_view_change, null);

        var do_view_change = try allocator.alloc(?*Message, replica_count);
        errdefer allocator.free(do_view_change);
        std.mem.set(?*Message, do_view_change, null);

        var nack_prepare = try allocator.alloc(?*Message, replica_count);
        errdefer allocator.free(nack_prepare);
        std.mem.set(?*Message, nack_prepare, null);

        // TODO Initialize the journal when initializing the cluster:
        var init_prepare = Header{
            .nonce = 0,
            .client = 0,
            .cluster = cluster,
            .view = 0,
            .op = 0,
            .commit = 0,
            .offset = 0,
            .size = @sizeOf(Header),
            .epoch = 0,
            .request = 0,
            .replica = 0,
            .command = .prepare,
            .operation = .init,
        };
        init_prepare.set_checksum_body(&[0]u8{});
        init_prepare.set_checksum();
        assert(init_prepare.valid_checksum());
        assert(init_prepare.invalid() == null);
        journal.headers[0] = init_prepare;
        journal.assert_headers_reserved_from(init_prepare.op + 1);

        var self = Replica{
            .allocator = allocator,
            .cluster = cluster,
            .replica_count = replica_count,
            .replica = replica,
            .f = f,
            // TODO Drop these @intCasts when the client table branch lands:
            .clock = try Clock.init(
                allocator,
                @intCast(u8, replica_count),
                @intCast(u8, replica),
                time,
            ),
            .journal = journal,
            .message_bus = message_bus,
            .state_machine = state_machine,
            .client_table = client_table,
            .view = init_prepare.view,
            .op = init_prepare.op,
            .commit_min = init_prepare.commit,
            .commit_max = init_prepare.commit,
            .prepare_ok_from_all_replicas = prepare_ok,
            .start_view_change_from_other_replicas = start_view_change,
            .do_view_change_from_all_replicas = do_view_change,
            .nack_prepare_from_other_replicas = nack_prepare,

            .ping_timeout = Timeout{
                .name = "ping_timeout",
                .replica = replica,
                .after = 100,
            },
            .prepare_timeout = Timeout{
                .name = "prepare_timeout",
                .replica = replica,
                .after = 50,
            },
            .commit_timeout = Timeout{
                .name = "commit_timeout",
                .replica = replica,
                .after = 100,
            },
            .normal_timeout = Timeout{
                .name = "normal_timeout",
                .replica = replica,
                .after = 500,
            },
            .view_change_timeout = Timeout{
                .name = "view_change_timeout",
                .replica = replica,
                .after = 500,
            },
            .view_change_message_timeout = Timeout{
                .name = "view_change_message_timeout",
                .replica = replica,
                .after = 50,
            },
            .repair_timeout = Timeout{
                .name = "repair_timeout",
                .replica = replica,
                .after = 50,
            },
        };

        // We must initialize timeouts here, not in tick() on the first tick, because on_message()
        // can race with tick()... before timeouts have been initialized:
        assert(self.status == .normal);
        if (self.leader()) {
            log.debug("{}: init: leader", .{self.replica});
            self.ping_timeout.start();
            self.commit_timeout.start();
            self.repair_timeout.start();
        } else {
            log.debug("{}: init: follower", .{self.replica});
            self.ping_timeout.start();
            self.normal_timeout.start();
            self.repair_timeout.start();
        }

        return self;
    }

    pub fn deinit(self: *Replica) void {
        self.client_table.deinit();
        self.allocator.free(self.prepare_ok_from_all_replicas);
        self.allocator.free(self.start_view_change_from_other_replicas);
        self.allocator.free(self.do_view_change_from_all_replicas);
        self.allocator.free(self.nack_prepare_from_other_replicas);
    }

    /// Returns whether the replica is a follower for the current view.
    /// This may be used only when the replica status is normal.
    pub fn follower(self: *Replica) bool {
        return !self.leader();
    }

    /// Returns whether the replica is the leader for the current view.
    /// This may be used only when the replica status is normal.
    pub fn leader(self: *Replica) bool {
        assert(self.status == .normal);
        return self.leader_index(self.view) == self.replica;
    }

    /// Returns the index into the configuration of the leader for a given view.
    pub fn leader_index(self: *Replica, view: u32) u8 {
        return @intCast(u8, @mod(view, self.replica_count));
    }

    /// Time is measured in logical ticks that are incremented on every call to tick().
    /// This eliminates a dependency on the system time and enables deterministic testing.
    pub fn tick(self: *Replica) void {
        self.clock.tick();

        self.ping_timeout.tick();
        self.prepare_timeout.tick();
        self.commit_timeout.tick();
        self.normal_timeout.tick();
        self.view_change_timeout.tick();
        self.view_change_message_timeout.tick();
        self.repair_timeout.tick();

        if (self.ping_timeout.fired()) self.on_ping_timeout();
        if (self.prepare_timeout.fired()) self.on_prepare_timeout();
        if (self.commit_timeout.fired()) self.on_commit_timeout();
        if (self.normal_timeout.fired()) self.on_normal_timeout();
        if (self.view_change_timeout.fired()) self.on_view_change_timeout();
        if (self.view_change_message_timeout.fired()) self.on_view_change_message_timeout();
        if (self.repair_timeout.fired()) self.on_repair_timeout();

        self.repair_last_queued_message_if_any();
    }

    /// Called by the MessageBus to deliver a message to the replica.
    pub fn on_message(self: *Replica, message: *Message) void {
        log.debug("{}:", .{self.replica});
        log.debug("{}: on_message: view={} status={s} {}", .{
            self.replica,
            self.view,
            @tagName(self.status),
            message.header,
        });
        if (message.header.invalid()) |reason| {
            log.debug("{}: on_message: invalid ({s})", .{ self.replica, reason });
            return;
        }
        if (message.header.cluster != self.cluster) {
            log.warn("{}: on_message: wrong cluster (message.header.cluster={} instead of {})", .{
                self.replica,
                message.header.cluster,
                self.cluster,
            });
            return;
        }
        assert(message.header.replica < self.replica_count);
        switch (message.header.command) {
            .ping => self.on_ping(message),
            .pong => self.on_pong(message),
            .request => self.on_request(message),
            .prepare => self.on_prepare(message),
            .prepare_ok => self.on_prepare_ok(message),
            .commit => self.on_commit(message),
            .start_view_change => self.on_start_view_change(message),
            .do_view_change => self.on_do_view_change(message),
            .start_view => self.on_start_view(message),
            .request_start_view => self.on_request_start_view(message),
            .request_prepare => self.on_request_prepare(message),
            .request_headers => self.on_request_headers(message),
            .headers => self.on_headers(message),
            .nack_prepare => self.on_nack_prepare(message),
            else => unreachable,
        }
    }

    fn on_ping(self: *Replica, message: *const Message) void {
        if (self.status != .normal and self.status != .view_change) return;

        assert(self.status == .normal or self.status == .view_change);

        // TODO Drop pings that were not addressed to us.

        var pong = Header{
            .command = .pong,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
        };

        if (message.header.client > 0) {
            assert(message.header.replica == 0);

            // 4.5 Client Recovery
            // If a client crashes and recovers it must start up with a request number larger than
            // what it had before it failed. It fetches its latest number from the replicas and adds
            // 2 to this value to be sure the new request number is big enough. Adding 2 ensures
            // that its next request will have a unique number even in the odd case where the latest
            // request it sent before it failed is still in transit (since that request will have as
            // its request number the number the client learns plus 1).
            //
            // TODO Lookup latest request number from client table:
            pong.request = 0;
            self.message_bus.send_header_to_client(message.header.client, pong);
        } else if (message.header.replica == self.replica) {
            log.warn("{}: on_ping: ignoring (self)", .{self.replica});
        } else {
            // Copy the ping's monotonic timestamp across to our pong and add our wall clock sample:
            pong.op = message.header.op;
            pong.offset = @bitCast(u64, self.clock.realtime());
            self.message_bus.send_header_to_replica(message.header.replica, pong);
        }
    }

    fn on_pong(self: *Replica, message: *const Message) void {
        if (message.header.client > 0) return;
        if (message.header.replica == self.replica) return;

        const m0 = message.header.op;
        const t1 = @bitCast(i64, message.header.offset);
        const m2 = self.clock.monotonic();

        // TODO Drop the @intCast when the client table branch lands.
        self.clock.learn(@intCast(u8, message.header.replica), m0, t1, m2);
    }

    /// The primary advances op-number, adds the request to the end of the log, and updates the
    /// information for this client in the client-table to contain the new request number, s.
    /// Then it sends a ⟨PREPARE v, m, n, k⟩ message to the other replicas, where v is the current
    /// view-number, m is the message it received from the client, n is the op-number it assigned to
    /// the request, and k is the commit-number.
    fn on_request(self: *Replica, message: *Message) void {
        if (self.ignore_request_message(message)) return;

        if (self.request_checksum) |request_checksum| {
            assert(message.header.command == .request);
            if (message.header.checksum == request_checksum) {
                log.debug("{}: on_request: ignoring (already preparing)", .{self.replica});
                return;
            }
        }

        // TODO Queue (or drop client requests after a limit) to handle one request at a time:
        // TODO Clear this queue if we lose our leadership (critical for correctness).
        assert(self.commit_min == self.commit_max and self.commit_max == self.op);
        assert(self.request_checksum == null);
        self.request_checksum = message.header.checksum;

        log.debug("{}: on_request: request {}", .{ self.replica, message.header.checksum });

        var body = message.buffer[@sizeOf(Header)..message.header.size];
        self.state_machine.prepare(message.header.operation.to_state_machine_op(StateMachine), body);

        var latest_entry = self.journal.entry_for_op_exact(self.op).?;
        message.header.nonce = latest_entry.checksum;
        message.header.view = self.view;
        message.header.op = self.op + 1;
        message.header.commit = self.commit_max;
        message.header.offset = self.journal.next_offset(latest_entry);
        message.header.replica = self.replica;
        message.header.command = .prepare;

        message.header.set_checksum_body(body);
        message.header.set_checksum();

        assert(message.header.checksum != self.request_checksum.?);

        log.debug("{}: on_request: prepare {}", .{ self.replica, message.header.checksum });

        assert(self.prepare_message == null);
        assert(self.prepare_attempt == 0);
        for (self.prepare_ok_from_all_replicas) |received| assert(received == null);
        assert(self.prepare_timeout.ticking == false);

        self.prepare_message = message.ref();
        self.prepare_attempt = 0;
        self.prepare_timeout.start();

        // Use the same replication code path for the leader and followers:
        self.send_message_to_replica(self.replica, message);
    }

    /// Replication is simple, with a single code path for the leader and followers:
    ///
    /// The leader starts by sending a prepare message to itself.
    ///
    /// Each replica (including the leader) then forwards this prepare message to the next replica
    /// in the configuration, in parallel to writing to its own journal, closing the circle until
    /// the next replica is back to the leader, in which case the replica does not forward.
    ///
    /// This keeps the leader's outgoing bandwidth limited (one-for-one) to incoming bandwidth,
    /// since the leader need only replicate to the next replica. Otherwise, the leader would need
    /// to replicate to multiple followers, dividing available bandwidth.
    ///
    /// This does not impact latency, since with Flexible Paxos we need only one remote prepare_ok.
    /// It is ideal if this synchronous replication to one remote replica is to the next replica,
    /// since that is the replica next in line to be leader, which will need to be up-to-date before
    /// it can start the next view.
    ///
    /// At the same time, asynchronous replication keeps going, so that if our local disk is slow
    /// then any latency spike will be masked by more remote prepare_ok messages as they come in.
    /// This gives automatic tail latency tolerance for storage latency spikes.
    ///
    /// The remaining problem then is tail latency tolerance for network latency spikes.
    /// If the next replica is down or partitioned, then the leader's prepare timeout will fire,
    /// and the leader will resend but to another replica, until it receives enough prepare_ok's.
    fn on_prepare(self: *Replica, message: *Message) void {
        self.view_jump(message.header);

        if (self.is_repair(message)) {
            log.debug("{}: on_prepare: ignoring (repair)", .{self.replica});
            self.on_repair(message);
            return;
        }

        if (self.status != .normal) {
            log.debug("{}: on_prepare: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.leader() or self.follower());
        assert(message.header.replica == self.leader_index(message.header.view));
        assert(message.header.op > self.op);
        assert(message.header.op > self.commit_min);

        if (self.follower()) self.normal_timeout.reset();

        if (message.header.op > self.op + 1) {
            log.debug("{}: on_prepare: newer op", .{self.replica});
            self.jump_to_newer_op_in_normal_status(message.header);
        }

        if (self.journal.previous_entry(message.header)) |previous| {
            // Any previous entry may be a whole journal's worth of ops behind due to wrapping.
            // We therefore do not do any further op, offset or checksum assertions beyond this:
            self.panic_if_hash_chain_would_break_in_the_same_view(previous, message.header);
        }

        // We must advance our op and set the header as dirty before replicating and journalling.
        // The leader needs this before its journal is outrun by any prepare_ok quorum:
        log.debug("{}: on_prepare: advancing: op={}..{} checksum={}..{}", .{
            self.replica,
            self.op,
            message.header.op,
            message.header.nonce,
            message.header.checksum,
        });
        assert(message.header.op == self.op + 1);
        self.op = message.header.op;
        self.journal.set_entry_as_dirty(message.header);

        // We have the latest op from the leader and have therefore cleared the view jump barrier:
        if (self.view_jump_barrier) {
            self.view_jump_barrier = false;
            log.notice("{}: on_prepare: cleared view jump barrier", .{self.replica});
        }

        // TODO Update client's information in the client table.

        self.replicate(message);
        self.append(message);

        if (self.follower()) {
            // A prepare may already be committed if requested by repair() so take the max:
            self.commit_ops_through(std.math.max(message.header.commit, self.commit_max));
        }
    }

    fn on_prepare_ok(self: *Replica, message: *Message) void {
        if (self.status != .normal) {
            log.warn("{}: on_prepare_ok: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_prepare_ok: ignoring (older view)", .{self.replica});
            return;
        }

        if (message.header.view > self.view) {
            // Another replica is treating us as the leader for a view we do not know about.
            // This may be caused by a fault in the network topology.
            log.warn("{}: on_prepare_ok: ignoring (newer view)", .{self.replica});
            return;
        }

        if (self.follower()) {
            // This may be caused by a fault in the network topology.
            log.warn("{}: on_prepare_ok: ignoring (follower)", .{self.replica});
            return;
        }

        if (self.prepare_message) |prepare_message| {
            if (message.header.nonce != prepare_message.header.checksum) {
                log.debug("{}: on_prepare_ok: ignoring (different nonce)", .{self.replica});
                return;
            }
        } else {
            log.debug("{}: on_prepare_ok: ignoring (not preparing)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.leader());

        assert(message.header.command == .prepare_ok);
        assert(message.header.nonce == self.prepare_message.?.header.checksum);
        assert(message.header.client == self.prepare_message.?.header.client);
        assert(message.header.cluster == self.prepare_message.?.header.cluster);
        assert(message.header.view == self.prepare_message.?.header.view);
        assert(message.header.op == self.prepare_message.?.header.op);
        assert(message.header.commit == self.prepare_message.?.header.commit);
        assert(message.header.offset == self.prepare_message.?.header.offset);
        assert(message.header.epoch == self.prepare_message.?.header.epoch);
        assert(message.header.request == self.prepare_message.?.header.request);
        assert(message.header.operation == self.prepare_message.?.header.operation);
        assert(message.header.op == self.op);
        assert(message.header.op == self.commit_min + 1);
        assert(message.header.op == self.commit_max + 1);

        // Wait until we have `f + 1` messages (including ourself) for quorum:
        const threshold = self.f + 1;
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.prepare_ok_from_all_replicas,
            message,
            threshold,
        ) orelse return;

        assert(count == threshold);
        log.debug("{}: on_prepare_ok: quorum received", .{self.replica});

        self.commit_op(self.prepare_message.?);
        assert(self.commit_min == self.op);
        assert(self.commit_max == self.op);

        self.reset_quorum_prepare();
    }

    fn on_commit(self: *Replica, message: *const Message) void {
        self.view_jump(message.header);

        if (self.status != .normal) {
            log.debug("{}: on_commit: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_commit: ignoring (older view)", .{self.replica});
            return;
        }

        if (self.leader()) {
            log.warn("{}: on_commit: ignoring (leader)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.follower());
        assert(message.header.replica == self.leader_index(message.header.view));

        // We may not always have the latest commit entry but if we do these checksums must match:
        if (self.journal.entry_for_op_exact(message.header.commit)) |commit_entry| {
            if (commit_entry.checksum == message.header.nonce) {
                log.debug("{}: on_commit: verified commit checksum", .{self.replica});
            } else {
                @panic("commit checksum verification failed");
            }
        }

        self.normal_timeout.reset();

        self.commit_ops_through(message.header.commit);
    }

    fn on_repair(self: *Replica, message: *Message) void {
        assert(message.header.command == .prepare);

        if (self.status != .normal and self.status != .view_change) {
            log.debug("{}: on_repair: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view > self.view) {
            log.debug("{}: on_repair: ignoring (newer view)", .{self.replica});
            return;
        }

        if (self.status == .view_change and message.header.view == self.view) {
            log.debug("{}: on_repair: ignoring (view started)", .{self.replica});
            return;
        }

        if (self.status == .view_change and self.leader_index(self.view) != self.replica) {
            log.debug("{}: on_repair: ignoring (view change, follower)", .{self.replica});
            return;
        }

        if (self.status == .view_change and !self.do_view_change_quorum) {
            log.debug("{}: on_repair: ignoring (view change, waiting for quorum)", .{self.replica});
            return;
        }

        if (message.header.op > self.op) {
            assert(message.header.view < self.view);
            log.debug("{}: on_repair: ignoring (would advance self.op)", .{self.replica});
            return;
        }

        assert(self.status == .normal or self.status == .view_change);
        assert(self.repairs_allowed());
        assert(message.header.view <= self.view);
        assert(message.header.op <= self.op); // Repairs may never advance `self.op`.

        if (self.journal.has_clean(message.header)) {
            log.debug("{}: on_repair: duplicate", .{self.replica});
            self.send_prepare_ok(message.header);
            return;
        }

        if (self.repair_header(message.header)) {
            assert(self.journal.has_dirty(message.header));

            if (self.nack_prepare_op) |nack_prepare_op| {
                if (nack_prepare_op == message.header.op) {
                    log.debug("{}: on_repair: repairing uncommitted op={}", .{
                        self.replica,
                        message.header.op,
                    });
                    self.reset_quorum_nack_prepare();
                }
            }

            if (self.repairing) return self.repair_later(message);

            log.debug("{}: on_repair: repairing journal", .{self.replica});
            self.repairing_frame = async self.write_to_journal(message, &self.repairing);
        }
    }

    fn on_start_view_change(self: *Replica, message: *Message) void {
        if (self.ignore_view_change_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view >= self.view);
        assert(message.header.replica != self.replica);

        self.view_jump(message.header);

        assert(!self.view_jump_barrier);
        assert(self.status == .view_change);
        assert(message.header.view == self.view);

        // Wait until we have `f` messages (excluding ourself) for quorum:
        assert(self.replica_count > 1);
        assert(self.f > 0 or self.replica_count == 2);
        const threshold = std.math.max(1, self.f);

        const count = self.add_message_and_receive_quorum_exactly_once(
            self.start_view_change_from_other_replicas,
            message,
            threshold,
        ) orelse return;

        assert(count == threshold);
        assert(self.start_view_change_from_other_replicas[self.replica] == null);
        log.debug("{}: on_start_view_change: quorum received", .{self.replica});

        assert(!self.start_view_change_quorum);
        assert(!self.do_view_change_quorum);
        self.start_view_change_quorum = true;

        // When replica i receives start_view_change messages for its view from f other replicas,
        // it sends a ⟨do_view_change v, l, v’, n, k, i⟩ message to the node that will be the
        // primary in the new view. Here v is its view, l is its log, v′ is the view number of the
        // latest view in which its status was normal, n is the op number, and k is the commit
        // number.
        self.send_do_view_change();
    }

    /// When the new primary receives f + 1 do_view_change messages from different replicas
    /// (including itself), it sets its view number to that in the messages and selects as the
    /// new log the one contained in the message with the largest v′; if several messages have
    /// the same v′ it selects the one among them with the largest n. It sets its op number to
    /// that of the topmost entry in the new log, sets its commit number to the largest such
    /// number it received in the do_view_change messages, changes its status to normal, and
    /// informs the other replicas of the completion of the view change by sending
    /// ⟨start_view v, l, n, k⟩ messages to the other replicas, where l is the new log, n is the
    /// op number, and k is the commit number.
    fn on_do_view_change(self: *Replica, message: *Message) void {
        if (self.ignore_view_change_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view >= self.view);
        assert(self.leader_index(message.header.view) == self.replica);

        self.view_jump(message.header);

        assert(!self.view_jump_barrier);
        assert(self.status == .view_change);
        assert(message.header.view == self.view);

        // We may receive a `do_view_change` quorum from other replicas, which already have a
        // `start_view_change_quorum`, before we receive a `start_view_change_quorum`:
        if (!self.start_view_change_quorum) {
            log.notice("{}: on_do_view_change: waiting for start_view_change quorum", .{
                self.replica,
            });
            return;
        }

        // Wait until we have `f + 1` messages (including ourself) for quorum:
        const threshold = self.f + 1;
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.do_view_change_from_all_replicas,
            message,
            threshold,
        ) orelse return;

        assert(count == threshold);
        assert(self.do_view_change_from_all_replicas[self.replica] != null);
        log.debug("{}: on_do_view_change: quorum received", .{self.replica});

        var latest = Header.reserved();
        var k: ?u64 = null;

        for (self.do_view_change_from_all_replicas) |received, replica| {
            if (received) |m| {
                assert(m.header.command == .do_view_change);
                assert(m.header.cluster == self.cluster);
                assert(m.header.replica == replica);
                assert(m.header.view == self.view);

                if (k == null or m.header.commit > k.?) k = m.header.commit;
                self.set_latest_header(self.message_body_as_headers(m), &latest);
            }
        }

        self.set_latest_op_and_k(&latest, k.?, "on_do_view_change");

        // Now that we have the latest op in place, repair any other headers:
        for (self.do_view_change_from_all_replicas) |received| {
            if (received) |m| {
                for (self.message_body_as_headers(m)) |*h| {
                    _ = self.repair_header(h);
                }
            }
        }

        // Verify that the repairs above have not replaced or advanced the latest op:
        assert(self.journal.entry_for_op_exact(self.op).?.checksum == latest.checksum);

        assert(self.start_view_change_quorum);
        assert(!self.do_view_change_quorum);
        self.do_view_change_quorum = true;

        // Start repairs according to the CTRL protocol:
        assert(!self.repair_timeout.ticking);
        self.repair_timeout.start();
        self.repair();
    }

    /// When other replicas receive the start_view message, they replace their log with the one
    /// in the message, set their op number to that of the latest entry in the log, set their
    /// view number to the view number in the message, change their status to normal, and update
    /// the information in their client table. If there are non-committed operations in the log,
    /// they send a ⟨prepare_ok v, n, i⟩ message to the primary; here n is the op-number. Then
    /// they execute all operations known to be committed that they haven’t executed previously,
    /// advance their commit number, and update the information in their client table.
    fn on_start_view(self: *Replica, message: *const Message) void {
        if (self.ignore_view_change_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view >= self.view);
        assert(message.header.replica != self.replica);
        assert(message.header.replica == self.leader_index(message.header.view));

        self.view_jump(message.header);

        assert(!self.view_jump_barrier or self.status == .normal);
        assert(self.status == .view_change or self.view_jump_barrier);
        assert(message.header.view == self.view);

        var latest = Header.reserved();
        self.set_latest_header(self.message_body_as_headers(message), &latest);
        assert(latest.op == message.header.op);

        self.set_latest_op_and_k(&latest, message.header.commit, "on_start_view");

        // Now that we have the latest op in place, repair any other headers:
        for (self.message_body_as_headers(message)) |*h| {
            _ = self.repair_header(h);
        }

        // Verify that the repairs above have not replaced or advanced the latest op:
        assert(self.journal.entry_for_op_exact(self.op).?.checksum == latest.checksum);

        if (self.view_jump_barrier) {
            assert(self.status == .normal);
            self.view_jump_barrier = false;
            log.notice("{}: on_start_view: resolved view jump barrier", .{self.replica});
        } else {
            assert(self.status == .view_change);
            self.transition_to_normal_status(message.header.view);
        }

        assert(!self.view_jump_barrier);
        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.follower());

        // TODO Send prepare_ok messages for uncommitted ops.

        self.commit_ops_through(self.commit_max);

        self.repair();
    }

    fn on_request_start_view(self: *Replica, message: *const Message) void {
        if (self.ignore_repair_message(message)) return;

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(message.header.replica != self.replica);
        assert(self.leader());

        const start_view = self.create_do_view_change_or_start_view_message(.start_view) orelse {
            log.debug("{}: on_request_start_view: dropping start_view, no message available", .{
                self.replica,
            });
            return;
        };
        defer self.message_bus.unref(start_view);

        assert(start_view.references == 1);
        assert(start_view.header.command == .start_view);
        assert(start_view.header.view == self.view);
        assert(start_view.header.op == self.op);
        assert(start_view.header.commit == self.commit_max);

        self.send_message_to_replica(message.header.replica, start_view);
    }

    fn on_request_prepare(self: *Replica, message: *const Message) void {
        if (self.ignore_repair_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view == self.view);
        assert(message.header.replica != self.replica);

        const op = message.header.op;
        var checksum: ?u128 = message.header.nonce;
        if (self.leader_index(self.view) == self.replica and message.header.nonce == 0) {
            checksum = null;
        }

        if (self.journal.entry_for_op_exact_with_checksum(op, checksum)) |entry| {
            assert(entry.op == op);

            if (!self.journal.dirty.bit(op)) {
                assert(!self.journal.faulty.bit(op));

                if (self.sending_prepare) return;
                self.sending_prepare_frame = async self.send_prepare_to_replica(
                    message.header.replica,
                    op,
                    checksum,
                );
                // We have guaranteed the prepare and our copy is clean (not safe to nack).
                return;
            } else if (self.journal.faulty.bit(op)) {
                // We have gauranteed the prepare but our copy is faulty (not safe to nack).
                return;
            }

            // We know of the prepare but we have yet to write or guarantee it (safe to nack).
            // Continue through below...
        }

        if (self.status == .view_change) {
            assert(message.header.replica == self.leader_index(self.view));
            assert(checksum != null);
            if (self.journal.entry_for_op_exact_with_checksum(op, checksum) != null) {
                assert(self.journal.dirty.bit(op) and !self.journal.faulty.bit(op));
            }
            self.send_header_to_replica(message.header.replica, .{
                .command = .nack_prepare,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .op = op,
                .nonce = checksum.?,
            });
        }
    }

    fn on_request_headers(self: *Replica, message: *const Message) void {
        if (self.ignore_repair_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view == self.view);
        assert(message.header.replica != self.replica);

        const op_min = message.header.commit;
        const op_max = message.header.op;
        assert(op_max >= op_min);

        // We must add 1 because op_max and op_min are both inclusive:
        const count_max = @intCast(u32, std.math.min(64, op_max - op_min + 1));
        assert(count_max > 0);

        const size_max = @sizeOf(Header) + @sizeOf(Header) * count_max;

        const response = self.message_bus.get_message() orelse {
            log.debug("{}: on_request_headers: dropping response, no message available", .{
                self.replica,
            });
            return;
        };
        defer self.message_bus.unref(response);

        response.header.* = .{
            .command = .headers,
            // We echo the nonce back to the replica so that they can match up our response:
            .nonce = message.header.nonce,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
        };

        const count = self.journal.copy_latest_headers_between(
            op_min,
            op_max,
            std.mem.bytesAsSlice(Header, response.buffer[@sizeOf(Header)..size_max]),
        );

        response.header.size = @intCast(u32, @sizeOf(Header) + @sizeOf(Header) * count);
        const body = response.buffer[@sizeOf(Header)..response.header.size];

        response.header.set_checksum_body(body);
        response.header.set_checksum();

        self.send_message_to_replica(message.header.replica, response);
    }

    fn on_nack_prepare(self: *Replica, message: *Message) void {
        if (self.ignore_repair_message(message)) return;

        assert(self.status == .view_change);
        assert(message.header.view == self.view);
        assert(message.header.replica != self.replica);
        assert(self.leader_index(self.view) == self.replica);
        assert(self.do_view_change_quorum);
        assert(self.repairs_allowed());

        if (self.nack_prepare_op == null) {
            log.debug("{}: on_nack_prepare: ignoring (no longer expected)", .{self.replica});
            return;
        }

        const op = self.nack_prepare_op.?;
        const checksum = self.journal.entry_for_op_exact(op).?.checksum;

        if (message.header.op != op) {
            log.debug("{}: on_nack_prepare: ignoring (repairing another op)", .{self.replica});
            return;
        }

        // Followers may not send a `nack_prepare` for a different checksum:
        assert(message.header.nonce == checksum);

        // We require a `nack_prepare` from a majority of followers if our op is faulty:
        // Otherwise, we know we do not have the op and need only `f` other nacks.
        assert(self.replica_count > 1);
        assert(self.f > 0 or self.replica_count == 2);
        assert(self.f + 1 == (self.replica_count - 1) / 2 + 1);
        const threshold = if (self.journal.faulty.bit(op)) self.f + 1 else std.math.max(1, self.f);

        // Wait until we have `threshold` messages for quorum:
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.nack_prepare_from_other_replicas,
            message,
            threshold,
        ) orelse return;

        assert(count == threshold);
        assert(self.nack_prepare_from_other_replicas[self.replica] == null);
        log.debug("{}: on_nack_prepare: quorum received", .{self.replica});

        assert(self.valid_hash_chain("on_nack_prepare"));

        assert(op > self.commit_max);
        assert(op <= self.op);
        assert(self.journal.entry_for_op_exact_with_checksum(op, checksum) != null);
        assert(self.journal.dirty.bit(op));

        log.debug("{}: on_nack_prepare: discarding uncommitted ops={}..{}", .{
            self.replica,
            op,
            self.op,
        });

        self.journal.remove_entries_from(op);
        self.op = op - 1;

        assert(self.journal.entry_for_op(op) == null);
        assert(!self.journal.dirty.bit(op));
        assert(!self.journal.faulty.bit(op));

        // We require that `self.op` always exists. Rewinding `self.op` could change that.
        // However, we do this only as the leader within a view change, with all headers intact.
        assert(self.journal.entry_for_op_exact(self.op) != null);

        self.reset_quorum_nack_prepare();

        self.repair();
    }

    fn on_headers(self: *Replica, message: *const Message) void {
        if (self.ignore_repair_message(message)) return;

        assert(self.status == .normal or self.status == .view_change);
        assert(message.header.view == self.view);
        assert(message.header.replica != self.replica);

        var op_min: ?u64 = null;
        var op_max: ?u64 = null;
        for (self.message_body_as_headers(message)) |*h| {
            if (op_min == null or h.op < op_min.?) op_min = h.op;
            if (op_max == null or h.op > op_max.?) op_max = h.op;
            _ = self.repair_header(h);
        }
        assert(op_max.? >= op_min.?);

        self.repair();
    }

    fn on_ping_timeout(self: *Replica) void {
        self.ping_timeout.reset();

        // TODO We may want to ping for connectivity during a view change.
        assert(self.status == .normal);
        assert(self.leader() or self.follower());

        var ping = Header{
            .command = .ping,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
            .op = self.clock.monotonic(),
        };

        self.send_header_to_other_replicas(ping);
    }

    fn on_prepare_timeout(self: *Replica) void {
        // TODO Exponential backoff.
        // TODO Prevent flooding the network due to multiple concurrent rounds of replication.
        self.prepare_timeout.reset();
        self.prepare_attempt += 1;

        assert(self.status == .normal);
        assert(self.leader());
        assert(self.request_checksum != null);
        assert(self.prepare_message != null);

        var message = self.prepare_message.?;
        assert(message.header.view == self.view);

        // The list of remote replicas yet to send a prepare_ok:
        var waiting: [32]u8 = undefined;
        var waiting_len: usize = 0;
        for (self.prepare_ok_from_all_replicas) |received, replica| {
            if (received == null and replica != self.replica) {
                waiting[waiting_len] = @intCast(u8, replica);
                waiting_len += 1;
                if (waiting_len == waiting.len) break;
            }
        }

        if (waiting_len == 0) {
            log.debug("{}: on_prepare_timeout: waiting for journal", .{self.replica});
            assert(self.prepare_ok_from_all_replicas[self.replica] == null);
            return;
        }

        for (waiting[0..waiting_len]) |replica| {
            log.debug("{}: on_prepare_timeout: waiting for replica {}", .{ self.replica, replica });
        }

        // Cycle through the list for each attempt to reach live replicas and get around partitions:
        // If only the first replica in the list was chosen... liveness would suffer if it was down!
        var replica = waiting[@mod(self.prepare_attempt, waiting_len)];
        assert(replica != self.replica);

        log.debug("{}: on_prepare_timeout: replicating to replica {}", .{ self.replica, replica });
        self.send_message_to_replica(replica, message);
    }

    fn on_commit_timeout(self: *Replica) void {
        self.commit_timeout.reset();

        assert(self.status == .normal);
        assert(self.leader());
        assert(self.commit_min == self.commit_max);

        // TODO Snapshots: Use snapshot checksum if commit is no longer in journal.
        const latest_committed_entry = self.journal.entry_for_op_exact(self.commit_max).?;

        self.send_header_to_other_replicas(.{
            .command = .commit,
            .nonce = latest_committed_entry.checksum,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
            .commit = self.commit_max,
        });
    }

    fn on_normal_timeout(self: *Replica) void {
        assert(self.status == .normal);
        assert(self.follower());
        self.transition_to_view_change_status(self.view + 1);
    }

    fn on_view_change_timeout(self: *Replica) void {
        assert(self.status == .view_change);
        self.transition_to_view_change_status(self.view + 1);
    }

    fn on_view_change_message_timeout(self: *Replica) void {
        self.view_change_message_timeout.reset();
        assert(self.status == .view_change);

        // Keep sending `start_view_change` messages:
        // We may have a `start_view_change_quorum` but other replicas may not.
        // However, the leader may stop sending once it has a `do_view_change_quorum`.
        if (!self.do_view_change_quorum) self.send_start_view_change();

        // It is critical that a `do_view_change` message implies a `start_view_change_quorum`:
        if (self.start_view_change_quorum) {
            // The leader need not retry to send a `do_view_change` message to itself:
            // We assume the MessageBus will not drop messages sent by a replica to itself.
            if (self.leader_index(self.view) != self.replica) self.send_do_view_change();
        }
    }

    fn on_repair_timeout(self: *Replica) void {
        assert(self.status == .normal or self.status == .view_change);
        self.repair();
    }

    fn add_message_and_receive_quorum_exactly_once(
        self: *Replica,
        messages: []?*Message,
        message: *Message,
        threshold: u32,
    ) ?usize {
        assert(messages.len == self.replica_count);
        assert(message.header.cluster == self.cluster);
        assert(message.header.view == self.view);

        switch (message.header.command) {
            .prepare_ok => {
                assert(self.status == .normal);
                assert(self.leader());
                assert(message.header.nonce == self.prepare_message.?.header.checksum);
            },
            .start_view_change => assert(self.status == .view_change),
            .do_view_change, .nack_prepare => {
                assert(self.status == .view_change);
                assert(self.leader_index(self.view) == self.replica);
            },
            else => unreachable,
        }

        assert(threshold >= 1);
        assert(threshold <= self.replica_count);

        const command: []const u8 = @tagName(message.header.command);

        // Do not allow duplicate messages to trigger multiple passes through a state transition:
        if (messages[message.header.replica]) |m| {
            // Assert that this truly is a duplicate message and not a different message:
            assert(m.header.command == message.header.command);
            assert(m.header.replica == message.header.replica);
            assert(m.header.view == message.header.view);
            assert(m.header.op == message.header.op);
            assert(m.header.commit == message.header.commit);
            assert(m.header.checksum_body == message.header.checksum_body);
            assert(m.header.checksum == message.header.checksum);
            log.debug("{}: on_{s}: ignoring (duplicate message)", .{ self.replica, command });
            return null;
        }

        // Record the first receipt of this message:
        assert(messages[message.header.replica] == null);
        messages[message.header.replica] = message.ref();

        // Count the number of unique messages now received:
        const count = self.count_quorum(messages, message.header.command, message.header.nonce);
        log.debug("{}: on_{s}: {} message(s)", .{ self.replica, command, count });

        // Wait until we have exactly `threshold` messages for quorum:
        if (count < threshold) {
            log.debug("{}: on_{s}: waiting for quorum", .{ self.replica, command });
            return null;
        }

        // This is not the first time we have had quorum, the state transition has already happened:
        if (count > threshold) {
            log.debug("{}: on_{s}: ignoring (quorum received already)", .{ self.replica, command });
            return null;
        }

        assert(count == threshold);
        return count;
    }

    fn append(self: *Replica, message: *Message) void {
        assert(self.status == .normal);
        assert(message.header.command == .prepare);
        assert(message.header.view == self.view);
        assert(message.header.op == self.op);

        if (self.appending) {
            log.debug("{}: append: skipping (slow journal outrun by quorum)", .{self.replica});
            self.repair_later(message);
            return;
        }

        log.debug("{}: append: appending to journal", .{self.replica});
        self.appending_frame = async self.write_to_journal(message, &self.appending);
    }

    /// Returns whether `b` succeeds `a` by having a newer view or same view and newer op.
    fn ascending_viewstamps(
        self: *Replica,
        a: *const Header,
        b: *const Header,
    ) bool {
        assert(a.command == .prepare);
        assert(b.command == .prepare);

        if (a.view < b.view) {
            // We do not assert b.op >= a.op, ops may be reordered during a view change.
            return true;
        } else if (a.view > b.view) {
            // We do not assert b.op <= a.op, ops may be reordered during a view change.
            return false;
        } else if (a.op < b.op) {
            assert(a.view == b.view);
            return true;
        } else if (a.op > b.op) {
            assert(a.view == b.view);
            return false;
        } else {
            unreachable;
        }
    }

    /// Choose a different replica each time if possible (excluding ourself).
    /// The choice of replica is a deterministic function of:
    /// 1. `choose_any_other_replica_ticks`, and
    /// 2. whether the replica is connected and ready for sending in the MessageBus.
    fn choose_any_other_replica(self: *Replica) ?u8 {
        var count: usize = 0;
        while (count < self.replica_count) : (count += 1) {
            self.choose_any_other_replica_ticks += 1;
            const replica = @mod(
                self.replica + self.choose_any_other_replica_ticks,
                self.replica_count,
            );
            if (replica == self.replica) continue;
            // TODO if (!MessageBus.can_send_to_replica(replica)) continue;
            return @intCast(u8, replica);
        }
        return null;
    }

    fn commit_ops_through(self: *Replica, commit: u64) void {
        // TODO Restrict `view_change` status only to the leader purely as defense-in-depth.
        // Be careful of concurrency when doing this, as successive view changes can happen quickly.
        assert(self.status == .normal or self.status == .view_change);
        assert(self.commit_min <= self.commit_max);
        assert(self.commit_min <= self.op);
        assert(self.commit_max <= self.op or self.commit_max > self.op);
        assert(commit <= self.op or commit > self.op);

        // We have already committed this far:
        if (commit <= self.commit_min) return;

        // Guard against multiple concurrent invocations of commit_ops_through():
        if (self.committing) {
            log.debug("{}: commit_ops_through: already committing...", .{self.replica});
            return;
        }

        self.committing = true;
        defer self.committing = false;

        if (commit > self.commit_max) {
            log.debug("{}: commit_ops_through: advancing commit_max={}..{}", .{
                self.replica,
                self.commit_max,
                commit,
            });
            self.commit_max = commit;
        }

        if (!self.valid_hash_chain("commit_ops_through")) return;
        assert(!self.view_jump_barrier);
        assert(self.op >= self.commit_max);

        // We may receive commit numbers for ops we do not yet have (`commit_max > self.op`):
        // Even a naive state transfer may fail to correct for this.
        while (self.commit_min < self.commit_max and self.commit_min < self.op) {
            const op = self.commit_min + 1;
            const checksum = self.journal.entry_for_op_exact(op).?.checksum;

            if (self.create_prepare_message(op, checksum)) |prepare| {
                defer self.message_bus.unref(prepare);

                // Things change quickly when we're reading from disk:
                if (self.status != .normal and self.status != .view_change) return;

                // Guard against any re-entrancy concurrent to reading this prepare from disk:
                assert(op == self.commit_min + 1);
                assert(prepare.header.op == op);
                assert(prepare.header.checksum == checksum);

                const commit_min = self.commit_min;
                self.commit_op(prepare);
                assert(self.commit_min == commit_min + 1);
                assert(self.commit_min <= self.op);
            } else {
                return;
            }
        }

        // This is an optimization to expedite the view change without waiting for `repair_timeout`:
        if (self.status == .view_change and self.repairs_allowed()) self.repair();
    }

    fn commit_op(self: *Replica, prepare: *const Message) void {
        assert(self.status == .normal or self.status == .view_change);
        assert(prepare.header.command == .prepare);
        assert(prepare.header.operation != .init);
        assert(prepare.header.op == self.commit_min + 1);
        assert(prepare.header.op <= self.op);

        if (!self.valid_hash_chain("commit_op")) return;
        assert(!self.view_jump_barrier);
        assert(self.op >= self.commit_max);

        const reply = self.message_bus.get_message() orelse {
            log.debug("{}: commit_op: waiting for a message", .{self.replica});
            return;
        };
        defer self.message_bus.unref(reply);

        const reply_body_size = @intCast(u32, self.state_machine.commit(
            prepare.header.operation.to_state_machine_op(StateMachine),
            prepare.buffer[@sizeOf(Header)..prepare.header.size],
            reply.buffer[@sizeOf(Header)..],
        ));

        log.debug("{}: commit_op: executing op={} checksum={} ({s})", .{
            self.replica,
            prepare.header.op,
            prepare.header.checksum,
            @tagName(prepare.header.operation),
        });

        self.commit_min += 1;
        assert(self.commit_min == prepare.header.op);
        if (self.commit_min > self.commit_max) self.commit_max = self.commit_min;

        reply.header.* = .{
            .command = .reply,
            .operation = prepare.header.operation,
            .nonce = prepare.header.checksum,
            .client = prepare.header.client,
            .request = prepare.header.request,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
            .op = prepare.header.op,
            .commit = prepare.header.op,
            .size = @sizeOf(Header) + reply_body_size,
        };
        assert(reply.header.offset == 0);
        assert(reply.header.epoch == 0);

        reply.header.set_checksum_body(reply.buffer[@sizeOf(Header)..reply.header.size]);
        reply.header.set_checksum();

        // TODO Add reply to the client table to answer future duplicate requests idempotently.
        // Lookup client table entry using client id.
        // If client's last request ID is <= this request ID, then update client table entry.
        // Otherwise the client is already ahead of us, and we don't need to update the entry.

        if (self.leader_index(self.view) == self.replica) {
            log.debug("{}: commit_op: replying to client: {}", .{ self.replica, reply.header });
            self.message_bus.send_message_to_client(reply.header.client, reply);
        }
    }

    fn count_quorum(self: *Replica, messages: []?*Message, command: Command, nonce: u128) usize {
        assert(messages.len == self.replica_count);

        var count: usize = 0;
        for (messages) |received, replica| {
            if (received) |m| {
                assert(m.header.command == command);
                assert(m.header.nonce == nonce);
                assert(m.header.cluster == self.cluster);
                assert(m.header.replica == replica);
                assert(m.header.view == self.view);
                switch (command) {
                    .prepare_ok => {},
                    .start_view_change => assert(m.header.replica != self.replica),
                    .do_view_change => {},
                    .nack_prepare => {
                        assert(m.header.replica != self.replica);
                        assert(m.header.op == self.nack_prepare_op.?);
                    },
                    else => unreachable,
                }
                count += 1;
            }
        }
        return count;
    }

    /// The caller owns the returned message, if any, which has exactly 1 reference.
    fn create_do_view_change_or_start_view_message(self: *Replica, command: Command) ?*Message {
        assert(command == .do_view_change or command == .start_view);

        // We may also send a start_view message in normal status to resolve a follower's view jump:
        assert(self.status == .normal or self.status == .view_change);

        const size_max = @sizeOf(Header) * 8;

        const message = self.message_bus.get_message() orelse return null;
        defer self.message_bus.unref(message);

        message.header.* = .{
            .command = command,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
            .op = self.op,
            .commit = self.commit_max,
        };

        var dest = std.mem.bytesAsSlice(Header, message.buffer[@sizeOf(Header)..size_max]);
        const count = self.journal.copy_latest_headers_between(0, self.op, dest);
        assert(count > 0);

        message.header.size = @intCast(u32, @sizeOf(Header) + @sizeOf(Header) * count);
        const body = message.buffer[@sizeOf(Header)..message.header.size];

        message.header.set_checksum_body(body);
        message.header.set_checksum();

        return message.ref();
    }

    /// The caller owns the returned message, if any, which has exactly 1 reference.
    fn create_prepare_message(self: *Replica, op: u64, checksum: ?u128) ?*Message {
        if (op > self.op) {
            self.create_prepare_message_notice(op, checksum, "beyond self.op");
            return null;
        }

        const exact = self.journal.entry_for_op_exact_with_checksum(op, checksum);
        if (exact == null) {
            self.create_prepare_message_notice(op, checksum, "no entry exactly");
            return null;
        }

        if (self.journal.faulty.bit(op)) {
            self.create_prepare_message_notice(op, checksum, "faulty");
            return null;
        }

        if (self.journal.dirty.bit(op)) {
            self.create_prepare_message_notice(op, checksum, "dirty");
            return null;
        }

        // Do not use this pointer beyond the read() below, as the header memory may then change:
        const entry = exact.?;

        const sector_size = @intCast(u32, Journal.sector_ceil(entry.size));
        assert(sector_size >= entry.size);

        const message = self.message_bus.get_message() orelse {
            self.create_prepare_message_notice(op, checksum, "no message available");
            return null;
        };
        defer self.message_bus.unref(message);

        // Skip the disk read if the header is all we need:
        if (entry.size == @sizeOf(Header)) {
            message.header.* = entry.*;
            return message.ref();
        }

        assert(entry.offset + sector_size <= self.journal.size_circular_buffer);
        self.journal.read_sectors(
            message.buffer[0..sector_size],
            self.journal.offset_in_circular_buffer(entry.offset),
        );

        if (!message.header.valid_checksum()) {
            self.create_prepare_message_notice(op, checksum, "corrupt header after read");
            return null;
        }

        const body = message.buffer[@sizeOf(Header)..message.header.size];
        if (!message.header.valid_checksum_body(body)) {
            self.create_prepare_message_notice(op, checksum, "corrupt body after read");
            return null;
        }

        if (message.header.op != op) {
            self.create_prepare_message_notice(op, checksum, "op changed during read");
            return null;
        }

        if (checksum != null and message.header.checksum != checksum.?) {
            self.create_prepare_message_notice(op, checksum, "checksum changed during read");
            return null;
        }

        return message.ref();
    }

    fn create_prepare_message_notice(
        self: *Replica,
        op: u64,
        checksum: ?u128,
        notice: []const u8,
    ) void {
        log.notice("{}: create_prepare_message: op={} checksum={}: {s}", .{
            self.replica,
            op,
            checksum,
            notice,
        });
    }

    fn discard_repair_queue(self: *Replica) void {
        while (self.repair_queue) |message| {
            log.notice("{}: discard_repair_queue: op={}", .{ self.replica, message.header.op });
            assert(self.repair_queue_len > 0);
            self.repair_queue = message.next;
            self.repair_queue_len -= 1;

            message.next = null;
            self.message_bus.unref(message);
        }
        assert(self.repair_queue_len == 0);
    }

    fn ignore_repair_message(self: *Replica, message: *const Message) bool {
        assert(message.header.command == .request_start_view or
            message.header.command == .request_headers or
            message.header.command == .request_prepare or
            message.header.command == .headers or
            message.header.command == .nack_prepare);

        const command: []const u8 = @tagName(message.header.command);

        if (self.status != .normal and self.status != .view_change) {
            log.debug("{}: on_{s}: ignoring ({})", .{ self.replica, command, self.status });
            return true;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_{s}: ignoring (older view)", .{ self.replica, command });
            return true;
        }

        // We should never view jump unless we know what our status should be after the jump:
        // Otherwise we may be normal before the leader, or in a view change that has completed.
        if (message.header.view > self.view) {
            log.debug("{}: on_{s}: ignoring (newer view)", .{ self.replica, command });
            return true;
        }

        if (self.ignore_repair_message_during_view_change(message)) return true;

        if (message.header.replica == self.replica) {
            log.warn("{}: on_{s}: ignoring (self)", .{ self.replica, command });
            return true;
        }

        if (self.leader_index(self.view) != self.replica) {
            switch (message.header.command) {
                // Only the leader may receive these messages:
                .request_start_view, .nack_prepare => {
                    log.warn("{}: on_{s}: ignoring (follower)", .{ self.replica, command });
                    return true;
                },
                // Only the leader may answer a request for a prepare without a nonce:
                .request_prepare => if (message.header.nonce == 0) {
                    log.warn("{}: on_{s}: ignoring (no nonce)", .{ self.replica, command });
                    return true;
                },
                else => {},
            }
        }

        if (message.header.command == .nack_prepare and self.status == .normal) {
            log.debug("{}: on_{s}: ignoring (view started)", .{ self.replica, command });
            return true;
        }

        // Only allow repairs for same view as defense-in-depth:
        assert(message.header.view == self.view);
        return false;
    }

    fn ignore_repair_message_during_view_change(self: *Replica, message: *const Message) bool {
        if (self.status != .view_change) return false;

        const command: []const u8 = @tagName(message.header.command);

        switch (message.header.command) {
            .request_start_view => {
                log.debug("{}: on_{s}: ignoring (view change)", .{ self.replica, command });
                return true;
            },
            .request_headers, .request_prepare => {
                if (self.leader_index(self.view) != message.header.replica) {
                    log.debug("{}: on_{s}: ignoring (view change, requested by follower)", .{
                        self.replica,
                        command,
                    });
                    return true;
                }
            },
            .headers, .nack_prepare => {
                if (self.leader_index(self.view) != self.replica) {
                    log.debug("{}: on_{s}: ignoring (view change, received by follower)", .{
                        self.replica,
                        command,
                    });
                    return true;
                } else if (!self.do_view_change_quorum) {
                    log.debug("{}: on_{s}: ignoring (view change, waiting for quorum)", .{
                        self.replica,
                        command,
                    });
                    return true;
                }
            },
            else => unreachable,
        }

        return false;
    }

    fn ignore_request_message(self: *Replica, message: *Message) bool {
        assert(message.header.command == .request);

        // We allow followers to reply to duplicate committed requests because we do not want to
        // ignore or forward a client request if we know that we have the reply in our client table.
        // We assume this is safe (even without regard to status below) since commits are immutable.
        // This improves latency and reduces traffic.
        // This may resend the reply if this is the latest committed request:
        if (self.ignore_request_message_duplicate(message)) return true;

        if (self.status != .normal) {
            log.debug("{}: on_request: ignoring ({s})", .{ self.replica, self.status });
            return true;
        }

        // This may forward the request to a newer leader:
        if (self.ignore_request_message_follower(message)) return true;

        return false;
    }

    /// Returns whether the request is stale or a duplicate of the latest committed request.
    /// Resends the reply to the latest request if the request has been committed.
    /// May be called by any replica in any status: `.normal`, `.view_change`, `.recovering`.
    fn ignore_request_message_duplicate(self: *Replica, message: *Message) bool {
        assert(message.header.command == .request);
        assert(message.header.client > 0);

        // The client stores the session number in the nonce of the header. This information is only
        // available to the active leader. There is not enough space in the header to propagate this
        // through to the prepare as we must repurpose the nonce to hash chain the previous prepare.
        const session = message.header.nonce;
        const request = message.header.request;

        if (message.header.operation == .register) {
            assert(session == 0);
            assert(request == 0);
        } else {
            assert(session > 0);
            assert(request > 0);
        }

        if (self.client_table.getPtr(message.header.client)) |entry| {
            // If we are going to drop duplicate requests or resend the latest committed reply,
            // then be sure that we do so for the correct request. There is alot at stake.
            assert(entry.reply.header.command == .reply);
            assert(message.header.client == entry.reply.header.client);

            if (session == 0) {
                assert(message.header.operation == .register);
                log.debug("{}: on_request: duplicate or stale .register request", .{self.replica});
                // Use the request number below to determine whether to resend the reply.
            } else if (session < entry.session) {
                // TODO Send eviction message to client.
                // We may be behind the cluster, but the client is definitely behind us.
                log.debug("{}: on_request: stale session", .{self.replica});
                return true;
            } else if (session > entry.session) {
                // The session number is committed information. At first glance, it might seem that
                // we may assert that the client's session is not newer than ours because the leader
                // always has all committed information. However, this function may be called by any
                // replica (leader or follower) in any status, so that we may not actually have all
                // committed information, and the client may well be ahead of us.
                // In other words, this function is an optimization:
                // Reply if we have the committed reply for this request, otherwise do nothing.
                return false;
            }

            if (request < entry.reply.header.request) {
                // Do nothing further with this request (e.g. do not forward to the leader).
                log.debug("{}: on_request: stale request", .{self.replica});
                return true;
            } else if (request == entry.reply.header.request) {
                assert(message.header.operation == entry.reply.header.operation);

                log.debug("{}: on_request: resending reply to duplicate request", .{self.replica});
                self.message_bus.send_message_to_client(message.header.client, entry.reply);
                return true;
            } else {
                // The client is ahead of us or about to make the next request.
                // TODO There is an optimization here where if we are the leader we could detect
                // that we have been partitioned from the cluster and drop the message instead of
                // flooding the network with prepares that will never be acked.
                // For example, if we are the leader and we see
                return false;
            }
        } else if (message.header.operation == .register) {
            // We always create a newer session for the client (if we are the leader).
            return false;
        } else {
            // Be sure that we have all commits to know whether or not the session has been evicted:
            // Otherwise we may send an eviction message for a session that is yet to be registered.
            if (self.status == .normal and self.leader()) {
                // TODO Send eviction message to client.
                // There is still the risk of sending an eviction message if we as the leader have
                // been partitioned, simply don't know about a session and the client talks to us.
                // We can ameliorate this risk by having clients include the view number and
                // rejecting messages from clients with newer views.
                log.debug("{}: on_request: no session", .{self.replica});
                return true;
            } else {
                // If this message will ever be queued anywhere, then be sure that this function is
                // evaluated again after the replica enters normal status, otherwise we may execute
                // the same request twice.
                return false;
            }
        }
    }

    /// Returns whether the replica is eligible to process this request as the leader.
    /// Takes the client's perspective into account if the client is aware of a newer view.
    /// Forwards requests to the leader if the client has an older view.
    fn ignore_request_message_follower(self: *Replica, message: *Message) bool {
        assert(message.header.command == .request);

        // The client is aware of a newer view:
        // Even if we think we are the leader, we may be partitioned from the rest of the cluster.
        // We therefore drop the message rather than flood our partition with traffic.
        if (message.header.view > self.view) {
            log.debug("{}: on_request: ignoring (newer view)", .{self.replica});
            return true;
        } else if (self.leader()) {
            return false;
        }

        if (message.header.operation == .register) {
            // We do not forward `.register` requests for the sake of `Header.peer_type()`.
            // This enables the MessageBus to identify client connections on the first message.
            log.debug("{}: on_request: ignoring (follower, register)", .{self.replica});
        } else if (message.header.view < self.view) {
            // The client may not know who the leader is, or may be retrying after a leader failure.
            // We forward to the new leader ahead of any client retry timeout to reduce latency.
            // Since the client is already connected to all replicas, the client will receive the
            // reply from the new leader directly.
            log.debug("{}: on_request: forwarding (follower)", .{self.replica});
            self.send_message_to_replica(self.leader_index(self.view), message);
        } else {
            assert(message.header.view == self.view);
            // The client has the correct view, but has retried against a follower.
            // This may mean that the leader is down and that we are about to do a view change.
            // There is also not much we can do as the client already knows who the leader is.
            // We do not forward as this would amplify traffic on the network.

            // TODO This may also indicate a client-leader partition. If we see enough of these,
            // should we trigger a view change to select a leader that clients can reach?
            // This is a question of weighing the probability of a partition vs routing error.
            log.debug("{}: on_request: ignoring (follower, same view)", .{self.replica});
        }

        assert(self.follower());
        return true;
    }

    fn ignore_view_change_message(self: *Replica, message: *const Message) bool {
        assert(message.header.command == .start_view_change or
            message.header.command == .do_view_change or
            message.header.command == .start_view);
        assert(message.header.view > 0); // The initial view is already zero.

        const command: []const u8 = @tagName(message.header.command);

        // 4.3 Recovery
        // While a replica's status is recovering it does not participate in either the request
        // processing protocol or the view change protocol.
        // This is critical for correctness (to avoid data loss):
        if (self.status == .recovering) {
            log.debug("{}: on_{s}: ignoring (recovering)", .{ self.replica, command });
            return true;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_{s}: ignoring (older view)", .{ self.replica, command });
            return true;
        }

        if (message.header.view == self.view and self.status == .normal) {
            if (message.header.command != .start_view or !self.view_jump_barrier) {
                log.debug("{}: on_{s}: ignoring (view started)", .{ self.replica, command });
                return true;
            }
        }

        // These may be caused by faults in the network topology.
        switch (message.header.command) {
            .start_view_change, .start_view => {
                if (message.header.replica == self.replica) {
                    log.warn("{}: on_{s}: ignoring (self)", .{ self.replica, command });
                    return true;
                }
            },
            .do_view_change => {
                if (self.leader_index(message.header.view) != self.replica) {
                    log.warn("{}: on_{s}: ignoring (follower)", .{ self.replica, command });
                    return true;
                }
            },
            else => unreachable,
        }

        return false;
    }

    fn is_repair(self: *Replica, message: *const Message) bool {
        assert(message.header.command == .prepare);

        if (self.status == .normal) {
            if (message.header.view < self.view) return true;
            if (message.header.view == self.view and message.header.op <= self.op) return true;
        } else if (self.status == .view_change) {
            if (message.header.view < self.view) return true;
            // The view has already started or is newer.
        }

        return false;
    }

    /// Advances `op` to where we need to be before `header` can be processed as a prepare:
    fn jump_to_newer_op_in_normal_status(self: *Replica, header: *const Header) void {
        assert(self.status == .normal);
        assert(self.follower());
        assert(header.view == self.view);
        assert(header.op > self.op + 1);
        // We may have learned of a higher `commit_max` through a commit message before jumping to a
        // newer op that is less than `commit_max` but greater than `commit_min`:
        assert(header.op > self.commit_min);

        log.debug("{}: jump_to_newer_op: advancing: op={}..{} checksum={}..{}", .{
            self.replica,
            self.op,
            header.op - 1,
            self.journal.entry_for_op_exact(self.op).?.checksum,
            header.nonce,
        });

        self.op = header.op - 1;
        assert(self.op >= self.commit_min);
        assert(self.op + 1 == header.op);
    }

    fn message_body_as_headers(self: *Replica, message: *const Message) []Header {
        // TODO Assert message commands that we expect this to be called for.
        assert(message.header.size > @sizeOf(Header)); // Body must contain at least one header.
        return std.mem.bytesAsSlice(Header, message.buffer[@sizeOf(Header)..message.header.size]);
    }

    /// Panics if immediate neighbors in the same view would have a broken hash chain.
    /// Assumes gaps and does not require that a preceeds b.
    fn panic_if_hash_chain_would_break_in_the_same_view(
        self: *Replica,
        a: *const Header,
        b: *const Header,
    ) void {
        assert(a.command == .prepare);
        assert(b.command == .prepare);
        assert(a.cluster == b.cluster);
        if (a.view == b.view and a.op + 1 == b.op and a.checksum != b.nonce) {
            assert(a.valid_checksum());
            assert(b.valid_checksum());
            log.emerg("{}: panic_if_hash_chain_would_break: a: {}", .{ self.replica, a });
            log.emerg("{}: panic_if_hash_chain_would_break: b: {}", .{ self.replica, b });
            @panic("hash chain would break");
        }
    }

    /// Starting from the latest journal entry, backfill any missing or disconnected headers.
    /// A header is disconnected if it breaks the hash chain with its newer neighbor to the right.
    /// Since we work backwards from the latest entry, we should always be able to fix the chain.
    /// Once headers are connected, backfill any dirty or faulty prepares.
    fn repair(self: *Replica) void {
        self.repair_timeout.reset();

        assert(self.status == .normal or self.status == .view_change);
        assert(self.repairs_allowed());
        assert(self.commit_min <= self.op);
        assert(self.commit_min <= self.commit_max);

        // TODO Handle case where we are requesting reordered headers that no longer exist.

        // We expect these always to exist:
        assert(self.journal.entry_for_op_exact(self.commit_min) != null);
        assert(self.journal.entry_for_op_exact(self.op) != null);

        // Resolve any view jump by requesting the leader's latest op:
        if (self.view_jump_barrier) {
            assert(self.status == .normal);
            assert(self.follower());
            log.notice("{}: repair: resolving view jump barrier", .{self.replica});
            self.send_header_to_replica(self.leader_index(self.view), .{
                .command = .request_start_view,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
            });
            return;
        }

        // Request outstanding committed prepares to advance our op number:
        // This handles the case of an idle cluster, where a follower will not otherwise advance.
        // This is not required for correctness, but for durability.
        if (self.op < self.commit_max) {
            // If the leader repairs during a view change, it will have already advanced `self.op`
            // to the latest op according to the quorum of `do_view_change` messages received, so we
            // must therefore be in normal status:
            assert(self.status == .normal);
            assert(self.follower());
            log.notice("{}: repair: op={} < commit_max={}", .{
                self.replica,
                self.op,
                self.commit_max,
            });
            // We need to advance our op number and therefore have to `request_prepare`,
            // since only `on_prepare()` can do this, not `repair_header()` in `on_headers()`.
            self.send_header_to_replica(self.leader_index(self.view), .{
                .command = .request_prepare,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .op = self.commit_max,
                // We cannot yet know the nonce so we set it to 0:
                // The nonce is optional when requesting from the leader but required otherwise.
                .nonce = 0,
            });
            return;
        }

        // Request any missing or disconnected headers:
        // TODO Snapshots: Ensure that self.commit_min op always exists in the journal.
        var broken = self.journal.find_latest_headers_break_between(self.commit_min, self.op);
        if (broken) |range| {
            log.notice("{}: repair: latest break: {}", .{ self.replica, range });
            assert(range.op_min > self.commit_min);
            assert(range.op_max < self.op);
            // A range of `op_min=0` or `op_max=0` should be impossible as a header break:
            // This is the init op that is prepared when the cluster is initialized.
            assert(range.op_min > 0);
            assert(range.op_max > 0);
            if (self.choose_any_other_replica()) |replica| {
                self.send_header_to_replica(replica, .{
                    .command = .request_headers,
                    .cluster = self.cluster,
                    .replica = self.replica,
                    .view = self.view,
                    .commit = range.op_min,
                    .op = range.op_max,
                });
            }
            return;
        }

        // Assert that all headers are now present and connected with a perfect hash chain:
        assert(!self.view_jump_barrier);
        assert(self.op >= self.commit_max);
        assert(self.valid_hash_chain_between(self.commit_min, self.op));

        // Request and repair any dirty or faulty prepares:
        if (self.journal.dirty.len > 0) return self.repair_prepares();

        // Commit ops, which may in turn discover faulty prepares and drive more repairs:
        if (self.commit_min < self.commit_max) return self.commit_ops_through(self.commit_max);

        if (self.status == .view_change and self.leader_index(self.view) == self.replica) {
            // TODO Drive uncommitted ops to completion through replication to a majority:
            if (self.commit_max < self.op) {
                log.debug("{}: repair: waiting for uncomitted ops to replicate", .{self.replica});
                return;
            }

            // Start the view as the new leader:
            self.start_view_as_the_new_leader();
        }
    }

    /// Decide whether or not to insert or update a header:
    ///
    /// A repair may never advance or replace `self.op` (critical for correctness):
    ///
    /// Repairs must always backfill in behind `self.op` but may never advance `self.op`.
    /// Otherwise, a split-brain leader may reapply an op that was removed through a view
    /// change, which could be committed by a higher `commit_max` number in a commit message.
    ///
    /// See this commit message for an example:
    /// https://github.com/coilhq/tigerbeetle/commit/6119c7f759f924d09c088422d5c60ac6334d03de
    ///
    /// Our guiding principles around repairs in general:
    ///
    /// * The latest op makes sense of everything else and must not be replaced with a different op
    /// or advanced except by the leader in the current view.
    ///
    /// * Do not jump to a view in normal status without imposing a view jump barrier.
    ///
    /// * Do not commit before resolving the view jump barrier with the leader.
    ///
    /// * Do not commit until the hash chain between `self.commit_min` and `self.op` is fully
    /// connected, to ensure that all the ops in this range are correct.
    ///
    /// * Ensure that `self.commit_max` is never advanced for a newer view without first imposing a
    /// view jump barrier, otherwise `self.commit_max` may again refer to different ops.
    ///
    /// * Ensure that `self.op` is never advanced by a repair since repairs may occur in a view
    /// change where the view has not yet started.
    ///
    /// * Do not assume that an existing op with a older viewstamp can be replaced by an op with a
    /// newer viewstamp, but only compare ops in the same view or with reference to the hash chain.
    /// See Figure 3.7 on page 41 in Diego Ongaro's Raft thesis for an example of where an op with
    /// an older view number may be committed instead of an op with a newer view number:
    /// http://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf.
    ///
    fn repair_header(self: *Replica, header: *const Header) bool {
        assert(header.valid_checksum());
        assert(header.invalid() == null);
        assert(header.command == .prepare);

        switch (self.status) {
            .normal => assert(header.view <= self.view),
            .view_change => assert(header.view < self.view),
            else => unreachable,
        }

        if (header.op > self.op) {
            log.debug("{}: repair_header: ignoring (would advance self.op)", .{self.replica});
            return false;
        } else if (header.op == self.op) {
            if (self.journal.entry_for_op_exact_with_checksum(self.op, header.checksum) == null) {
                log.debug("{}: repair_header: ignoring (would replace self.op)", .{self.replica});
                return false;
            }
        }

        if (self.journal.entry(header)) |existing| {
            // Do not replace any existing op lightly as doing so may impair durability and even
            // violate correctness by undoing a prepare already acknowledged to the leader:
            if (existing.checksum == header.checksum) {
                if (self.journal.dirty.bit(header.op)) {
                    // We may safely replace this existing op (with hash chain and overlap caveats):
                    log.debug("{}: repair_header: exists (dirty checksum)", .{self.replica});
                } else {
                    log.debug("{}: repair_header: ignoring (clean checksum)", .{self.replica});
                    return false;
                }
            } else if (existing.view == header.view) {
                // We expect that the same view and op must have the same checksum:
                assert(existing.op != header.op);

                // The journal must have wrapped:
                if (existing.op < header.op) {
                    // We may safely replace this existing op (with hash chain and overlap caveats):
                    log.debug("{}: repair_header: exists (same view, older op)", .{self.replica});
                } else if (existing.op > header.op) {
                    log.debug("{}: repair_header: ignoring (same view, newer op)", .{self.replica});
                    return false;
                } else {
                    unreachable;
                }
            } else {
                assert(existing.view != header.view);
                assert(existing.op == header.op or existing.op != header.op);

                if (self.repair_header_would_connect_hash_chain(header)) {
                    // We may safely replace this existing op:
                    log.debug("{}: repair_header: exists (hash chain break)", .{self.replica});
                } else {
                    // We cannot replace this existing op until we are sure that doing so would not
                    // violate any prior commitments made to the leader.
                    log.debug("{}: repair_header: ignoring (hash chain doubt)", .{self.replica});
                    return false;
                }
            }
        } else {
            // We may repair the gap (with hash chain and overlap caveats):
            log.debug("{}: repair_header: gap", .{self.replica});
        }

        // Caveat: Do not repair an existing op or gap if doing so would break the hash chain:
        if (self.repair_header_would_break_hash_chain_with_next_entry(header)) {
            log.debug("{}: repair_header: ignoring (would break hash chain)", .{self.replica});
            return false;
        }

        // Caveat: Do not repair an existing op or gap if doing so would overlap another:
        if (self.repair_header_would_overlap_another(header)) {
            if (self.repair_header_would_connect_hash_chain(header)) {
                // We may overlap previous entries in order to connect the hash chain:
                log.debug("{}: repair_header: overlap (would connect hash chain)", .{self.replica});
            } else {
                log.debug("{}: repair_header: ignoring (would overlap another)", .{self.replica});
                return false;
            }
        }

        // TODO Snapshots: Skip if this header is already snapshotted.

        assert(header.op < self.op or
            self.journal.entry_for_op_exact(self.op).?.checksum == header.checksum);

        self.journal.set_entry_as_dirty(header);
        return true;
    }

    /// If we repair this header, then would this break the hash chain only to our immediate right?
    /// This offers a weak guarantee compared to `repair_header_would_connect_hash_chain()` below.
    /// However, this is useful for allowing repairs when the hash chain is sparse.
    fn repair_header_would_break_hash_chain_with_next_entry(
        self: *Replica,
        header: *const Header,
    ) bool {
        if (self.journal.previous_entry(header)) |previous| {
            self.panic_if_hash_chain_would_break_in_the_same_view(previous, header);
        }

        if (self.journal.next_entry(header)) |next| {
            self.panic_if_hash_chain_would_break_in_the_same_view(header, next);

            if (header.checksum == next.nonce) {
                assert(header.view <= next.view);
                assert(header.op + 1 == next.op);
                // We don't break with `next` but this is no guarantee that `next` does not break.
                return false;
            } else {
                // If the journal has wrapped, then err in favor of a break regardless of op order:
                return true;
            }
        }

        // We are not completely sure since there is no entry to the immediate right:
        return false;
    }

    /// If we repair this header, then would this connect the hash chain through to the latest op?
    /// This offers a strong guarantee that may be used to replace or overlap an existing op.
    ///
    /// Here is an example of what could go wrong if we did not check for complete connection:
    ///
    /// 1. We do a prepare that's going to be committed.
    /// 2. We do a stale prepare to the right of this, ignoring the hash chain break to the left.
    /// 3. We do another stale prepare that replaces the first op because it connects to the second.
    ///
    /// This would violate our quorum replication commitment to the leader.
    /// The mistake in this example was not that we ignored the break to the left, which we must do
    /// to repair reordered ops, but that we did not check for complete connection to the right.
    fn repair_header_would_connect_hash_chain(self: *Replica, header: *const Header) bool {
        var entry = header;

        while (entry.op < self.op) {
            if (self.journal.next_entry(entry)) |next| {
                if (entry.checksum == next.nonce) {
                    assert(entry.view <= next.view);
                    assert(entry.op + 1 == next.op);
                    entry = next;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        assert(entry.op == self.op);
        assert(entry.checksum == self.journal.entry_for_op_exact(self.op).?.checksum);
        return true;
    }

    /// If we repair this header, then would this overlap and overwrite part of another batch?
    /// Journal entries have variable-sized batches that may overlap if entries are disconnected.
    fn repair_header_would_overlap_another(self: *Replica, header: *const Header) bool {
        // TODO Snapshots: Handle journal wrap around.
        {
            // Look behind this entry for any preceeding entry that this would overlap:
            var op: u64 = header.op;
            while (op > 0) {
                op -= 1;
                if (self.journal.entry_for_op(op)) |neighbor| {
                    if (self.journal.next_offset(neighbor) > header.offset) return true;
                    break;
                }
            }
        }
        {
            // Look beyond this entry for any succeeding entry that this would overlap:
            var op: u64 = header.op + 1;
            while (op <= self.op) : (op += 1) {
                if (self.journal.entry_for_op(op)) |neighbor| {
                    if (self.journal.next_offset(header) > neighbor.offset) return true;
                    break;
                }
            }
        }
        return false;
    }

    fn repair_last_queued_message_if_any(self: *Replica) void {
        if (self.status != .normal and self.status != .view_change) return;
        if (!self.repairs_allowed()) return;

        while (!self.repairing) {
            if (self.repair_queue) |message| {
                defer self.message_bus.unref(message);

                assert(self.repair_queue_len > 0);
                self.repair_queue = message.next;
                self.repair_queue_len -= 1;

                message.next = null;
                self.on_repair(message);
                assert(self.repair_queue != message); // Catch an accidental requeue by on_repair().
            } else {
                assert(self.repair_queue_len == 0);
                break;
            }
        }
    }

    fn repair_later(self: *Replica, message: *Message) void {
        assert(self.repairs_allowed());
        assert(self.appending or self.repairing);
        assert(message.references > 0);
        assert(message.header.command == .prepare);
        assert(message.next == null);

        if (!self.repairing) {
            log.debug("{}: repair_later: repairing immediately", .{self.replica});
            self.on_repair(message);
            return;
        }

        log.debug("{}: repair_later: {} message(s)", .{ self.replica, self.repair_queue_len });

        if (self.repair_queue_len == self.repair_queue_max) {
            log.debug("{}: repair_later: dropping", .{self.replica});
            return;
        }

        log.debug("{}: repair_later: queueing", .{self.replica});
        assert(self.repair_queue_len < self.repair_queue_max);

        message.next = self.repair_queue;
        self.repair_queue = message.ref();
        self.repair_queue_len += 1;
    }

    fn repair_prepares(self: *Replica) void {
        assert(self.status == .normal or self.status == .view_change);
        assert(self.repairs_allowed());
        assert(self.journal.dirty.len > 0);

        if (self.repair_queue_len == self.repair_queue_max) {
            log.debug("{}: repair_prepares: waiting for repair queue to drain", .{self.replica});
            return;
        }

        // We may be appending to or repairing the journal concurrently.
        // We do not want to re-request any of these prepares unnecessarily.
        // TODO Add journal.writing bits to clear this up (and needed anyway).
        if (self.appending or self.repairing) {
            log.debug("{}: repair_prepares: waiting for dirty bits to settle", .{self.replica});
            return;
        }

        // Request enough prepares to fill the repair queue:
        var budget = self.repair_queue_max - self.repair_queue_len;
        assert(budget > 0);

        var op = self.op + 1;
        while (op > 0) {
            op -= 1;

            if (self.journal.dirty.bit(op)) {
                // If this is an uncommitted op, and we are the leader in `view_change` status, then
                // we will `request_prepare` from the rest of the cluster, set `nack_prepare_op`,
                // and stop repairing any further prepares:
                // This will also rebroadcast any `request_prepare` every `repair_timeout` tick.
                self.repair_prepare(op);
                if (self.nack_prepare_op) |nack_prepare_op| {
                    assert(nack_prepare_op == op);
                    assert(self.status == .view_change);
                    assert(self.leader_index(self.view) == self.replica);
                    assert(op > self.commit_max);
                    return;
                }

                // Otherwise, we continue to request prepares until our budget is used up:
                budget -= 1;
                if (budget == 0) {
                    log.debug("{}: repair_prepares: request budget used up", .{self.replica});
                    return;
                }
            } else {
                assert(!self.journal.faulty.bit(op));
            }
        }
    }

    /// During a view change, for uncommitted ops, which may be one or two, we optimize for latency:
    ///
    /// * request a `prepare` or `nack_prepare` from all followers in parallel,
    /// * repair as soon as we receive a `prepare`, or
    /// * discard as soon as we receive a majority of `nack_prepare` messages for the same checksum.
    ///
    /// For committed ops, which likely represent the bulk of repairs, we optimize for throughput:
    ///
    /// * have multiple requests in flight to prime the repair queue,
    /// * rotate these requests across the cluster round-robin,
    /// * to spread the load across connected peers,
    /// * to take advantage of each peer's outgoing bandwidth, and
    /// * to parallelize disk seeks and disk read bandwidth.
    ///
    /// This is effectively "many-to-one" repair, where a single replica recovers using the
    /// resources of many replicas, for faster recovery.
    fn repair_prepare(self: *Replica, op: u64) void {
        assert(self.status == .normal or self.status == .view_change);
        assert(self.repairs_allowed());
        assert(self.journal.dirty.bit(op));

        const request_prepare = Header{
            .command = .request_prepare,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
            .op = op,
            // If we request a prepare from a follower, as below, it is critical to pass a checksum:
            // Otherwise we could receive different prepares for the same op number.
            .nonce = self.journal.entry_for_op_exact(op).?.checksum,
        };

        if (self.status == .view_change and op > self.commit_max) {
            // Only the leader is allowed to do repairs in a view change:
            assert(self.leader_index(self.view) == self.replica);

            // Initialize the `nack_prepare` quorum counter for this uncommitted op:
            // It is also possible that we may start repairing a lower uncommitted op, having
            // initialized `nack_prepare_op` before we learn of a higher uncommitted dirty op,
            // in which case we also want to reset the quorum counter.
            if (self.nack_prepare_op) |nack_prepare_op| {
                assert(nack_prepare_op <= op);
                if (nack_prepare_op != op) {
                    self.nack_prepare_op = op;
                    self.reset_quorum_counter(self.nack_prepare_from_other_replicas, .nack_prepare);
                }
            } else {
                self.nack_prepare_op = op;
                self.reset_quorum_counter(self.nack_prepare_from_other_replicas, .nack_prepare);
            }
            log.debug("{}: repair_prepare: requesting uncommitted op={}", .{ self.replica, op });
            assert(self.nack_prepare_op.? == op);
            assert(request_prepare.nonce != 0);
            self.send_header_to_other_replicas(request_prepare);
        } else {
            log.debug("{}: repair_prepare: requesting committed op={}", .{ self.replica, op });
            // We expect that `repair_prepare()` is called in reverse chronological order:
            // Any uncommitted ops should have already been dealt with.
            // We never roll back committed ops, and thus never regard any `nack_prepare` responses.
            assert(self.nack_prepare_op == null);
            assert(request_prepare.nonce != 0);
            if (self.choose_any_other_replica()) |replica| {
                self.send_header_to_replica(replica, request_prepare);
            }
        }
    }

    fn repairs_allowed(self: *Replica) bool {
        switch (self.status) {
            .view_change => {
                if (self.do_view_change_quorum) {
                    assert(self.leader_index(self.view) == self.replica);
                    return true;
                } else {
                    return false;
                }
            },
            .normal => return true,
            else => return false,
        }
    }

    /// Replicates to the next replica in the configuration (until we get back to the leader):
    /// Replication starts and ends with the leader, we never forward back to the leader.
    /// Does not flood the network with prepares that have already committed.
    /// TODO Use recent heartbeat data for next replica to leapfrog if faulty.
    fn replicate(self: *Replica, message: *Message) void {
        assert(self.status == .normal);
        assert(message.header.command == .prepare);
        assert(message.header.view == self.view);
        assert(message.header.op == self.op);

        if (message.header.op <= self.commit_max) {
            log.debug("{}: replicate: not replicating (committed)", .{self.replica});
            return;
        }

        const next = @mod(self.replica + 1, @intCast(u8, self.replica_count));
        if (next == self.leader_index(message.header.view)) {
            log.debug("{}: replicate: not replicating (completed)", .{self.replica});
            return;
        }

        log.debug("{}: replicate: replicating to replica {}", .{ self.replica, next });
        self.send_message_to_replica(next, message);
    }

    fn reset_quorum_counter(self: *Replica, messages: []?*Message, command: Command) void {
        var count: usize = 0;
        for (messages) |*received, replica| {
            if (received.*) |message| {
                assert(message.header.command == command);
                assert(message.header.replica == replica);
                assert(message.header.view <= self.view);
                self.message_bus.unref(message);
                count += 1;
            }
            received.* = null;
        }
        log.debug("{}: reset {} {s} message(s)", .{ self.replica, count, @tagName(command) });
    }

    fn reset_quorum_do_view_change(self: *Replica) void {
        self.reset_quorum_counter(self.do_view_change_from_all_replicas, .do_view_change);
        self.do_view_change_quorum = false;
    }

    fn reset_quorum_nack_prepare(self: *Replica) void {
        self.reset_quorum_counter(self.nack_prepare_from_other_replicas, .nack_prepare);
        self.nack_prepare_op = null;
    }

    fn reset_quorum_prepare(self: *Replica) void {
        if (self.prepare_message) |message| {
            self.request_checksum = null;
            self.message_bus.unref(message);
            self.prepare_message = null;
            self.prepare_attempt = 0;
            self.prepare_timeout.stop();
            self.reset_quorum_counter(self.prepare_ok_from_all_replicas, .prepare_ok);
        }
        assert(self.request_checksum == null);
        assert(self.prepare_message == null);
        assert(self.prepare_attempt == 0);
        assert(self.prepare_timeout.ticking == false);
        for (self.prepare_ok_from_all_replicas) |received| assert(received == null);
    }

    fn reset_quorum_start_view_change(self: *Replica) void {
        self.reset_quorum_counter(self.start_view_change_from_other_replicas, .start_view_change);
        self.start_view_change_quorum = false;
    }

    fn send_prepare_ok(self: *Replica, header: *const Header) void {
        assert(header.command == .prepare);
        assert(header.cluster == self.cluster);
        assert(header.replica == self.leader_index(header.view));
        assert(header.view <= self.view);
        assert(header.op <= self.op or header.view < self.view);

        if (self.status != .normal) {
            log.debug("{}: send_prepare_ok: not sending ({})", .{ self.replica, self.status });
            return;
        }

        if (header.op > self.op) {
            assert(header.view < self.view);
            // An op may be reordered concurrently through a view change while being journalled:
            log.debug("{}: send_prepare_ok: not sending (reordered)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        // After a view change followers must send prepare_oks for uncommitted ops with older views:
        // However, we will only ever send to the leader of our current view.
        assert(header.view <= self.view);
        assert(header.op <= self.op);

        if (header.op <= self.commit_max) {
            log.debug("{}: send_prepare_ok: not sending (committed)", .{self.replica});
            return;
        }

        // TODO Think through a scenario of where not doing this would be wrong.
        if (!self.valid_hash_chain("send_prepare_ok")) return;
        assert(!self.view_jump_barrier);
        assert(self.op >= self.commit_max);

        if (self.journal.has_clean(header)) {
            // It is crucial that replicas stop accepting prepare messages from earlier views once
            // they start the view change protocol. Without this constraint, the system could get
            // into a state in which there are two active primaries: the old one, which hasn't
            // failed but is merely slow or not well connected to the network, and the new one. If a
            // replica sent a prepare_ok message to the old primary after sending its log to the new
            // one, the old primary might commit an operation that the new primary doesn't learn
            // about in the do_view_change messages.

            // We therefore only send to the leader of the current view, never to the leader of the
            // prepare header's view:
            // TODO We could surprise the new leader with this, if it is preparing a different op.
            self.send_header_to_replica(self.leader_index(self.view), .{
                .command = .prepare_ok,
                .nonce = header.checksum,
                .client = header.client,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = header.view,
                .op = header.op,
                .commit = header.commit,
                .offset = header.offset,
                .epoch = header.epoch,
                .request = header.request,
                .operation = header.operation,
            });
        } else {
            log.debug("{}: send_prepare_ok: not sending (dirty)", .{self.replica});
            return;
        }
    }

    fn send_prepare_oks_through(self: *Replica, op: u64) void {
        assert(self.status == .normal);
        assert(op >= self.commit_max);
        assert(op <= self.op);

        if (!self.valid_hash_chain("send_prepare_oks_through")) return;
        assert(!self.view_jump_barrier);
        assert(self.op >= self.commit_max);

        while (op > self.commit_max and op < self.op) : (op += 1) {
            const header = self.journal.entry_for_op_exact(op).?;
            assert(header.op == op);
            assert(header.operation != .init);

            self.send_prepare_ok(header);
        }
    }

    fn send_prepare_to_replica(self: *Replica, replica: u8, op: u64, checksum: ?u128) void {
        assert(self.status == .normal or self.status == .view_change);
        assert(replica != self.replica);

        assert(!self.sending_prepare);
        self.sending_prepare = true;
        defer self.sending_prepare = false;

        if (self.create_prepare_message(op, checksum)) |message| {
            defer self.message_bus.unref(message);

            assert(message.header.op == op);
            assert(checksum == null or message.header.checksum == checksum.?);
            self.send_message_to_replica(replica, message);
        }
    }

    fn send_start_view_change(self: *Replica) void {
        assert(self.status == .view_change);
        assert(!self.do_view_change_quorum);
        // Send only to other replicas (and not to ourself) to avoid a quorum off-by-one error:
        // This could happen if the replica mistakenly counts its own message in the quorum.
        self.send_header_to_other_replicas(.{
            .command = .start_view_change,
            .cluster = self.cluster,
            .replica = self.replica,
            .view = self.view,
        });
    }

    fn send_do_view_change(self: *Replica) void {
        assert(self.status == .view_change);
        assert(self.start_view_change_quorum);
        assert(!self.do_view_change_quorum);
        const count_start_view_change = self.count_quorum(
            self.start_view_change_from_other_replicas,
            .start_view_change,
            0,
        );
        assert(count_start_view_change >= self.f);

        const message = self.create_do_view_change_or_start_view_message(.do_view_change) orelse {
            log.warn("{}: send_do_view_change: dropping do_view_change, no message available", .{
                self.replica,
            });
            return;
        };
        defer self.message_bus.unref(message);

        assert(message.references == 1);
        assert(message.header.command == .do_view_change);
        assert(message.header.view == self.view);
        assert(message.header.op == self.op);
        assert(message.header.commit == self.commit_max);
        // TODO Assert that latest header in message body matches self.op.

        self.send_message_to_replica(self.leader_index(self.view), message);
    }

    fn send_header_to_other_replicas(self: *Replica, header: Header) void {
        var replica: u8 = 0;
        while (replica < self.replica_count) : (replica += 1) {
            if (replica != self.replica) {
                self.send_header_to_replica(replica, header);
            }
        }
    }

    // TODO Work out the maximum number of messages a replica may output per tick() or on_message().
    fn send_header_to_replica(self: *Replica, replica: u8, header: Header) void {
        log.debug("{}: sending {s} to replica {}: {}", .{
            self.replica,
            @tagName(header.command),
            replica,
            header,
        });
        assert(header.replica == self.replica);
        assert(header.view == self.view);

        self.message_bus.send_header_to_replica(replica, header);
    }

    fn send_message_to_other_replicas(self: *Replica, message: *Message) void {
        var replica: u8 = 0;
        while (replica < self.replica_count) : (replica += 1) {
            if (replica != self.replica) {
                self.send_message_to_replica(replica, message);
            }
        }
    }

    fn send_message_to_replica(self: *Replica, replica: u8, message: *Message) void {
        log.debug("{}: sending {s} to replica {}: {}", .{
            self.replica,
            @tagName(message.header.command),
            replica,
            message.header,
        });
        switch (message.header.command) {
            .request => {
                // We do not assert message.header.replica as we would for send_header_to_replica()
                // because we may forward .request or .prepare messages.
                assert(self.status == .normal);
                assert(message.header.view <= self.view);
            },
            .prepare => {
                switch (self.status) {
                    .normal => assert(message.header.view <= self.view),
                    .view_change => assert(message.header.view < self.view),
                    else => unreachable,
                }
            },
            .do_view_change => {
                assert(self.status == .view_change);
                assert(self.start_view_change_quorum);
                assert(!self.do_view_change_quorum);
                assert(message.header.view == self.view);
            },
            .start_view => switch (self.status) {
                .normal => {
                    // A follower may ask the leader to resend the start_view message.
                    assert(!self.start_view_change_quorum);
                    assert(!self.do_view_change_quorum);
                    assert(message.header.view == self.view);
                },
                .view_change => {
                    assert(self.start_view_change_quorum);
                    assert(self.do_view_change_quorum);
                    assert(message.header.view == self.view);
                },
                else => unreachable,
            },
            .headers => {
                assert(self.status == .normal or self.status == .view_change);
                assert(message.header.view == self.view);
                assert(message.header.replica == self.replica);
            },
            else => unreachable,
        }
        assert(message.header.cluster == self.cluster);
        self.message_bus.send_message_to_replica(replica, message);
    }

    fn set_latest_header(self: *Replica, headers: []Header, latest: *Header) void {
        switch (latest.command) {
            .reserved, .prepare => assert(latest.valid_checksum()),
            else => unreachable,
        }

        for (headers) |header| {
            assert(header.valid_checksum());
            assert(header.invalid() == null);
            assert(header.command == .prepare);

            if (latest.command == .reserved) {
                latest.* = header;
            } else if (header.view > latest.view) {
                latest.* = header;
            } else if (header.view == latest.view and header.op > latest.op) {
                latest.* = header;
            }
        }
    }

    fn set_latest_op_and_k(self: *Replica, latest: *const Header, k: u64, method: []const u8) void {
        assert(self.status == .view_change);

        assert(latest.valid_checksum());
        assert(latest.invalid() == null);
        assert(latest.command == .prepare);
        assert(latest.cluster == self.cluster);
        assert(latest.view < self.view); // Latest normal view before this view change.
        // Ops may be rewound through a view change so we use `self.commit_max` and not `self.op`:
        assert(latest.op >= self.commit_max);
        // We expect that `commit_min` may be greater than `latest.commit` because the latter is
        // only the commit number at the time the latest op was prepared (but not committed).
        // We therefore only assert `latest.commit` against `commit_max` above and `k` below.

        assert(k >= latest.commit);
        assert(k >= self.commit_max);

        log.debug("{}: {s}: view={} op={}..{} commit={}..{} checksum={} offset={}", .{
            self.replica,
            method,
            self.view,
            self.op,
            latest.op,
            self.commit_max,
            k,
            latest.checksum,
            latest.offset,
        });

        self.op = latest.op;
        self.commit_max = k;

        // Do not set the latest op as dirty if we already have it exactly:
        // Otherwise, this would trigger a repair and delay the view change.
        if (self.journal.entry_for_op_exact_with_checksum(latest.op, latest.checksum) == null) {
            self.journal.set_entry_as_dirty(latest);
        } else {
            log.debug("{}: {s}: latest op exists exactly", .{ self.replica, method });
        }

        assert(self.op == latest.op);
        self.journal.remove_entries_from(self.op + 1);
        assert(self.journal.entry_for_op_exact(self.op).?.checksum == latest.checksum);
    }

    fn start_view_as_the_new_leader(self: *Replica) void {
        assert(self.status == .view_change);
        assert(self.leader_index(self.view) == self.replica);
        assert(self.do_view_change_quorum);

        // TODO Do one last count of our do_view_change quorum messages.

        assert(!self.view_jump_barrier);
        assert(self.commit_min == self.op);
        assert(self.commit_max == self.op);
        assert(self.valid_hash_chain_between(self.commit_min, self.op));

        assert(self.journal.dirty.len == 0);
        assert(self.journal.faulty.len == 0);
        assert(self.nack_prepare_op == null);

        const start_view = self.create_do_view_change_or_start_view_message(.start_view) orelse {
            log.warn("{}: start_view_as_the_new_leader: waiting for a message", .{self.replica});
            return;
        };
        defer self.message_bus.unref(start_view);

        self.transition_to_normal_status(self.view);

        assert(self.status == .normal);
        assert(self.leader());

        assert(start_view.references == 1);
        assert(start_view.header.command == .start_view);
        assert(start_view.header.view == self.view);
        assert(start_view.header.op == self.op);
        assert(start_view.header.commit == self.commit_max);

        self.send_message_to_other_replicas(start_view);
    }

    fn transition_to_normal_status(self: *Replica, new_view: u32) void {
        log.debug("{}: transition_to_normal_status: view={}", .{ self.replica, new_view });
        // In the VRR paper it's possible to transition from .normal to .normal for the same view.
        // For example, this could happen after a state transfer triggered by an op jump.
        assert(new_view >= self.view);
        self.view = new_view;
        self.status = .normal;

        if (self.leader()) {
            log.debug("{}: transition_to_normal_status: leader", .{self.replica});

            self.ping_timeout.start();
            self.commit_timeout.start();
            self.normal_timeout.stop();
            self.view_change_timeout.stop();
            self.view_change_message_timeout.stop();
            self.repair_timeout.start();
        } else {
            log.debug("{}: transition_to_normal_status: follower", .{self.replica});

            self.ping_timeout.start();
            self.commit_timeout.stop();
            self.normal_timeout.start();
            self.view_change_timeout.stop();
            self.view_change_message_timeout.stop();
            self.repair_timeout.start();
        }

        self.reset_quorum_prepare();
        self.reset_quorum_start_view_change();
        self.reset_quorum_do_view_change();
        self.reset_quorum_nack_prepare();

        assert(self.start_view_change_quorum == false);
        assert(self.do_view_change_quorum == false);
        assert(self.nack_prepare_op == null);
    }

    /// A replica i that notices the need for a view change advances its view, sets its status to
    /// view_change, and sends a ⟨start_view_change v, i⟩ message to all the other replicas,
    /// where v identifies the new view. A replica notices the need for a view change either based
    /// on its own timer, or because it receives a start_view_change or do_view_change message for
    /// a view with a larger number than its own view.
    fn transition_to_view_change_status(self: *Replica, new_view: u32) void {
        log.debug("{}: transition_to_view_change_status: view={}", .{ self.replica, new_view });
        assert(new_view > self.view);
        self.view = new_view;
        self.status = .view_change;

        self.ping_timeout.stop();
        self.commit_timeout.stop();
        self.normal_timeout.stop();
        self.view_change_timeout.start();
        self.view_change_message_timeout.start();
        self.repair_timeout.stop();

        // Do not reset quorum counters only on entering a view, assuming that the view will be
        // followed only by a single subsequent view change to the next view, because multiple
        // successive view changes can fail, e.g. after a view change timeout.
        // We must therefore reset our counters here to avoid counting messages from an older view,
        // which would violate the quorum intersection property essential for correctness.
        self.reset_quorum_prepare();
        self.reset_quorum_start_view_change();
        self.reset_quorum_do_view_change();
        self.reset_quorum_nack_prepare();

        assert(self.start_view_change_quorum == false);
        assert(self.do_view_change_quorum == false);
        assert(self.nack_prepare_op == null);

        self.send_start_view_change();
    }

    /// Whether it is safe to commit or send prepare_ok messages.
    /// Returns true if the hash chain is valid and up to date for the current view.
    /// This is a stronger guarantee than `valid_hash_chain_between()` below.
    fn valid_hash_chain(self: *Replica, method: []const u8) bool {
        // If we know we have uncommitted ops that may have been reordered through a view change
        // then wait until the latest of these has been resolved with the leader:
        if (self.view_jump_barrier) {
            log.notice("{}: {s}: waiting to resolve view jump barrier", .{ self.replica, method });
            return false;
        }

        // If we know we could validate the hash chain even further, then wait until we can:
        // This is partial defense-in-depth in case `self.op` is ever advanced by a reordered op.
        if (self.op < self.commit_max) {
            log.notice("{}: {s}: waiting for repair (op={} < commit={})", .{
                self.replica,
                method,
                self.op,
                self.commit_max,
            });
            return false;
        }

        // We must validate the hash chain as far as possible, since `self.op` may disclose a fork:
        if (!self.valid_hash_chain_between(self.commit_min, self.op)) {
            log.notice("{}: {s}: waiting for repair (hash chain)", .{ self.replica, method });
            return false;
        }

        return true;
    }

    /// Returns true if all operations are present, correctly ordered and connected by hash chain,
    /// between `op_min` and `op_max` (both inclusive).
    fn valid_hash_chain_between(self: *Replica, op_min: u64, op_max: u64) bool {
        assert(op_min <= op_max);

        // If we use anything less than self.op then we may commit ops for a forked hash chain that
        // have since been reordered by a new leader.
        assert(op_max == self.op);
        var b = self.journal.entry_for_op_exact(op_max).?;

        var op = op_max;
        while (op > op_min) {
            op -= 1;

            if (self.journal.entry_for_op_exact(op)) |a| {
                assert(a.op + 1 == b.op);
                if (a.checksum == b.nonce) {
                    assert(self.ascending_viewstamps(a, b));
                    b = a;
                } else {
                    log.notice("{}: valid_hash_chain_between: break: A: {}", .{ self.replica, a });
                    log.notice("{}: valid_hash_chain_between: break: B: {}", .{ self.replica, b });
                    return false;
                }
            } else {
                log.notice("{}: valid_hash_chain_between: missing op={}", .{ self.replica, op });
                return false;
            }
        }
        assert(b.op == op_min);
        return true;
    }

    fn view_jump(self: *Replica, header: *const Header) void {
        const to_status: Status = switch (header.command) {
            .prepare, .commit => .normal,
            .start_view_change, .do_view_change, .start_view => .view_change,
            else => unreachable,
        };

        if (self.status != .normal and self.status != .view_change) return;

        // If this is for an older view, then ignore:
        if (header.view < self.view) return;

        // Compare status transitions and decide whether to view jump or ignore:
        switch (self.status) {
            .normal => switch (to_status) {
                // If the transition is to `.normal`, then ignore if this is for the same view:
                .normal => if (header.view == self.view) return,
                // If the transition is to `.view_change`, then ignore if the view has started:
                .view_change => if (header.view == self.view) return,
                else => unreachable,
            },
            .view_change => switch (to_status) {
                // This is an interesting special case:
                // If the transition is to `.normal` in the same view, then we missed the
                // `start_view` message and we must also consider this a view jump:
                // If we don't view jump here, then our `view_change_timeout` will fire and we will
                // disrupt the cluster by starting another view change for a newer view.
                .normal => {},
                // If the transition is to `.view_change`, then ignore if this is for the same view:
                .view_change => if (header.view == self.view) return,
                else => unreachable,
            },
            else => unreachable,
        }

        if (to_status == .normal) {
            assert(header.view >= self.view);

            const command: []const u8 = @tagName(header.command);
            if (header.view == self.view) {
                assert(self.status == .view_change and to_status == .normal);
                log.debug("{}: view_jump: exiting view change and starting view", .{self.replica});
            } else {
                log.debug("{}: view_jump: jumping to newer view", .{self.replica});
            }

            if (self.op > self.commit_max) {
                // We have uncommitted ops, and these may have been removed or replaced by the new
                // leader through a view change in which we were not involved.
                //
                // In Section 5.2, the VR paper simply removes these uncommitted ops and does a
                // state transfer. However, while strictly safe, this impairs safety in terms of
                // durability, and adds unnecessary repair overhead if the ops were committed.
                //
                // We rather impose a view jump barrier to keep `commit_ops_through()` from
                // committing. This preserves and maximizes durability and minimizes repair traffic.
                //
                // This view jump barrier is cleared or may be resolved, respectively, as soon as:
                // 1. we receive a new prepare from the leader that advances our latest op, or
                // 2. we request and receive a `start_view` message from the leader for this view.
                //
                // This is safe because advancing our latest op in the current view or receiving the
                // latest op from the leader both ensure that we have the latest hash chain head.
                log.notice("{}: view_jump: imposing view jump barrier", .{self.replica});
                self.view_jump_barrier = true;
            } else {
                assert(self.op == self.commit_max);

                // We may still need to resolve any prior view jump barrier:
                // For example, if we jump to view 3 and jump again to view 7 both in normal status.
                assert(self.view_jump_barrier == true or self.view_jump_barrier == false);
            }
        } else if (to_status == .view_change) {
            assert(header.view > self.view);

            // The view change will set the latest op in on_do_view_change() or on_start_view():
            // There is no need to impose a view jump barrier and any existing barrier is cleared.
            // We only need to transition to view change status.
            if (self.view_jump_barrier) {
                log.notice("{}: view_jump: clearing view jump barrier", .{self.replica});
                self.view_jump_barrier = false;
            }
        } else {
            unreachable;
        }

        switch (to_status) {
            .normal => self.transition_to_normal_status(header.view),
            .view_change => self.transition_to_view_change_status(header.view),
            else => unreachable,
        }
    }

    fn write_to_journal(self: *Replica, message: *Message, lock: *bool) void {
        assert(lock.* == false);
        lock.* = true;
        defer lock.* = false;

        assert(message.references > 0);
        assert(message.header.command == .prepare);
        assert(message.header.view <= self.view);
        assert(message.header.op <= self.op);

        if (!self.journal.has(message.header)) {
            log.debug("{}: write_to_journal: ignoring (header changed)", .{self.replica});
            return;
        }

        if (self.journal.dirty.bit(message.header.op)) {
            self.journal.write(message);
        } else {
            // Any function that sets the faulty bit should also set the dirty bit:
            assert(!self.journal.faulty.bit(message.header.op));
            log.debug("{}: write_to_journal: skipping (clean)", .{self.replica});
            // Continue through below to send a prepare_ok message if necessary.
        }

        self.send_prepare_ok(message.header);

        // If this was a repair, continue immediately to repair the next prepare:
        // This is an optimization to eliminate waiting until the next repair timeout.
        if (lock == &self.repairing) self.repair();
    }
};
