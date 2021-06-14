const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vr);

const config = @import("config.zig");

/// The version of our Viewstamped Replication protocol in use, including customizations.
/// For backwards compatibility through breaking changes (e.g. upgrading checksums/ciphers).
pub const Version: u8 = 0;

pub const Replica = @import("vr/replica.zig").Replica;
pub const Clock = @import("vr/clock.zig").Clock;
pub const DeterministicTime = @import("vr/clock.zig").DeterministicTime;
pub const SystemTime = @import("vr/clock.zig").SystemTime;
pub const Journal = @import("vr/journal.zig").Journal;

/// Viewstamped Replication protocol commands:
pub const Command = packed enum(u8) {
    reserved,

    ping,
    pong,

    request,
    prepare,
    prepare_ok,
    reply,
    commit,

    start_view_change,
    do_view_change,
    start_view,

    request_start_view,
    request_headers,
    request_prepare,
    headers,
    nack_prepare,
};

/// This type exists to avoid making the Header type dependant on the state
/// machine used, which would cause awkward circular type dependencies.
pub const Operation = enum(u8) {
    /// Operations reserved by the VR protocol (for all state machines):
    ///
    /// The value 0 is reserved to prevent a spurious zero from being interpreted as an operation.
    reserved = 0,
    /// The value 1 is reserved to initialize the cluster.
    init = 1,
    /// The value 2 is reserved to register a client session with the cluster.
    register = 2,

    /// Operations exported by the state machine (all other values are free):
    _,

    pub fn to_state_machine_op(op: Operation, comptime StateMachine: type) StateMachine.Operation {
        check_state_machine_op_type(StateMachine.Operation);
        return @intToEnum(StateMachine.Operation, @enumToInt(op));
    }

    pub fn from_state_machine_op(comptime StateMachine: type, op: StateMachine.Operation) Operation {
        return @intToEnum(Operation, @enumToInt(op));
    }

    fn check_state_machine_op_type(comptime Op: type) void {
        if (!@hasField(Op, "reserved") or std.meta.fieldInfo(Op, .reserved).value != 0) {
            @compileError("StateMachine.Operation must have a 'reserved' field with value 0!");
        }
        if (!@hasField(Op, "init") or std.meta.fieldInfo(Op, .init).value != 1) {
            @compileError("StateMachine.Operation must have an 'init' field with value 1!");
        }
    }
};

/// Network message and journal entry header:
/// We reuse the same header for both so that prepare messages from the leader can simply be
/// journalled as is by the followers without requiring any further modification.
/// TODO Move from packed struct to extern struct for C ABI:
pub const Header = packed struct {
    comptime {
        assert(@sizeOf(Header) == 128);
    }
    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to recv() or read() the associated body.
    /// This checksum is enough to uniquely identify a network message or journal entry.
    checksum: u128 = 0,

    /// A checksum covering only the associated body after this header.
    checksum_body: u128 = 0,

    /// A backpointer to the previous request or prepare checksum for hash chain verification.
    /// This provides a cryptographic guarantee for linearizability:
    /// 1. across a client's requests, and
    /// 2. across the distributed log of prepares.
    /// This may also be used as the initialization vector for AEAD encryption at rest, provided
    /// that the leader ratchets the encryption key every view change to ensure that prepares
    /// reordered through a view change never repeat the same IV for the same encryption key.
    parent: u128 = 0,

    /// Each client process generates a unique, random and ephemeral client ID at initialization.
    /// The client ID identifies connections made by the client to the cluster for the sake of
    /// routing messages back to the client.
    ///
    /// With the client ID in hand, the client then registers a monotonically increasing session
    /// number (committed through the cluster) to allow the client's session to be evicted safely
    /// from the client table if too many concurrent clients cause the client table to overflow.
    /// The monotonically increasing session number prevents duplicate client requests from being
    /// replayed.
    ///
    /// The problem of routing is therefore solved by the 128-bit client ID, and the problem of
    /// detecting whether a session has been evicted is solved by the session number.
    client: u128 = 0,

    /// The checksum of the message to which this message refers, or a unique recovery nonce.
    ///
    /// We use this cryptographic context in various ways, for example:
    ///
    /// * A `request` sets this to the client's session number.
    /// * A `prepare` sets this to the checksum of the client's request.
    /// * A `prepare_ok` sets this to the checksum of the prepare being acked.
    /// * A `reply` sets this to that of the `prepare` for end-to-end integrity at the client.
    /// * A `commit` sets this to the checksum of the latest committed prepare.
    /// * A `request_prepare` sets this to the checksum of the prepare being requested.
    /// * A `nack_prepare` sets this to the checksum of the prepare being nacked.
    ///
    /// This allows for cryptographic guarantees beyond request, op, and commit numbers, which have
    /// low entropy and may otherwise collide in the event of any correctness bugs.
    context: u128 = 0,

    /// Each request is given a number by the client and later requests must have larger numbers
    /// than earlier ones. The request number is used by the replicas to avoid running requests more
    /// than once; it is also used by the client to discard duplicate responses to its requests.
    /// A client is allowed to have at most one request inflight at a time.
    request: u32 = 0,

    /// The cluster number binds intention into the header, so that a client or replica can indicate
    /// the cluster it believes it is speaking to, instead of accidentally talking to the wrong
    /// cluster (for example, staging vs production).
    cluster: u32,

    /// The cluster reconfiguration epoch number (for future use).
    epoch: u32 = 0,

    /// Every message sent from one replica to another contains the sending replica's current view.
    /// A `u32` allows for a minimum lifetime of 136 years at a rate of one view change per second.
    view: u32 = 0,

    /// The op number of the latest prepare that may or may not yet be committed. Uncommitted ops
    /// may be replaced by different ops if they do not survive through a view change.
    op: u64 = 0,

    /// The commit number of the latest committed prepare. Committed ops are immutable.
    commit: u64 = 0,

    /// The journal offset to which this message relates. This enables direct access to a prepare in
    /// storage, without yet having any previous prepares. All prepares are of variable size, since
    /// a prepare may contain any number of data structures (even if these are of fixed size).
    offset: u64 = 0,

    /// The size of the Header structure (always), plus any associated body.
    size: u32 = @sizeOf(Header),

    /// The index of the replica in the cluster configuration array that authored this message.
    /// This identifies only the ultimate author because messages may be forwarded amongst replicas.
    replica: u8 = 0,

    /// The Viewstamped Replication protocol command for this message.
    command: Command,

    /// The state machine operation to apply.
    operation: Operation = .reserved,

    /// The version of the protocol implementation that originated this message.
    version: u8 = Version,

    pub fn calculate_checksum(self: *const Header) u128 {
        const checksum_size = @sizeOf(@TypeOf(self.checksum));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(std.mem.asBytes(self)[checksum_size..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn calculate_checksum_body(self: *const Header, body: []const u8) u128 {
        assert(self.size == @sizeOf(Header) + body.len);
        const checksum_size = @sizeOf(@TypeOf(self.checksum_body));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(body[0..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    /// This must be called only after set_checksum_body() so that checksum_body is also covered:
    pub fn set_checksum(self: *Header) void {
        self.checksum = self.calculate_checksum();
    }

    pub fn set_checksum_body(self: *Header, body: []const u8) void {
        self.checksum_body = self.calculate_checksum_body(body);
    }

    pub fn valid_checksum(self: *const Header) bool {
        return self.checksum == self.calculate_checksum();
    }

    pub fn valid_checksum_body(self: *const Header, body: []const u8) bool {
        return self.checksum_body == self.calculate_checksum_body(body);
    }

    /// Returns null if all fields are set correctly according to the command, or else a warning.
    /// This does not verify that checksum is valid, and expects that this has already been done.
    pub fn invalid(self: *const Header) ?[]const u8 {
        if (self.version != Version) return "version != Version";
        if (self.size < @sizeOf(Header)) return "size < @sizeOf(Header)";
        if (self.epoch != 0) return "epoch != 0";
        return switch (self.command) {
            .reserved => self.invalid_reserved(),
            .request => self.invalid_request(),
            .prepare => self.invalid_prepare(),
            .prepare_ok => self.invalid_prepare_ok(),
            else => return null, // TODO Add validators for all commands.
        };
    }

    fn invalid_reserved(self: *const Header) ?[]const u8 {
        assert(self.command == .reserved);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.cluster != 0) return "cluster != 0";
        if (self.view != 0) return "view != 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.offset != 0) return "offset != 0";
        if (self.replica != 0) return "replica != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_request(self: *const Header) ?[]const u8 {
        assert(self.command == .request);
        if (self.parent != 0) return "parent != 0";
        if (self.client == 0) return "client == 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.offset != 0) return "offset != 0";
        if (self.replica != 0) return "replica != 0";
        switch (self.operation) {
            .reserved => return "operation == .reserved",
            .init => return "operation == .init",
            .register => {
                // The first request a client makes must be to register with the cluster:
                if (self.context != 0) return "context != 0";
                if (self.request != 0) return "request != 0";
            },
            else => {
                // Thereafter, the client must provide the session number in the context:
                if (self.context == 0) return "context == 0";
                if (self.request == 0) return "request == 0";
            },
        }
        return null;
    }

    fn invalid_prepare(self: *const Header) ?[]const u8 {
        assert(self.command == .prepare);
        switch (self.operation) {
            .reserved => return "operation == .reserved",
            .init => {
                if (self.parent != 0) return "init: parent != 0";
                if (self.client != 0) return "init: client != 0";
                if (self.context != 0) return "init: context != 0";
                if (self.request != 0) return "init: request != 0";
                if (self.view != 0) return "init: view != 0";
                if (self.op != 0) return "init: op != 0";
                if (self.commit != 0) return "init: commit != 0";
                if (self.offset != 0) return "init: offset != 0";
                if (self.size != @sizeOf(Header)) return "init: size != @sizeOf(Header)";
                if (self.replica != 0) return "init: replica != 0";
            },
            else => {
                if (self.client == 0) return "client == 0";
                if (self.op == 0) return "op == 0";
                if (self.op <= self.commit) return "op <= commit";
                if (self.operation == .register) {
                    // Client session numbers are replaced by the reference to the previous prepare.
                    if (self.request != 0) return "request != 0";
                } else {
                    // Client session numbers are replaced by the reference to the previous prepare.
                    if (self.request == 0) return "request == 0";
                }
            },
        }
        return null;
    }

    fn invalid_prepare_ok(self: *const Header) ?[]const u8 {
        assert(self.command == .prepare_ok);
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
        switch (self.operation) {
            .reserved => return "operation == .reserved",
            .init => {
                if (self.parent != 0) return "init: parent != 0";
                if (self.client != 0) return "init: client != 0";
                if (self.context != 0) return "init: context != 0";
                if (self.request != 0) return "init: request != 0";
                if (self.view != 0) return "init: view != 0";
                if (self.op != 0) return "init: op != 0";
                if (self.commit != 0) return "init: commit != 0";
                if (self.offset != 0) return "init: offset != 0";
                if (self.replica != 0) return "init: replica != 0";
            },
            else => {
                if (self.client == 0) return "client == 0";
                if (self.op == 0) return "op == 0";
                if (self.op <= self.commit) return "op <= commit";
                if (self.operation == .register) {
                    if (self.request != 0) return "request != 0";
                } else {
                    if (self.request == 0) return "request == 0";
                }
            },
        }
        return null;
    }

    /// Returns whether the immediate sender is a replica or client (if this can be determined).
    /// Some commands such as .request or .prepare may be forwarded on to other replicas so that
    /// Header.replica or Header.client only identifies the ultimate origin, not the latest peer.
    pub fn peer_type(self: *const Header) enum { unknown, replica, client } {
        switch (self.command) {
            .reserved => unreachable,
            // These messages cannot always identify the peer as they may be forwarded:
            .request => switch (self.operation) {
                // However, we do not forward the first .register request sent by a client:
                .register => return .client,
                else => return .unknown,
            },
            .prepare => return .unknown,
            // These messages identify the peer as either a replica or a client:
            // TODO Assert that pong responses from a replica do not echo the pinging client's ID.
            .ping, .pong => {
                if (self.client > 0) {
                    assert(self.replica == 0);
                    return .client;
                } else {
                    return .replica;
                }
            },
            // All other messages identify the peer as a replica:
            else => return .replica,
        }
    }

    pub fn reserved() Header {
        var header = Header{ .command = .reserved, .cluster = 0 };
        header.set_checksum_body(&[0]u8{});
        header.set_checksum();
        assert(header.invalid() == null);
        return header;
    }
};

pub const Timeout = struct {
    name: []const u8,
    /// TODO: get rid of this field as this is used by Client as well
    replica: u8,
    after: u64,
    ticks: u64 = 0,
    ticking: bool = false,

    /// It's important to check that when fired() is acted on that the timeout is stopped/started,
    /// otherwise further ticks around the event loop may trigger a thundering herd of messages.
    pub fn fired(self: *Timeout) bool {
        if (self.ticking and self.ticks >= self.after) {
            log.debug("{}: {s} fired", .{ self.replica, self.name });
            if (self.ticks > self.after) {
                log.emerg("{}: {s} is firing every tick", .{ self.replica, self.name });
                @panic("timeout was not reset correctly");
            }
            return true;
        } else {
            return false;
        }
    }

    pub fn reset(self: *Timeout) void {
        assert(self.ticking);
        self.ticks = 0;
        log.debug("{}: {s} reset", .{ self.replica, self.name });
    }

    pub fn start(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = true;
        log.debug("{}: {s} started", .{ self.replica, self.name });
    }

    pub fn stop(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = false;
        log.debug("{}: {s} stopped", .{ self.replica, self.name });
    }

    pub fn tick(self: *Timeout) void {
        if (self.ticking) self.ticks += 1;
    }
};

/// Returns An array containing the remote or local addresses of each of the 2f + 1 replicas:
/// Unlike the VRR paper, we do not sort the array but leave the order explicitly to the user.
/// There are several advantages to this:
/// * The operator may deploy a cluster with proximity in mind since replication follows order.
/// * A replica's IP address may be changed without reconfiguration.
/// This does require that the user specify the same order to all replicas.
/// The caller owns the memory of the returned slice of addresses.
/// TODO Unit tests.
/// TODO Integrate into `src/cli.zig`.
pub fn parse_configuration(allocator: *std.mem.Allocator, raw: []const u8) ![]std.net.Address {
    var addresses = try allocator.alloc(std.net.Address, config.replicas_max);
    errdefer allocator.free(addresses);

    var index: usize = 0;
    var comma_iterator = std.mem.split(raw, ",");
    while (comma_iterator.next()) |raw_address| : (index += 1) {
        if (raw_address.len == 0) return error.AddressHasTrailingComma;
        if (index == config.replicas_max) return error.AddressLimitExceeded;

        var colon_iterator = std.mem.split(raw_address, ":");
        // The split iterator will always return non-null once, even if the delimiter is not found:
        const raw_ipv4 = colon_iterator.next().?;

        if (colon_iterator.next()) |raw_port| {
            if (colon_iterator.next() != null) return error.AddressHasMoreThanOneColon;

            const port = std.fmt.parseUnsigned(u16, raw_port, 10) catch |err| switch (err) {
                error.Overflow => return error.PortOverflow,
                error.InvalidCharacter => return error.PortInvalid,
            };
            addresses[index] = std.net.Address.parseIp4(raw_ipv4, port) catch {
                return error.AddressInvalid;
            };
        } else {
            // There was no colon in the address so there are now two cases:
            // 1. an IPv4 address with the default port, or
            // 2. a port with the default IPv4 address.

            // Let's try parsing as a port first:
            if (std.fmt.parseUnsigned(u16, raw_address, 10)) |port| {
                addresses[index] = std.net.Address.parseIp4(config.address, port) catch unreachable;
            } else |err| switch (err) {
                error.Overflow => return error.PortOverflow,
                error.InvalidCharacter => {
                    // Something was not a digit, let's try parsing as an IPv4 instead:
                    addresses[index] = std.net.Address.parseIp4(raw_address, config.port) catch {
                        return error.AddressInvalid;
                    };
                },
            }
        }
    }
    return addresses[0..index];
}
