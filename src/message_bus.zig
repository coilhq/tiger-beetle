const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const os = std.os;

const config = @import("config.zig");
const log = std.log.scoped(.message_bus);

const vr = @import("vr.zig");
const Header = vr.Header;
const Journal = vr.Journal;
const Replica = vr.Replica;
const Client = @import("client.zig").Client;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const IO = @import("io.zig").IO;
const MessagePool = @import("message_pool.zig").MessagePool;

pub const Message = MessagePool.Message;

const SendQueue = RingBuffer(*Message, config.connection_send_queue_max);

pub const MessageBusReplica = MessageBusImpl(.replica);
pub const MessageBusClient = MessageBusImpl(.client);

const ProcessType = enum { replica, client };

fn MessageBusImpl(comptime process_type: ProcessType) type {
    return struct {
        const Self = @This();

        pool: MessagePool,
        io: *IO,

        cluster: u32,
        configuration: []std.net.Address,

        /// The Replica or Client process that will receive messages from this Self.
        process: switch (process_type) {
            .replica => struct {
                replica: *Replica,
                /// Used to store messages sent by a process to itself for delivery in flush().
                send_queue: SendQueue = .{},
                /// The file descriptor for the process on which to accept connections.
                accept_fd: os.socket_t,
                accept_completion: IO.Completion = undefined,
                /// The connection reserved for the currently in progress accept operation.
                /// This is non-null exactly when an accept operation is submitted.
                accept_connection: ?*Connection = null,
                /// Map from client id to the currently active connection for that client.
                /// This is used to make lookup of client connections when sending messages
                /// efficient and to ensure old client connections are dropped if a new one
                /// is established.
                clients: std.AutoHashMapUnmanaged(u128, *Connection) = .{},
            },
            .client => struct {
                client: *Client,
            },
        },

        /// This slice is allocated with a fixed size in the init function and never reallocated.
        connections: []Connection,
        /// Number of connections currently in use (i.e. connection.peer != .none).
        connections_used: usize = 0,

        /// Map from replica index to the currently active connection for that replica, if any.
        /// The connection for the process replica if any will always be null.
        replicas: []?*Connection,
        /// The number of outgoing `connect()` attempts for a given replica:
        /// Reset to zero after a successful `on_connect()`.
        replicas_connect_attempts: []u4,

        /// Used to apply full jitter when calculating exponential backoff:
        /// Seeded with the process' replica index or client ID.
        prng: std.rand.DefaultPrng,

        /// Initialize the Self for the given cluster, configuration and replica/client process.
        pub fn init(
            allocator: *mem.Allocator,
            cluster: u32,
            configuration: []std.net.Address,
            process: switch (process_type) {
                .replica => u8,
                .client => u128,
            },
            io: *IO,
        ) !Self {
            // There must be enough connections for all replicas and at least one client.
            assert(config.connections_max > configuration.len);

            const connections = try allocator.alloc(Connection, config.connections_max);
            errdefer allocator.free(connections);
            mem.set(Connection, connections, .{});

            const replicas = try allocator.alloc(?*Connection, configuration.len);
            errdefer allocator.free(replicas);
            mem.set(?*Connection, replicas, null);

            const replicas_connect_attempts = try allocator.alloc(u4, configuration.len);
            errdefer allocator.free(replicas_connect_attempts);
            mem.set(u4, replicas_connect_attempts, 0);

            const prng_seed = switch (process_type) {
                .replica => process,
                .client => @truncate(u64, process),
            };

            var self: Self = .{
                .pool = try MessagePool.init(allocator),
                .io = io,
                .cluster = cluster,
                .configuration = configuration,
                .process = switch (process_type) {
                    .replica => .{
                        .replica = undefined,
                        .accept_fd = try init_tcp(configuration[process]),
                    },
                    .client => .{ .client = undefined },
                },
                .connections = connections,
                .replicas = replicas,
                .replicas_connect_attempts = replicas_connect_attempts,
                .prng = std.rand.DefaultPrng.init(prng_seed),
            };

            // Pre-allocate enough memory to hold all possible connections in the client map.
            if (process_type == .replica) {
                try self.process.clients.ensureCapacity(allocator, config.connections_max);
            }

            return self;
        }

        /// TODO This is required by the Client.
        pub fn deinit(self: *Self) void {}

        fn init_tcp(address: std.net.Address) !os.socket_t {
            const fd = try os.socket(
                address.any.family,
                os.SOCK_STREAM | os.SOCK_CLOEXEC,
                os.IPPROTO_TCP,
            );
            errdefer os.close(fd);

            const set = struct {
                fn set(_fd: os.socket_t, level: u32, option: u32, value: c_int) !void {
                    try os.setsockopt(_fd, level, option, &mem.toBytes(value));
                }
            }.set;

            try set(fd, os.SOL_SOCKET, os.SO_REUSEADDR, 1);
            if (config.tcp_rcvbuf > 0) {
                // Requires CAP_NET_ADMIN privilege (settle for SO_RCVBUF in the event of an EPERM):
                set(fd, os.SOL_SOCKET, os.SO_RCVBUFFORCE, config.tcp_rcvbuf) catch |err| {
                    switch (err) {
                        error.PermissionDenied => {
                            try set(fd, os.SOL_SOCKET, os.SO_RCVBUF, config.tcp_rcvbuf);
                        },
                        else => return err,
                    }
                };
            }
            if (config.tcp_sndbuf > 0) {
                // Requires CAP_NET_ADMIN privilege (settle for SO_SNDBUF in the event of an EPERM):
                set(fd, os.SOL_SOCKET, os.SO_SNDBUFFORCE, config.tcp_sndbuf) catch |err| {
                    switch (err) {
                        error.PermissionDenied => {
                            try set(fd, os.SOL_SOCKET, os.SO_SNDBUF, config.tcp_sndbuf);
                        },
                        else => return err,
                    }
                };
            }
            if (config.tcp_keepalive) {
                try set(fd, os.SOL_SOCKET, os.SO_KEEPALIVE, 1);
                try set(fd, os.IPPROTO_TCP, os.TCP_KEEPIDLE, config.tcp_keepidle);
                try set(fd, os.IPPROTO_TCP, os.TCP_KEEPINTVL, config.tcp_keepintvl);
                try set(fd, os.IPPROTO_TCP, os.TCP_KEEPCNT, config.tcp_keepcnt);
            }
            if (config.tcp_user_timeout > 0) {
                try set(fd, os.IPPROTO_TCP, os.TCP_USER_TIMEOUT, config.tcp_user_timeout);
            }
            if (config.tcp_nodelay) {
                try set(fd, os.IPPROTO_TCP, os.TCP_NODELAY, 1);
            }

            try os.bind(fd, &address.any, address.getOsSockLen());
            try os.listen(fd, config.tcp_backlog);

            return fd;
        }

        pub fn tick(self: *Self) void {
            switch (process_type) {
                .replica => {
                    // Each replica is responsible for connecting to replicas that come
                    // after it in the configuration. This ensures that replicas never try
                    // to connect to each other at the same time.
                    var replica: u8 = self.process.replica.replica + 1;
                    while (replica < self.replicas.len) : (replica += 1) {
                        self.maybe_connect_to_replica(replica);
                    }

                    // Only replicas accept connections from other replicas and clients:
                    self.maybe_accept();

                    // Even though we call this after delivering all messages received over
                    // a socket, it is necessary to call here as well in case a replica sends
                    // a message to itself in Replica.tick().
                    self.flush_send_queue();
                },
                .client => {
                    // The client connects to all replicas.
                    var replica: u8 = 0;
                    while (replica < self.replicas.len) : (replica += 1) {
                        self.maybe_connect_to_replica(replica);
                    }
                },
            }
        }

        fn maybe_connect_to_replica(self: *Self, replica: u8) void {
            // We already have a connection to the given replica.
            if (self.replicas[replica] != null) {
                assert(self.connections_used > 0);
                return;
            }

            // Obtain a connection struct for our new replica connection.
            // If there is a free connection, use that. Otherwise drop
            // a client or unknown connection to make space. Prefer dropping
            // a client connection to an unknown one as the unknown peer may
            // be a replica. Since shutting a connection down does not happen
            // instantly, simply return after starting the shutdown and try again
            // on the next tick().
            for (self.connections) |*connection| {
                if (connection.state == .free) {
                    assert(connection.peer == .none);
                    // This will immediately add the connection to self.replicas,
                    // or else will return early if a socket file descriptor cannot be obtained:
                    // TODO See if we can clean this up to remove/expose the early return branch.
                    connection.connect_to_replica(self, replica);
                    return;
                }
            }

            // If there is already a connection being shut down, no need to kill another.
            for (self.connections) |*connection| {
                if (connection.state == .terminating) return;
            }

            log.notice("all connections in use but not all replicas are connected, " ++
                "attempting to disconnect a client", .{});
            for (self.connections) |*connection| {
                if (connection.peer == .client) {
                    connection.terminate(self, .shutdown);
                    return;
                }
            }

            log.notice("failed to disconnect a client as no peer was a known client, " ++
                "attempting to disconnect an unknown peer.", .{});
            for (self.connections) |*connection| {
                if (connection.peer == .unknown) {
                    connection.terminate(self, .shutdown);
                    return;
                }
            }

            // We assert that the max number of connections is greater
            // than the number of replicas in init().
            unreachable;
        }

        fn maybe_accept(self: *Self) void {
            comptime assert(process_type == .replica);

            if (self.process.accept_connection != null) return;
            // All connections are currently in use, do nothing.
            if (self.connections_used == self.connections.len) return;
            assert(self.connections_used < self.connections.len);
            self.process.accept_connection = for (self.connections) |*connection| {
                if (connection.state == .free) {
                    assert(connection.peer == .none);
                    connection.state = .accepting;
                    break connection;
                }
            } else unreachable;
            self.io.accept(
                *Self,
                self,
                on_accept,
                &self.process.accept_completion,
                self.process.accept_fd,
                os.SOCK_CLOEXEC,
            );
        }

        fn on_accept(
            self: *Self,
            completion: *IO.Completion,
            result: IO.AcceptError!os.socket_t,
        ) void {
            comptime assert(process_type == .replica);
            assert(self.process.accept_connection != null);
            defer self.process.accept_connection = null;
            const fd = result catch |err| {
                self.process.accept_connection.?.state = .free;
                // TODO: some errors should probably be fatal
                log.err("accept failed: {}", .{err});
                return;
            };
            self.process.accept_connection.?.on_accept(self, fd);
        }

        pub fn get_message(self: *Self) ?*Message {
            return self.pool.get_message();
        }

        pub fn unref(self: *Self, message: *Message) void {
            self.pool.unref(message);
        }

        /// Returns true if the target replica is connected and has space in its send queue.
        pub fn can_send_to_replica(self: *Self, replica: u8) bool {
            if (process_type == .replica and replica == self.process.replica.replica) {
                return !self.process.send_queue.full();
            } else {
                const connection = self.replicas[replica] orelse return false;
                return connection.state == .connected and !connection.send_queue.full();
            }
        }

        pub fn send_header_to_replica(self: *Self, replica: u8, header: Header) void {
            assert(header.size == @sizeOf(Header));

            if (!self.can_send_to_replica(replica)) {
                log.debug("cannot send to replica {}, dropping", .{replica});
                return;
            }

            const message = self.pool.get_header_only_message() orelse {
                log.debug("no header only message available, " ++
                    "dropping message to replica {}", .{replica});
                return;
            };
            defer self.unref(message);
            message.header.* = header;

            const body = message.buffer[@sizeOf(Header)..message.header.size];
            // The order matters here because checksum depends on checksum_body:
            message.header.set_checksum_body(body);
            message.header.set_checksum();

            self.send_message_to_replica(replica, message);
        }

        pub fn send_message_to_replica(self: *Self, replica: u8, message: *Message) void {
            // Messages sent by a process to itself are delivered directly in flush():
            if (process_type == .replica and replica == self.process.replica.replica) {
                self.process.send_queue.push(message.ref()) catch |err| switch (err) {
                    error.NoSpaceLeft => {
                        self.unref(message);
                        log.notice("process' message queue full, dropping message", .{});
                    },
                };
            } else if (self.replicas[replica]) |connection| {
                connection.send_message(self, message);
            } else {
                log.debug("no active connection to replica {}, " ++
                    "dropping message with header {}", .{ replica, message.header });
            }
        }

        pub fn send_header_to_client(self: *Self, client_id: u128, header: Header) void {
            assert(header.size == @sizeOf(Header));

            // TODO Do not allocate a message if we know we cannot send to the client.

            const message = self.pool.get_header_only_message() orelse {
                log.debug("no header only message available, " ++
                    "dropping message to client {}", .{client_id});
                return;
            };
            defer self.unref(message);
            message.header.* = header;

            const body = message.buffer[@sizeOf(Header)..message.header.size];
            // The order matters here because checksum depends on checksum_body:
            message.header.set_checksum_body(body);
            message.header.set_checksum();

            self.send_message_to_client(client_id, message);
        }

        /// Try to send the message to the client with the given id.
        /// If the client is not currently connected, the message is silently dropped.
        pub fn send_message_to_client(self: *Self, client_id: u128, message: *Message) void {
            comptime assert(process_type == .replica);

            if (self.process.clients.get(client_id)) |connection| {
                connection.send_message(self, message);
            } else {
                log.debug("no connection to client {x}", .{client_id});
            }
        }

        /// Deliver messages the replica has sent to itself.
        pub fn flush_send_queue(self: *Self) void {
            comptime assert(process_type == .replica);

            // There are currently 3 cases in which a replica will send a message to itself:
            // 1. In on_request, the leader sends a prepare to itself, and
            //    subsequent prepare timeout retries will never resend to self.
            // 2. In on_prepare, after writing to storage, the leader sends a
            //    prepare_ok back to itself.
            // 3. In on_start_view_change, after receiving a quorum of start_view_change
            //    messages, the new leader sends a do_view_change to itself.
            // Therefore we should never enter an infinite loop here. To catch the case in which we
            // do so that we can learn from it, assert that we never iterate more than 100 times.
            var i: usize = 0;
            while (i < 100) : (i += 1) {
                if (self.process.send_queue.empty()) return;

                var copy = self.process.send_queue;
                self.process.send_queue = .{};

                while (copy.pop()) |message| {
                    defer self.unref(message);
                    // TODO Rewrite ConcurrentRanges to not use async:
                    // This nosuspend is only safe because we do not do any async disk IO yet.
                    nosuspend await async self.process.replica.on_message(message);
                }
            }
            unreachable;
        }

        /// Calculates exponential backoff with full jitter according to the formula:
        /// `sleep = random_between(0, min(cap, base * 2 ** attempt))`
        ///
        /// `attempt` is zero-based.
        /// `attempt` is tracked as a u4 to flush out any overflow bugs sooner rather than later.
        pub fn exponential_backoff_with_full_jitter_in_ms(self: *Self, attempt: u4) u63 {
            assert(config.connection_delay_min < config.connection_delay_max);
            // Calculate the capped exponential backoff component: `min(cap, base * 2 ** attempt)`
            const base: u63 = config.connection_delay_min;
            const cap: u63 = config.connection_delay_max - config.connection_delay_min;
            const exponential_backoff = std.math.min(
                cap,
                // A "1" shifted left gives any power of two:
                // 1<<0 = 1, 1<<1 = 2, 1<<2 = 4, 1<<3 = 8:
                base * std.math.shl(u63, 1, attempt),
            );
            const jitter = self.prng.random.uintAtMostBiased(u63, exponential_backoff);
            const ms = base + jitter;
            assert(ms >= config.connection_delay_min);
            assert(ms <= config.connection_delay_max);
            return ms;
        }

        /// Used to send/receive messages to/from a client or fellow replica.
        const Connection = struct {
            /// The peer is determined by inspecting the first message header
            /// received.
            peer: union(enum) {
                /// No peer is currently connected.
                none: void,
                /// A connection is established but an unambiguous header has not yet been received.
                unknown: void,
                /// The peer is a client with the given id.
                client: u128,
                /// The peer is a replica with the given id.
                replica: u8,
            } = .none,
            state: enum {
                /// The connection is not in use, with peer set to `.none`.
                free,
                /// The connection has been reserved for an in progress accept operation,
                /// with peer set to `.none`.
                accepting,
                /// The peer is a replica and a connect operation has been started
                /// but not yet competed.
                connecting,
                /// The peer is fully connected and may be a client, replica, or unknown.
                connected,
                /// The connection is being terminated but cleanup has not yet finished.
                terminating,
            } = .free,
            /// This is guaranteed to be valid only while state is connected.
            /// It will be reset to -1 during the shutdown process and is always -1 if the
            /// connection is unused (i.e. peer == .none). We use -1 instead of undefined here
            /// for safety to ensure an error if the invalid value is ever used, instead of
            /// potentially performing an action on an active fd.
            fd: os.socket_t = -1,

            /// This completion is used for all recv operations.
            /// It is also used for the initial connect when establishing a replica connection.
            recv_completion: IO.Completion = undefined,
            /// True exactly when the recv_completion has been submitted to the IO abstraction
            /// but the callback has not yet been run.
            recv_submitted: bool = false,
            /// The Message with the buffer passed to the kernel for recv operations.
            recv_message: ?*Message = null,
            /// The number of bytes in `recv_message` that have been received and need parsing.
            recv_progress: usize = 0,
            /// The number of bytes in `recv_message` that have been parsed.
            recv_parsed: usize = 0,
            /// True if we have already checked the header checksum of the message we
            /// are currently receiving/parsing.
            recv_checked_header: bool = false,

            /// This completion is used for all send operations.
            send_completion: IO.Completion = undefined,
            /// True exactly when the send_completion has been submitted to the IO abstraction
            /// but the callback has not yet been run.
            send_submitted: bool = false,
            /// Number of bytes of the current message that have already been sent.
            send_progress: usize = 0,
            /// The queue of messages to send to the client or replica peer.
            send_queue: SendQueue = .{},

            /// Attempt to connect to a replica.
            /// The slot in the Message.replicas slices is immediately reserved.
            /// Failure is silent and returns the connection to an unused state.
            pub fn connect_to_replica(self: *Connection, bus: *Self, replica: u8) void {
                if (process_type == .replica) assert(replica != bus.process.replica.replica);

                assert(self.peer == .none);
                assert(self.state == .free);
                assert(self.fd == -1);

                // The first replica's network address family determines the
                // family for all other replicas:
                const family = bus.configuration[0].any.family;
                self.fd = os.socket(family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0) catch return;
                self.peer = .{ .replica = replica };
                self.state = .connecting;
                bus.connections_used += 1;

                assert(bus.replicas[replica] == null);
                bus.replicas[replica] = self;

                var attempts = &bus.replicas_connect_attempts[replica];
                const ms = bus.exponential_backoff_with_full_jitter_in_ms(attempts.*);
                // Saturate the counter at its maximum value if the addition wraps:
                attempts.* +%= 1;
                if (attempts.* == 0) attempts.* -%= 1;

                log.debug("connecting to replica {} in {}ms...", .{ self.peer.replica, ms });

                assert(!self.recv_submitted);
                self.recv_submitted = true;

                bus.io.timeout(
                    *Self,
                    bus,
                    on_connect_with_exponential_backoff,
                    // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                    &self.recv_completion,
                    ms * std.time.ns_per_ms,
                );
            }

            fn on_connect_with_exponential_backoff(
                bus: *Self,
                completion: *IO.Completion,
                result: IO.TimeoutError!void,
            ) void {
                const self = @fieldParentPtr(Connection, "recv_completion", completion);
                assert(self.recv_submitted);
                self.recv_submitted = false;
                if (self.state == .terminating) {
                    self.maybe_close(bus);
                    return;
                }
                assert(self.state == .connecting);
                result catch unreachable;

                log.debug("connecting to replica {}...", .{self.peer.replica});

                assert(!self.recv_submitted);
                self.recv_submitted = true;

                bus.io.connect(
                    *Self,
                    bus,
                    on_connect,
                    // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                    &self.recv_completion,
                    self.fd,
                    bus.configuration[self.peer.replica],
                );
            }

            fn on_connect(
                bus: *Self,
                completion: *IO.Completion,
                result: IO.ConnectError!void,
            ) void {
                const self = @fieldParentPtr(Connection, "recv_completion", completion);
                assert(self.recv_submitted);
                self.recv_submitted = false;

                if (self.state == .terminating) {
                    self.maybe_close(bus);
                    return;
                }
                assert(self.state == .connecting);
                self.state = .connected;

                result catch |err| {
                    log.err("error connecting to replica {}: {}", .{ self.peer.replica, err });
                    self.terminate(bus, .close);
                    return;
                };

                log.info("connected to replica {}", .{self.peer.replica});
                bus.replicas_connect_attempts[self.peer.replica] = 0;

                self.assert_recv_send_initial_state(bus);
                // This will terminate the connection if there are no messages available:
                self.get_recv_message_and_recv(bus);
                // A message may have been queued for sending while we were connecting:
                // TODO Should we relax recv() and send() to return if `self.state != .connected`?
                if (self.state == .connected) self.send(bus);
            }

            /// Given a newly accepted fd, start receiving messages on it.
            /// Callbacks will be continuously re-registered until terminate() is called.
            pub fn on_accept(self: *Connection, bus: *Self, fd: os.socket_t) void {
                assert(self.peer == .none);
                assert(self.state == .accepting);
                assert(self.fd == -1);

                self.peer = .unknown;
                self.state = .connected;
                self.fd = fd;
                bus.connections_used += 1;

                self.assert_recv_send_initial_state(bus);
                self.get_recv_message_and_recv(bus);
                assert(self.send_queue.empty());
            }

            fn assert_recv_send_initial_state(self: *Connection, bus: *Self) void {
                assert(bus.connections_used > 0);

                assert(self.peer == .unknown or self.peer == .replica);
                assert(self.state == .connected);
                assert(self.fd != -1);

                assert(self.recv_submitted == false);
                assert(self.recv_message == null);
                assert(self.recv_progress == 0);
                assert(self.recv_parsed == 0);

                assert(self.send_submitted == false);
                assert(self.send_progress == 0);
            }

            /// Add a message to the connection's send queue, starting a send operation
            /// if the queue was previously empty.
            pub fn send_message(self: *Connection, bus: *Self, message: *Message) void {
                assert(self.peer == .client or self.peer == .replica);
                switch (self.state) {
                    .connected, .connecting => {},
                    .terminating => return,
                    .free, .accepting => unreachable,
                }
                self.send_queue.push(message.ref()) catch |err| switch (err) {
                    error.NoSpaceLeft => {
                        bus.unref(message);
                        log.notice("message queue for peer {} full, dropping message", .{
                            self.peer,
                        });
                        return;
                    },
                };
                // If the connection has not yet been established we can't send yet.
                // Instead on_connect() will call send().
                if (self.state == .connecting) {
                    assert(self.peer == .replica);
                    return;
                }
                // If there is no send operation currently in progress, start one.
                if (!self.send_submitted) self.send(bus);
            }

            /// Clean up an active connection and reset it to its initial, unused, state.
            /// This reset does not happen instantly as currently in progress operations
            /// must first be stopped. The `how` arg allows the caller to specify if a
            /// shutdown syscall should be made or not before proceeding to wait for
            /// currently in progress operations to complete and close the socket.
            /// I'll be back! (when the Connection is reused after being fully closed)
            pub fn terminate(self: *Connection, bus: *Self, how: enum { shutdown, close }) void {
                assert(self.peer != .none);
                assert(self.state != .free);
                assert(self.fd != -1);
                switch (how) {
                    .shutdown => {
                        // The shutdown syscall will cause currently in progress send/recv
                        // operations to be gracefully closed while keeping the fd open.
                        const rc = os.linux.shutdown(self.fd, os.SHUT_RDWR);
                        switch (os.errno(rc)) {
                            0 => {},
                            os.EBADF => unreachable,
                            os.EINVAL => unreachable,
                            os.ENOTCONN => {
                                // This should only happen if we for some reason decide to terminate
                                // a connection while a connect operation is in progress.
                                // This is fine though, we simply continue with the logic below and
                                // wait for the connect operation to finish.

                                // TODO: This currently happens in other cases if the
                                // connection was closed due to an error. We need to intelligently
                                // decide whether to shutdown or close directly based on the error
                                // before these assertions may be re-enabled.

                                //assert(self.state == .connecting);
                                //assert(self.recv_submitted);
                                //assert(!self.send_submitted);
                            },
                            os.ENOTSOCK => unreachable,
                            else => |err| os.unexpectedErrno(err) catch {},
                        }
                    },
                    .close => {},
                }
                assert(self.state != .terminating);
                self.state = .terminating;
                self.maybe_close(bus);
            }

            fn parse_messages(self: *Connection, bus: *Self) void {
                assert(self.peer != .none);
                assert(self.state == .connected);
                assert(self.fd != -1);

                while (self.parse_message(bus)) |message| {
                    defer bus.unref(message);

                    self.on_message(bus, message);
                }
            }

            fn parse_message(self: *Connection, bus: *Self) ?*Message {
                const data = self.recv_message.?.buffer[self.recv_parsed..self.recv_progress];
                if (data.len < @sizeOf(Header)) {
                    self.get_recv_message_and_recv(bus);
                    return null;
                }

                const header = mem.bytesAsValue(Header, data[0..@sizeOf(Header)]);
                if (!self.recv_checked_header) {
                    if (!header.valid_checksum()) {
                        log.err("invalid header checksum received from {}", .{self.peer});
                        self.terminate(bus, .shutdown);
                        return null;
                    }

                    if (header.size < @sizeOf(Header) or header.size > config.message_size_max) {
                        log.err("header with invalid size {d} received from peer {}", .{
                            header.size,
                            self.peer,
                        });
                        self.terminate(bus, .shutdown);
                        return null;
                    }

                    if (header.cluster != bus.cluster) {
                        log.err("message addressed to the wrong cluster: {}", .{header.cluster});
                        self.terminate(bus, .shutdown);
                        return null;
                    }

                    switch (process_type) {
                        // Replicas may forward messages from clients or from other replicas so we
                        // may receive messages from a peer before we know who they are:
                        // This has the same effect as an asymmetric network where, for a short time
                        // bounded by the time it takes to ping, we can hear from a peer before we
                        // can send back to them.
                        .replica => self.maybe_set_peer(bus, header),
                        // The client connects only to replicas and should set peer when connecting:
                        .client => assert(self.peer == .replica),
                    }

                    self.recv_checked_header = true;
                }

                if (data.len < header.size) {
                    self.get_recv_message_and_recv(bus);
                    return null;
                }

                // At this point we know that we have the full message in our buffer.
                // We will now either deliver this message or terminate the connection
                // due to an error, so reset recv_checked_header for the next message.
                assert(self.recv_checked_header);
                self.recv_checked_header = false;

                const body = data[@sizeOf(Header)..header.size];
                if (!header.valid_checksum_body(body)) {
                    log.err("invalid body checksum received from {}", .{self.peer});
                    self.terminate(bus, .shutdown);
                    return null;
                }

                self.recv_parsed += header.size;

                // Return the parsed message using zero-copy if we can, or copy if the client is
                // pipelining:
                // If this is the first message but there are messages in the pipeline then we
                // copy the message so that its sector padding (if any) will not overwrite the
                // front of the pipeline.  If this is not the first message then we must copy
                // the message to a new message as each message needs to have its own unique
                // `references` and `header` metadata.
                if (self.recv_progress == header.size) return self.recv_message.?.ref();

                const message = bus.get_message() orelse {
                    // TODO Decrease the probability of this happening by:
                    // 1. using a header-only message if possible.
                    // 2. determining a true upper limit for static allocation.
                    log.err("no free buffer available to deliver message from {}", .{self.peer});
                    self.terminate(bus, .shutdown);
                    return null;
                };
                mem.copy(u8, message.buffer, data[0..header.size]);
                return message;
            }

            /// Forward a received message to `Process.on_message()`.
            /// Zero any `.prepare` sector padding up to the nearest sector multiple after the body.
            fn on_message(self: *Connection, bus: *Self, message: *Message) void {
                if (message == self.recv_message.?) {
                    assert(self.recv_parsed == message.header.size);
                    assert(self.recv_parsed == self.recv_progress);
                } else if (self.recv_parsed == message.header.size) {
                    assert(self.recv_parsed < self.recv_progress);
                } else {
                    assert(self.recv_parsed > message.header.size);
                    assert(self.recv_parsed <= self.recv_progress);
                }

                if (message.header.command == .request or message.header.command == .prepare) {
                    const sector_ceil = Journal.sector_ceil(message.header.size);
                    if (message.header.size != sector_ceil) {
                        assert(message.header.size < sector_ceil);
                        assert(message.buffer.len == config.message_size_max + config.sector_size);
                        mem.set(u8, message.buffer[message.header.size..sector_ceil], 0);
                    }
                }

                switch (process_type) {
                    .replica => {
                        // TODO Rewrite ConcurrentRanges to not use async:
                        // This nosuspend is only safe because we do not do any async disk IO yet.
                        nosuspend await async bus.process.replica.on_message(message);
                        // Flush any messages queued by `process.on_message()` above immediately:
                        // This optimization is critical for throughput, otherwise messages from a
                        // process to itself would be delayed until the next `tick()`.
                        bus.flush_send_queue();
                    },
                    .client => bus.process.client.on_message(message),
                }
            }

            fn maybe_set_peer(self: *Connection, bus: *Self, header: *const Header) void {
                comptime assert(process_type == .replica);

                assert(bus.cluster == header.cluster);
                assert(bus.connections_used > 0);

                assert(self.peer != .none);
                assert(self.state == .connected);
                assert(self.fd != -1);

                if (self.peer != .unknown) return;

                switch (header.peer_type()) {
                    .unknown => return,
                    .replica => {
                        self.peer = .{ .replica = header.replica };
                        // If there is a connection to this replica, terminate and replace it:
                        if (bus.replicas[self.peer.replica]) |old| {
                            assert(old.peer == .replica);
                            assert(old.peer.replica == self.peer.replica);
                            assert(old.state != .free);
                            if (old.state != .terminating) old.terminate(bus, .shutdown);
                        }
                        bus.replicas[self.peer.replica] = self;
                        log.info("connection from replica {}", .{self.peer.replica});
                    },
                    .client => {
                        self.peer = .{ .client = header.client };
                        const result = bus.process.clients.getOrPutAssumeCapacity(header.client);
                        // If there is a connection to this client, terminate and replace it:
                        if (result.found_existing) {
                            const old = result.value_ptr.*;
                            assert(old.peer == .client);
                            assert(old.peer.client == self.peer.client);
                            assert(old.state == .connected or old.state == .terminating);
                            if (old.state != .terminating) old.terminate(bus, .shutdown);
                        }
                        result.value_ptr.* = self;
                        log.info("connection from client {}", .{self.peer.client});
                    },
                }
            }

            /// Acquires a free message if necessary and then calls `recv()`.
            /// Terminates the connection if a free message cannot be obtained.
            /// If the connection has a `recv_message` and the message being parsed is
            /// at pole position then calls `recv()` immediately, otherwise copies any
            /// partially received message into a new Message and sets `recv_message`,
            /// releasing the old one.
            fn get_recv_message_and_recv(self: *Connection, bus: *Self) void {
                if (self.recv_message != null and self.recv_parsed == 0) {
                    self.recv(bus);
                    return;
                }

                const new_message = bus.get_message() orelse {
                    // TODO Decrease the probability of this happening by:
                    // 1. using a header-only message if possible.
                    // 2. determining a true upper limit for static allocation.
                    log.err("no free buffer available to recv message from {}", .{self.peer});
                    self.terminate(bus, .shutdown);
                    return;
                };
                defer bus.unref(new_message);

                if (self.recv_message) |recv_message| {
                    defer bus.unref(recv_message);

                    assert(self.recv_progress > 0);
                    assert(self.recv_parsed > 0);
                    const data = recv_message.buffer[self.recv_parsed..self.recv_progress];
                    mem.copy(u8, new_message.buffer, data);
                    self.recv_progress = data.len;
                    self.recv_parsed = 0;
                } else {
                    assert(self.recv_progress == 0);
                    assert(self.recv_parsed == 0);
                }

                self.recv_message = new_message.ref();
                self.recv(bus);
            }

            fn recv(self: *Connection, bus: *Self) void {
                assert(self.peer != .none);
                assert(self.state == .connected);
                assert(self.fd != -1);

                assert(!self.recv_submitted);
                self.recv_submitted = true;

                assert(self.recv_progress < config.message_size_max);

                bus.io.recv(
                    *Self,
                    bus,
                    on_recv,
                    &self.recv_completion,
                    self.fd,
                    self.recv_message.?.buffer[self.recv_progress..config.message_size_max],
                    os.MSG_NOSIGNAL,
                );
            }

            fn on_recv(bus: *Self, completion: *IO.Completion, result: IO.RecvError!usize) void {
                const self = @fieldParentPtr(Connection, "recv_completion", completion);
                assert(self.recv_submitted);
                self.recv_submitted = false;
                if (self.state == .terminating) {
                    self.maybe_close(bus);
                    return;
                }
                assert(self.state == .connected);
                const bytes_received = result catch |err| {
                    // TODO: maybe don't need to close on *every* error
                    log.err("error receiving from {}: {}", .{ self.peer, err });
                    self.terminate(bus, .shutdown);
                    return;
                };
                // No bytes received means that the peer closed its side of the connection.
                if (bytes_received == 0) {
                    log.info("peer performed an orderly shutdown: {}", .{self.peer});
                    self.terminate(bus, .close);
                    return;
                }
                self.recv_progress += bytes_received;
                self.parse_messages(bus);
            }

            fn send(self: *Connection, bus: *Self) void {
                assert(self.peer == .client or self.peer == .replica);
                assert(self.state == .connected);
                assert(self.fd != -1);
                const message = self.send_queue.peek() orelse return;
                assert(!self.send_submitted);
                self.send_submitted = true;
                bus.io.send(
                    *Self,
                    bus,
                    on_send,
                    &self.send_completion,
                    self.fd,
                    message.buffer[self.send_progress..message.header.size],
                    os.MSG_NOSIGNAL,
                );
            }

            fn on_send(bus: *Self, completion: *IO.Completion, result: IO.SendError!usize) void {
                const self = @fieldParentPtr(Connection, "send_completion", completion);
                assert(self.send_submitted);
                self.send_submitted = false;
                assert(self.peer == .client or self.peer == .replica);
                if (self.state == .terminating) {
                    self.maybe_close(bus);
                    return;
                }
                assert(self.state == .connected);
                self.send_progress += result catch |err| {
                    // TODO: maybe don't need to close on *every* error
                    log.err("error sending message to replica at {}: {}", .{ self.peer, err });
                    self.terminate(bus, .shutdown);
                    return;
                };
                assert(self.send_progress <= self.send_queue.peek().?.header.size);
                // If the message has been fully sent, move on to the next one.
                if (self.send_progress == self.send_queue.peek().?.header.size) {
                    self.send_progress = 0;
                    const message = self.send_queue.pop().?;
                    bus.unref(message);
                }
                self.send(bus);
            }

            fn maybe_close(self: *Connection, bus: *Self) void {
                assert(self.peer != .none);
                assert(self.state == .terminating);
                // If a recv or send operation is currently submitted to the kernel,
                // submitting a close would cause a race. Therefore we must wait for
                // any currently submitted operation to complete.
                if (self.recv_submitted or self.send_submitted) return;
                self.send_submitted = true;
                self.recv_submitted = true;
                // We can free resources now that there is no longer any I/O in progress.
                while (self.send_queue.pop()) |message| {
                    bus.unref(message);
                }
                if (self.recv_message) |message| {
                    bus.unref(message);
                    self.recv_message = null;
                }
                assert(self.fd != -1);
                defer self.fd = -1;
                // It's OK to use the send completion here as we know that no send
                // operation is currently in progress.
                bus.io.close(*Self, bus, on_close, &self.send_completion, self.fd);
            }

            fn on_close(bus: *Self, completion: *IO.Completion, result: IO.CloseError!void) void {
                const self = @fieldParentPtr(Connection, "send_completion", completion);
                assert(self.send_submitted);
                assert(self.recv_submitted);

                assert(self.peer != .none);
                assert(self.state == .terminating);

                // Reset the connection to its initial state.
                defer {
                    assert(self.recv_message == null);
                    assert(self.send_queue.empty());

                    switch (self.peer) {
                        .none => unreachable,
                        .unknown => {},
                        .client => switch (process_type) {
                            .replica => assert(bus.process.clients.remove(self.peer.client)),
                            .client => unreachable,
                        },
                        .replica => {
                            // A newer replica connection may have replaced this one:
                            if (bus.replicas[self.peer.replica] == self) {
                                bus.replicas[self.peer.replica] = null;
                            } else {
                                // A newer replica connection may even leapfrog this connection and
                                // then be terminated and set to null before we can get here:
                                assert(bus.replicas[self.peer.replica] != null or
                                    bus.replicas[self.peer.replica] == null);
                            }
                        },
                    }
                    bus.connections_used -= 1;
                    self.* = .{};
                }

                result catch |err| {
                    log.err("error closing connection to {}: {}", .{ self.peer, err });
                    return;
                };
            }
        };
    };
}
