const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const os = std.os;

const vr = @import("vr.zig");
const ConfigurationAddress = vr.ConfigurationAddress;
const Header = vr.Header;
const Replica = vr.Replica;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const IO = @import("io_callbacks.zig").IO;

const log = std.log.scoped(.message_bus);

const tcp_backlog = 64;
const num_connections = 32;
const queue_size = 3;

pub const Message = struct {
    header: *Header,
    buffer: []u8 align(vr.sector_size),
    references: usize = 1,
    next: ?*Message = null,
};

// TODO: use a hashmap to make client lookups faster
pub const MessageBus = struct {
    allocator: *mem.Allocator,
    allocated: usize = 0,
    io: *IO,

    configuration: []std.net.Address,

    /// The replica which is running the server
    server: *Replica,
    server_fd: os.socket_t,
    /// Used to store messages sent by the server to itself for delivery in flush().
    server_send_queue: RingBuffer(*Message, queue_size) = .{},

    accept_completion: IO.Completion = undefined,
    /// The connection reserved for the currently in progress accept operation.
    /// This is non-null exactly when an accept operation is submitted.
    accept_connection: ?*Connection = null,

    /// This slice is allocated with a fixed size in the init function and never reallocated.
    connections: []Connection,
    /// Number of connections currently in use (i.e. connection.peer != .none).
    connections_used: usize = 0,

    /// Map from replica index to the currently active connection for that replica, if any.
    /// The connection for the server replica will always be null.
    replicas: []?*Connection,

    /// Initialize the MessageBus for the given server replica and configuration.
    pub fn init(
        self: *MessageBus,
        allocator: *mem.Allocator,
        io: *IO,
        configuration: []std.net.Address,
        server: *Replica,
        server_index: u16,
    ) !void {
        // There must be enough connections for all replicas and at least one client.
        assert(num_connections > configuration.len);

        const connections = try allocator.alloc(Connection, num_connections);
        errdefer allocator.free(connections);
        mem.set(Connection, connections, .{ .message_bus = self });

        const replicas = try allocator.alloc(?*Connection, configuration.len);
        errdefer allocator.free(replicas);
        mem.set(?*Connection, replicas, null);

        self.* = .{
            .allocator = allocator,
            .io = io,
            .configuration = configuration,
            .server = server,
            .server_fd = try init_tcp(configuration[server_index]),
            .connections = connections,
            .replicas = replicas,
        };
    }

    fn init_tcp(address: std.net.Address) !os.socket_t {
        const fd = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, os.IPPROTO_TCP);
        errdefer os.close(fd);
        // TODO: configure RCVBUF, SNDBUF, KEEPALIVE, TIMEOUT, NODELAY
        try os.setsockopt(fd, os.SOL_SOCKET, os.SO_REUSEADDR, &mem.toBytes(@as(c_int, 1)));
        // TODO: port hopping
        try os.bind(fd, &address.any, address.getOsSockLen());
        try os.listen(fd, tcp_backlog);
        return fd;
    }

    pub fn tick(self: *MessageBus) void {
        // Each replica is responsible for connecting to replicas that come
        // after it in the configuration. This ensures that replicas never try
        // to connect to each other at the same time.
        var replica: u16 = self.server.replica + 1;
        while (replica < self.replicas.len) : (replica += 1) {
            self.maybe_connect_to_replica(replica);
        }
        self.maybe_accept();
    }

    fn maybe_connect_to_replica(self: *MessageBus, replica: u16) void {
        // We already have a connection to the given replica.
        if (self.replicas[replica] != null) return;

        // Obtain a connection struct for our new replica connection.
        // If there is an unused connection, use that. Otherwise drop
        // a client or unknown connection to make space. Prefer dropping
        // a client connection to an unknown one as the unknown peer may
        // be a replica. Since shutting a connection down does not happen
        // instantly, simply return after starting the shutdown and try again
        // on the next tick().
        for (self.connections) |*connection| {
            if (connection.state == .idle) {
                assert(connection.peer == .none);
                // This function immediately adds the connection to MessageBus.replicas.
                connection.connect_to_replica(replica);
                return;
            }
        }

        // If there is already a connection being shut down, no need to kill another.
        for (self.connections) |*connection| {
            if (connection.state == .shutting_down) return;
        }

        log.notice("all connections in use but not all replicas are connected, " ++
            "attempting to disconnect a client", .{});
        for (self.connections) |*connection| {
            if (connection.peer == .client) {
                connection.shutdown();
                return;
            }
        }

        log.notice("failed to disconnect a client as no peer was a known client, " ++
            "attempting to disconnect an unknown peer.", .{});
        for (self.connections) |*connection| {
            if (connection.peer == .unknown) {
                connection.shutdown();
                return;
            }
        }

        // We assert that the max number of connections is greater
        // than the number of replicas in init().
        unreachable;
    }

    fn maybe_accept(self: *MessageBus) void {
        if (self.accept_connection != null) return;
        // All connections are currently in use, do nothing.
        if (self.connections_used == self.connections.len) return;
        assert(self.connections_used < self.connections.len);
        self.accept_connection = for (self.connections) |*connection| {
            if (connection.state == .idle) {
                assert(connection.peer == .none);
                connection.state = .accepting;
                break connection;
            }
        } else unreachable;
        self.io.accept(
            *MessageBus,
            self,
            on_accept,
            &self.accept_completion,
            self.server_fd,
            os.SOCK_CLOEXEC,
        );
    }

    fn on_accept(self: *MessageBus, completion: *IO.Completion, result: IO.AcceptError!os.socket_t) void {
        assert(self.accept_connection != null);
        defer self.accept_connection = null;
        const fd = result catch |err| {
            self.accept_connection.?.state = .idle;
            // TODO: some errors should probably be fatal
            log.err("accept failed: {}", .{err});
            return;
        };
        self.accept_connection.?.on_accept(fd);
    }

    /// Teardown, using blocking syscalls to close all sockets
    /// Calling IO.run() after this function is illegal.
    /// TODO: should we free memory here or just let the OS clean up?
    pub fn deinit(self: *MessageBus) void {
        os.close(self.server_fd);
        for (self.connections) |connection| {
            if (connection.fd != -1) os.close(connection.fd);
        }
    }

    /// Increment the reference count of the message and return the same pointer passed.
    pub fn ref(self: *MessageBus, message: *Message) *Message {
        message.references += 1;
        return message;
    }

    /// Decrement the reference count of the message, possibly freeing it.
    pub fn unref(self: *MessageBus, message: *Message) void {
        message.references -= 1;
        if (message.references == 0) {
            log.debug("freeing {}", .{message.header});
            self.allocator.free(message.buffer);
            self.allocator.destroy(message);
            self.allocated -= 1;
        }
    }

    /// Returns true if the target replica is connected and has space in its send queue.
    pub fn can_send_to_replica(self: *MessageBus, replica: u16) bool {
        const connection = self.replicas[replica] orelse return false;
        return connection.state == .connected and !connection.send_queue.full();
    }

    pub fn send_header_to_replica(self: *MessageBus, replica: u16, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const body = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_body:
        message.header.set_checksum_body(body);
        message.header.set_checksum();

        assert(message.references == 0);
        self.send_message_to_replica(replica, message);
    }

    pub fn send_message_to_replica(self: *MessageBus, replica: u16, message: *Message) void {
        // Messages sent by the server to itself are delivered directly in flush()
        if (replica == self.server.replica) {
            self.server_send_queue.push(self.ref(message)) catch |err| switch (err) {
                error.NoSpaceLeft => {
                    self.unref(message);
                    log.notice("message queue for server full, dropping message", .{});
                },
            };
        } else if (self.replicas[replica]) |connection| {
            connection.send_message(message);
        } else {
            log.debug("no active connection to replica {}, " ++
                "dropping message with header {}", .{ replica, message.header });
        }
    }

    pub fn send_header_to_client(self: *MessageBus, client_id: u128, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const body = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_body:
        message.header.set_checksum_body(body);
        message.header.set_checksum();

        assert(message.references == 0);
        self.send_message_to_client(client_id, message);
    }

    /// Try to send the message to the client with the given id.
    /// If the client is not currently connected, the message is silently dropped.
    pub fn send_message_to_client(self: *MessageBus, client_id: u128, message: *Message) void {
        for (self.connections) |*connection| {
            switch (connection.peer) {
                .client => |id| if (id == client_id) {
                    connection.send_message(message);
                    return;
                },
                else => {},
            }
        }
    }

    pub fn flush(self: *MessageBus) void {
        // Deliver messages the server replica has sent to itself.
        // Iterate on a copy to avoid a potential infinite loop.
        var copy = self.server_send_queue;
        self.server_send_queue = .{};
        while (copy.pop()) |message| {
            self.server.on_message(message);
            self.unref(message);
        }
    }

    pub fn create_message(self: *MessageBus, size: u32) !*Message {
        assert(size >= @sizeOf(Header));

        var buffer = try self.allocator.allocAdvanced(u8, vr.sector_size, size, .exact);
        errdefer self.allocator.free(buffer);
        mem.set(u8, buffer, 0);

        var message = try self.allocator.create(Message);
        errdefer self.allocator.destroy(message);

        self.allocated += 1;

        message.* = .{
            .header = mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]),
            .buffer = buffer,
        };

        return message;
    }
};

/// Used to send/receive messages to/from a client or fellow replica.
const Connection = struct {
    message_bus: *MessageBus,

    /// The peer is determined by inspecting the first message header
    /// received.
    peer: union(enum) {
        /// No peer is currently connected.
        none: void,
        /// A connection has been established but the first header has not yet been received.
        unknown: void,
        /// The peer is a client with the given id.
        client: u128,
        /// The peer is a replica with the given id.
        replica: u16,
    } = .none,
    state: enum {
        /// The connection is currently inactive, peer is none.
        idle,
        /// This connection has been reserved for an in progress accept operation,
        /// peer is none.
        accepting,
        /// The peer is a replica and a connect operation has been started
        /// but not yet competed.
        connecting,
        /// The peer is fully connected and may be a client, replica, or unknown.
        connected,
        /// The connection is being terminated but cleanup has not yet finished.
        shutting_down,
    } = .idle,
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
    /// Number of bytes of the current header/message that have already been received.
    recv_progress: usize = 0,
    incoming_header: Header = undefined,
    incoming_message: *Message = undefined,

    /// This completion is used for all send operations.
    send_completion: IO.Completion = undefined,
    /// True exactly when the send_completion has been submitted to the IO abstraction
    /// but the callback has not yet been run.
    send_submitted: bool = false,
    /// Number of bytes of the current message that have already been sent.
    send_progress: usize = 0,
    /// The queue of messages to send to the client or replica peer.
    send_queue: RingBuffer(*Message, queue_size) = .{},

    /// Attempt to connect to a replica.
    /// The slot in the Message.replicas slices is immediately reserved.
    /// Failure is silent and returns the connection to an unused state.
    pub fn connect_to_replica(self: *Connection, replica: u16) void {
        assert(replica != self.message_bus.server.replica);
        assert(self.peer == .none);
        assert(self.state == .idle);
        assert(self.fd == -1);

        const bus = self.message_bus;
        const server_addr = bus.configuration[bus.server.replica];
        self.fd = os.socket(server_addr.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0) catch return;

        self.peer = .{ .replica = replica };
        self.state = .connecting;
        assert(bus.replicas[replica] == null);
        bus.replicas[replica] = self;

        assert(!self.recv_submitted);
        self.recv_submitted = true;
        bus.io.connect(
            *Connection,
            self,
            on_connect,
            // We need to use the recv_completion here
            &self.recv_completion,
            self.fd,
            bus.configuration[replica],
        );
    }

    fn on_connect(self: *Connection, completion: *IO.Completion, result: IO.ConnectError!void) void {
        assert(self.recv_submitted);
        self.recv_submitted = false;
        assert(self.state == .connecting);
        result catch |err| {
            log.err("error connecting to {}: {}", .{ self.peer, err });
            self.state = .shutting_down;
            self.maybe_close();
            return;
        };
        log.info("connected to {}", .{self.peer});
        self.recv_header();
    }

    /// Given a newly accepted fd, start receiving messages on it.
    /// Callbacks will be continuously re-registered until shutdown() is
    /// called and the connection is terminated.
    pub fn on_accept(self: *Connection, fd: os.socket_t) void {
        assert(self.peer == .none);
        assert(self.state == .accepting);
        assert(self.fd == -1);
        self.peer = .unknown;
        self.state = .connected;
        self.fd = fd;
        self.message_bus.connections_used += 1;
        self.recv_header();
    }

    /// Add a message to the connection's send queue, starting a send operation
    /// if the queue was previously empty.
    pub fn send_message(self: *Connection, message: *Message) void {
        assert(self.peer == .client or self.peer == .replica);
        if (self.state == .shutting_down) return;
        const queue_was_empty = self.send_queue.empty();
        self.send_queue.push(self.message_bus.ref(message)) catch |err| switch (err) {
            error.NoSpaceLeft => {
                self.message_bus.unref(message);
                log.notice("message queue for peer {} full, dropping message", .{self.peer});
                return;
            },
        };
        // If the queue was not empty, the message will be sent after the
        // messages currently being sent.
        if (queue_was_empty) self.send();
    }

    /// Clean up an active connection and reset it to its initial, unused, state.
    /// This reset does not happen instantly as currently in progress operations
    /// must first be stopped.
    pub fn shutdown(self: *Connection) void {
        assert(self.peer != .none);
        assert(self.state != .idle);
        assert(self.fd != -1);
        // The shutdown syscall will cause currently in progress send/recv
        // operations to be gracefully closed while keeping the fd open.
        const rc = os.linux.shutdown(self.fd, os.SHUT_RDWR);
        switch (os.errno(rc)) {
            0 => {},
            os.EBADF => unreachable,
            os.EINVAL => unreachable,
            os.ENOTCONN => {
                // This should only happen if we for some reason decide to shutdown()
                // a connection while a connect operation is in progress.
                // This is fine though, we simply continue with the logic below and
                // wait for the connect operation to finish.
                assert(self.state == .connecting);
                assert(self.recv_submitted);
                assert(!self.send_submitted);
            },
            os.ENOTSOCK => unreachable,
            else => |err| os.unexpectedErrno(err) catch {},
        }
        assert(self.state != .shutting_down);
        self.state = .shutting_down;
        self.maybe_close();
    }

    fn recv_header(self: *Connection) void {
        self.recv(on_recv_header, mem.asBytes(&self.incoming_header)[self.recv_progress..]);
    }

    fn recv_body(self: *Connection) void {
        self.recv(
            on_recv_body,
            self.incoming_message.buffer[self.recv_progress..][0..self.incoming_header.size],
        );
    }

    fn recv(
        self: *Connection,
        comptime callback: fn (*Connection, *IO.Completion, IO.RecvError!usize) void,
        buffer: []u8,
    ) void {
        assert(self.peer != .none);
        assert(self.state == .connected);
        assert(self.fd != -1);
        assert(!self.recv_submitted);
        self.recv_submitted = true;
        self.message_bus.io.recv(
            *Connection,
            self,
            callback,
            &self.recv_completion,
            self.fd,
            buffer,
            os.MSG_NOSIGNAL,
        );
    }

    fn on_recv_header(self: *Connection, completion: *IO.Completion, result: IO.RecvError!usize) void {
        assert(self.recv_submitted);
        self.recv_submitted = false;

        if (self.state == .shutting_down) {
            self.maybe_close();
            return;
        }
        assert(self.state == .connected);

        const bytes_received = result catch |err| {
            // TODO: maybe don't need to close on *every* error
            log.err("error receiving body from {}: {}", .{ self.peer, err });
            self.shutdown();
            return;
        };

        // No bytes received means a clean shutdown, either due to our
        // shutdown syscall or the peer closing the socket.
        if (bytes_received == 0) {
            if (self.state == .shutting_down) {
                self.maybe_close();
            } else {
                self.shutdown();
            }
            return;
        }

        self.recv_progress += bytes_received;

        if (self.recv_progress < @sizeOf(Header)) {
            // The header has not yet been fully received.
            if (self.state == .shutting_down) {
                self.maybe_close();
            } else {
                self.recv_header();
            }
            return;
        }
        assert(self.recv_progress == @sizeOf(Header));

        if (!self.incoming_header.valid_checksum()) {
            log.err("invalid checksum on header received from {}", .{self.peer});
            self.shutdown();
            return;
        }

        switch (self.peer) {
            .none => unreachable,
            .unknown => {
                // Ensure that the message is addressed to the correct cluster.
                if (self.incoming_header.cluster != self.message_bus.server.cluster) {
                    log.err("received message addressed to wrong cluster: {}", .{self.incoming_header.cluster});
                    self.shutdown();
                    return;
                }
                // The only command sent by clients is the request command.
                if (self.incoming_header.command == .request) {
                    self.peer = .{ .client = self.incoming_header.client };
                } else {
                    self.peer = .{ .replica = self.incoming_header.replica };
                    // If there is already a connection to this replica, terminate and replace it.
                    if (self.message_bus.replicas[self.peer.replica]) |old| {
                        assert(old.peer == .replica);
                        assert(old.peer.replica == self.peer.replica);
                        assert(old.state != .idle);
                        if (old.state != .shutting_down) old.shutdown();
                        self.message_bus.replicas[self.peer.replica] = null;
                    }
                    self.message_bus.replicas[self.peer.replica] = self;
                }
            },
            .client => assert(self.incoming_header.command == .request),
            .replica => assert(self.incoming_header.command != .request),
        }
        assert(self.incoming_header.cluster == self.message_bus.server.cluster);

        self.incoming_message = self.message_bus.create_message(self.incoming_header.size) catch unreachable;
        self.incoming_message.header.* = self.incoming_header;
        self.recv_body();
    }

    fn on_recv_body(self: *Connection, completion: *IO.Completion, result: IO.RecvError!usize) void {
        assert(self.recv_submitted);
        self.recv_submitted = false;

        if (self.state == .shutting_down) {
            self.maybe_close();
            return;
        }
        assert(self.state == .connected);

        const bytes_received = result catch |err| {
            // TODO: maybe don't need to close on *every* error
            log.err("error receiving body from {}: {}", .{ self.peer, err });
            self.shutdown();
            return;
        };

        // No bytes received means a clean shutdown, either due to our
        // shutdown syscall or the peer closing the socket.
        if (bytes_received == 0) {
            if (self.state == .shutting_down) {
                self.maybe_close();
            } else {
                self.shutdown();
            }
            return;
        }
        self.recv_progress += bytes_received;

        if (self.recv_progress < self.incoming_header.size) {
            // The body has not yet been fully received.
            if (self.state == .shutting_down) {
                self.maybe_close();
            } else {
                self.recv_body();
            }
            return;
        }
        assert(self.recv_progress == self.incoming_header.size);
        defer self.message_bus.unref(self.incoming_message);

        const body = self.incoming_message.buffer[@sizeOf(Header)..self.incoming_header.size];
        if (self.incoming_header.valid_checksum_body(body)) {
            self.message_bus.server.on_message(self.incoming_message);
        } else {
            log.err("invalid checksum on body received from {}", .{self.peer});
            self.shutdown();
            return;
        }

        // Reset state and try to receive the next message.
        self.incoming_header = undefined;
        self.incoming_message = undefined;
        self.recv_progress = 0;
        self.recv_header();
    }

    fn send(self: *Connection) void {
        assert(self.peer == .client or self.peer == .replica);
        assert(self.state == .connected);
        assert(self.fd != -1);
        const message = self.send_queue.peek() orelse return;
        assert(!self.send_submitted);
        self.send_submitted = true;
        self.message_bus.io.send(
            *Connection,
            self,
            on_send,
            &self.send_completion,
            self.fd,
            message.buffer[self.send_progress..][0..message.header.size],
            os.MSG_NOSIGNAL,
        );
    }

    fn on_send(self: *Connection, completion: *IO.Completion, result: IO.SendError!usize) void {
        assert(self.peer == .client or self.peer == .replica);
        assert(self.send_submitted);
        self.send_submitted = false;
        if (self.state == .shutting_down) {
            self.maybe_close();
            return;
        }
        self.send_progress += result catch |err| {
            // TODO: maybe don't need to close on *every* error
            log.err("error sending message to replica at {}: {}", .{ self.peer, err });
            self.shutdown();
            return;
        };
        assert(self.send_progress <= self.send_queue.peek().?.header.size);
        // If the message has been fully sent, move on to the next one.
        if (self.send_progress == self.send_queue.peek().?.header.size) {
            self.send_progress = 0;
            const message = self.send_queue.pop().?;
            self.message_bus.unref(message);
        }
        self.send();
    }

    fn maybe_close(self: *Connection) void {
        assert(self.peer != .none);
        assert(self.state == .shutting_down);
        // If a recv or send operation is currently submitted to the kernel,
        // submitting a close would cause a race. Therefore we must wait for
        // any currently submitted operation to complete.
        if (self.recv_submitted or self.send_submitted) return;
        self.send_submitted = true;
        self.recv_submitted = true;
        // We can free resources now that there is no longer any I/O in progress.
        while (self.send_queue.pop()) |message| {
            self.message_bus.unref(message);
        }
        assert(self.fd != -1);
        defer self.fd = -1;
        // It's OK to use the send completion here as we know that no send
        // operation is currently in progress.
        self.message_bus.io.close(*Connection, self, on_close, &self.send_completion, self.fd);
    }

    fn on_close(self: *Connection, completion: *IO.Completion, result: IO.CloseError!void) void {
        assert(self.peer != .none);
        assert(self.state == .shutting_down);

        // Reset the connection to its initial state.
        defer {
            assert(self.send_queue.empty());
            if (self.peer == .replica) {
                assert(self.message_bus.replicas[self.peer.replica] != null);
                // A newer replica connection may have replace this one.
                if (self.message_bus.replicas[self.peer.replica] == self) {
                    self.message_bus.replicas[self.peer.replica] = null;
                }
            }
            self.* = .{ .message_bus = self.message_bus };
        }

        result catch |err| {
            log.err("error closing connection to {}: {}", .{ self.peer, err });
            return;
        };
        log.debug("closed connection to {}", .{self.peer});
    }
};

test "" {
    std.testing.refAllDecls(MessageBus);
    std.testing.refAllDecls(Connection);
}
