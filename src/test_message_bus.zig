const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("config.zig");

const MessagePool = @import("message_pool.zig").MessagePool;
const Message = MessagePool.Message;

const vr = @import("vr.zig");
const Header = vr.Header;
const Replica = vr.Replica;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;

const log = std.log.scoped(.message_bus);

const SendQueue = RingBuffer(*Message, config.connection_send_queue_max);

pub const MessageBus = struct {
    pub const Address = *Replica;

    pool: MessagePool,

    configuration: []Address,
    send_queues: []SendQueue,

    pub fn init(allocator: *mem.Allocator, configuration: []Address) !MessageBus {
        const send_queues = try allocator.alloc(SendQueue, configuration.len);
        errdefer allocator.free(send_queues);
        mem.set(SendQueue, send_queues, .{});

        return MessageBus{
            .pool = try MessagePool.init(allocator),
            .configuration = configuration,
            .send_queues = send_queues,
        };
    }

    pub fn tick(self: *MessageBus) void {
        self.flush();
    }

    pub fn get_message(self: *MessageBus) ?*Message {
        return self.pool.get_message();
    }

    pub fn unref(self: *MessageBus, message: *Message) void {
        self.pool.unref(message);
    }

    /// Returns true if the target replica is connected and has space in its send queue.
    pub fn can_send_to_replica(self: *MessageBus, replica: u8) bool {
        return !self.send_queues[replica].full();
    }

    pub fn send_header_to_replica(self: *MessageBus, replica: u8, header: Header) void {
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

    pub fn send_message_to_replica(self: *MessageBus, replica: u8, message: *Message) void {
        self.send_queues[replica].push(message.ref()) catch |err| switch (err) {
            error.NoSpaceLeft => {
                self.unref(message);
                log.notice("message queue for replica {} full, dropping message", .{replica});
            },
        };
    }

    pub fn send_header_to_client(self: *MessageBus, client_id: u128, header: Header) void {
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
    pub fn send_message_to_client(self: *MessageBus, client_id: u128, message: *Message) void {
        // Do nothing
    }

    /// Deliver messages to all replicas. Always iterate on a copy to
    /// avoid potential infinite loops.
    pub fn flush(self: *MessageBus) void {
        for (self.send_queues) |*queue, replica| {
            var copy = queue.*;
            queue.* = .{};
            while (copy.pop()) |message| {
                self.configuration[replica].on_message(message);
                self.unref(message);
            }
        }
    }
};

test "" {
    std.testing.refAllDecls(MessageBus);
}
