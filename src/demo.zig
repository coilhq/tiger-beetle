const std = @import("std");
const assert = std.debug.assert;

usingnamespace @import("tigerbeetle.zig");

pub const Header = @import("vr.zig").Header;
pub const Operation = @import("state_machine.zig").Operation;

pub fn connect(port: u16) !std.os.fd_t {
    var addr = try std.net.Address.parseIp4("127.0.0.1", port);
    var connection = try std.net.tcpConnectToAddress(addr);
    return connection.handle;
}

// TODO We can share most of what follows with `src/benchmark.zig`:

pub const cluster_id: u32 = 0;
pub const client_id: u128 = 123;
pub var request_number: u32 = 0;
var sent_ping = false;

pub fn send(fd: std.os.fd_t, operation: Operation, batch: anytype, Results: anytype) !void {
    // This is required to greet the cluster and identify the connection as being from a client:
    if (!sent_ping) {
        sent_ping = true;

        var ping = Header{
            .command = .ping,
            .cluster = cluster_id,
            .client = client_id,
            .view = 0,
        };
        ping.set_checksum_body(&[0]u8{});
        ping.set_checksum();

        assert((try std.os.sendto(fd, std.mem.asBytes(&ping), 0, null, 0)) == @sizeOf(Header));

        var pong: [@sizeOf(Header)]u8 = undefined;
        var pong_size: u64 = 0;
        while (pong_size < @sizeOf(Header)) {
            var pong_bytes = try std.os.recvfrom(fd, pong[pong_size..], 0, null, null);
            if (pong_bytes == 0) @panic("server closed the connection (while waiting for pong)");
            pong_size += pong_bytes;
        }
    }

    request_number += 1;

    var body = std.mem.asBytes(batch[0..]);

    var request = Header{
        .cluster = cluster_id,
        .client = client_id,
        .view = 0,
        .request = request_number,
        .command = .request,
        .operation = operation,
        .size = @intCast(u32, @sizeOf(Header) + body.len),
    };
    request.set_checksum_body(body[0..]);
    request.set_checksum();

    const header = std.mem.asBytes(&request);
    assert((try std.os.sendto(fd, header[0..], 0, null, 0)) == header.len);
    if (body.len > 0) {
        assert((try std.os.sendto(fd, body[0..], 0, null, 0)) == body.len);
    }

    var recv: [1024 * 1024]u8 = undefined;
    const recv_bytes = try std.os.recvfrom(fd, recv[0..], 0, null, null);
    assert(recv_bytes >= @sizeOf(Header));

    var response = std.mem.bytesAsValue(Header, recv[0..@sizeOf(Header)]);

    // TODO Add jsonStringfy to vr.Header:
    //const stdout = std.io.getStdOut().writer();
    //try response.jsonStringify(.{}, stdout);
    //try stdout.writeAll("\n");
    std.debug.print("{}\n", .{response});

    assert(response.valid_checksum());
    assert(recv_bytes >= response.size);

    const response_body = recv[@sizeOf(Header)..response.size];
    assert(response.valid_checksum_body(response_body));

    for (std.mem.bytesAsSlice(Results, response_body)) |result| {
        // TODO
        //try result.jsonStringify(.{}, stdout);
        //try stdout.writeAll("\n");
        std.debug.print("{}\n", .{result});
    }
}
