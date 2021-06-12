const std = @import("std");

const config = @import("config.zig");

const MessageBus = @import("test_message_bus.zig").MessageBus;
const vr = @import("vr.zig");
const Replica = vr.Replica;
const Journal = vr.Journal;
const Storage = vr.Storage;
const StateMachine = @import("state_machine.zig").StateMachine;

const log = std.log.default;

const Account = @import("tigerbeetle.zig").Account;

pub fn run() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    const f = 1;
    const cluster = 0;

    var configuration: [3]*Replica = undefined;
    var message_bus = try MessageBus.init(allocator, &configuration);

    var storage: [2 * f + 1]Storage = undefined;
    var journals: [2 * f + 1]Journal = undefined;
    for (journals) |*journal, index| {
        storage[index] = try Storage.init(allocator, config.journal_size_max);
        journal.* = try Journal.init(
            allocator,
            &storage[index],
            @intCast(u8, index),
            config.journal_size_max,
            config.journal_headers_max,
        );
    }

    var state_machines: [2 * f + 1]StateMachine = undefined;
    for (state_machines) |*state_machine| {
        state_machine.* = try StateMachine.init(
            allocator,
            config.accounts_max,
            config.transfers_max,
            config.commits_max,
        );
    }

    var replicas: [2 * f + 1]Replica = undefined;
    for (replicas) |*replica, index| {
        replica.* = try Replica.init(
            allocator,
            cluster,
            &configuration,
            @intCast(u8, index),
            f,
            &journals[index],
            &message_bus,
            &state_machines[index],
        );
        configuration[index] = replica;
    }

    var ticks: usize = 0;
    while (true) : (ticks += 1) {
        var leader: ?*Replica = null;
        for (replicas) |*replica| {
            if (replica.status == .normal and replica.leader()) leader = replica;
            replica.tick();
        }

        if (leader) |replica| {
            if (ticks == 1 or ticks == 5) {
                const request = message_bus.get_message() orelse unreachable;
                defer message_bus.unref(request);

                request.header.* = .{
                    .cluster = cluster,
                    .client = 1,
                    .view = 0,
                    .request = @intCast(u32, ticks),
                    .command = .request,
                    .operation = .create_accounts,
                    .size = @sizeOf(vr.Header) + @sizeOf(Account),
                };

                var body = request.buffer[@sizeOf(vr.Header)..][0..@sizeOf(Account)];
                std.mem.bytesAsValue(Account, body).* = .{
                    .id = 1,
                    .custom = 0,
                    .flags = .{},
                    .unit = 710,
                    .debit_reserved = 0,
                    .debit_accepted = 0,
                    .credit_reserved = 0,
                    .credit_accepted = 0,
                    .debit_reserved_limit = 100_000,
                    .debit_accepted_limit = 1_000_000,
                    .credit_reserved_limit = 0,
                    .credit_accepted_limit = 0,
                };

                request.header.set_checksum_body(body);
                request.header.set_checksum();

                message_bus.send_message_to_replica(replica.replica, request);
            }
        }

        std.time.sleep(std.time.ns_per_ms * 50);
    }
}

pub fn main() !void {
    var frame = async run();
    nosuspend try await frame;
}
