const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vr);

const config = @import("../config.zig");

const MessageBus = @import("../message_bus.zig").MessageBusReplica;
const Message = @import("../message_bus.zig").Message;
const Storage = @import("../storage.zig").Storage;

const vr = @import("../vr.zig");
const Header = vr.Header;

const ConcurrentRanges = @import("../concurrent_ranges.zig").ConcurrentRanges;
const Range = @import("../concurrent_ranges.zig").Range;

pub const Journal = struct {
    allocator: *Allocator,
    storage: *Storage,
    replica: u8,
    size: u64,
    size_headers: u64,
    size_circular_buffer: u64,
    headers: []Header align(config.sector_size),

    /// Whether an entry is in memory only and needs to be written or is being written:
    /// We use this in the same sense as a dirty bit in the kernel page cache.
    /// A dirty bit means that we have not yet prepared the entry, or need to repair a faulty entry.
    dirty: BitSet,

    /// Whether an entry was written to disk and this write was subsequently lost due to:
    /// * corruption,
    /// * a misdirected write (or a misdirected read, we do not distinguish), or else
    /// * a latent sector error, where the sector can no longer be read.
    /// A faulty bit means that we prepared and then lost the entry.
    /// A faulty bit requires the dirty bit to also be set so that functions need not check both.
    /// A faulty bit is used then only to qualify the severity of the dirty bit.
    faulty: BitSet,

    /// We copy-on-write to this buffer when writing, as in-memory headers may change concurrently:
    write_headers_buffer: []u8 align(config.sector_size),

    /// Apart from the header written with the entry, we also store two redundant copies of each
    /// header at different locations on disk, and we alternate between these for each append.
    /// This tracks which version (0 or 1) should be written to next:
    write_headers_version: u1 = 0,

    /// These serialize concurrent writes but only for overlapping ranges:
    writing_headers: ConcurrentRanges = .{ .name = "write_headers" },
    writing_sectors: ConcurrentRanges = .{ .name = "write_sectors" },

    pub fn init(
        allocator: *Allocator,
        storage: *Storage,
        replica: u8,
        size: u64,
        headers_count: u32,
    ) !Journal {
        if (@mod(size, config.sector_size) != 0) return error.SizeMustBeAMultipleOfSectorSize;
        if (!std.math.isPowerOfTwo(headers_count)) return error.HeadersCountMustBeAPowerOfTwo;
        assert(storage.size == size);

        const headers_per_sector = @divExact(config.sector_size, @sizeOf(Header));
        assert(headers_per_sector > 0);
        assert(headers_count >= headers_per_sector);

        var headers = try allocator.allocAdvanced(
            Header,
            config.sector_size,
            headers_count,
            .exact,
        );
        errdefer allocator.free(headers);
        std.mem.set(Header, headers, Header.reserved());

        var dirty = try BitSet.init(allocator, headers.len);
        errdefer dirty.deinit();

        var faulty = try BitSet.init(allocator, headers.len);
        errdefer faulty.deinit();

        var write_headers_buffer = try allocator.allocAdvanced(
            u8,
            config.sector_size,
            @sizeOf(Header) * headers.len,
            .exact,
        );
        errdefer allocator.free(write_headers_buffer);
        std.mem.set(u8, write_headers_buffer, 0);

        const header_copies = 2;
        const size_headers = headers.len * @sizeOf(Header);
        const size_headers_copies = size_headers * header_copies;
        if (size_headers_copies >= size) return error.SizeTooSmallForHeadersCount;

        const size_circular_buffer = size - size_headers_copies;
        if (size_circular_buffer < 64 * 1024 * 1024) return error.SizeTooSmallForCircularBuffer;

        log.debug("{}: journal: size={} headers_len={} headers={} circular_buffer={}", .{
            replica,
            std.fmt.fmtIntSizeBin(size),
            headers.len,
            std.fmt.fmtIntSizeBin(size_headers),
            std.fmt.fmtIntSizeBin(size_circular_buffer),
        });

        var self = Journal{
            .allocator = allocator,
            .storage = storage,
            .replica = replica,
            .size = size,
            .size_headers = size_headers,
            .size_circular_buffer = size_circular_buffer,
            .headers = headers,
            .dirty = dirty,
            .faulty = faulty,
            .write_headers_buffer = write_headers_buffer,
        };

        assert(@mod(self.size_circular_buffer, config.sector_size) == 0);
        assert(@mod(@ptrToInt(&self.headers[0]), config.sector_size) == 0);
        assert(self.dirty.bits.len == self.headers.len);
        assert(self.faulty.bits.len == self.headers.len);
        assert(self.write_headers_buffer.len == @sizeOf(Header) * self.headers.len);

        return self;
    }

    pub fn deinit(self: *Journal) void {}

    /// Asserts that headers are .reserved (zeroed) from `op_min` (inclusive).
    pub fn assert_headers_reserved_from(self: *Journal, op_min: u64) void {
        // TODO Snapshots
        for (self.headers[op_min..]) |header| assert(header.command == .reserved);
    }

    /// Returns any existing entry at the location indicated by header.op.
    /// This existing entry may have an older or newer op number.
    pub fn entry(self: *Journal, header: *const Header) ?*const Header {
        assert(header.command == .prepare);
        return self.entry_for_op(header.op);
    }

    /// We use the op number directly to index into the headers array and locate ops without a scan.
    /// Op numbers cycle through the headers array and do not wrap when offsets wrap. The reason for
    /// this is to prevent variable offsets from impacting the location of an op. Otherwise, the
    /// same op number but for different views could exist at multiple locations in the journal.
    pub fn entry_for_op(self: *Journal, op: u64) ?*const Header {
        // TODO Snapshots
        const existing = &self.headers[op];
        if (existing.command == .reserved) return null;
        assert(existing.command == .prepare);
        return existing;
    }

    /// Returns the entry at `@mod(op)` location, but only if `entry.op == op`, else `null`.
    /// Be careful of using this without considering that there may still be an existing op.
    pub fn entry_for_op_exact(self: *Journal, op: u64) ?*const Header {
        if (self.entry_for_op(op)) |existing| {
            if (existing.op == op) return existing;
        }
        return null;
    }

    /// As per `entry_for_op_exact()`, but only if there is an optional checksum match.
    pub fn entry_for_op_exact_with_checksum(
        self: *Journal,
        op: u64,
        checksum: ?u128,
    ) ?*const Header {
        if (self.entry_for_op_exact(op)) |existing| {
            assert(existing.op == op);
            if (checksum == null or existing.checksum == checksum.?) return existing;
        }
        return null;
    }

    pub fn previous_entry(self: *Journal, header: *const Header) ?*const Header {
        // TODO Snapshots
        if (header.op == 0) return null;
        return self.entry_for_op(header.op - 1);
    }

    pub fn next_entry(self: *Journal, header: *const Header) ?*const Header {
        // TODO Snapshots
        if (header.op + 1 == self.headers.len) return null;
        return self.entry_for_op(header.op + 1);
    }

    pub fn next_offset(self: *Journal, header: *const Header) u64 {
        // TODO Snapshots
        assert(header.command == .prepare);
        return header.offset + Journal.sector_ceil(header.size);
    }

    pub fn has(self: *Journal, header: *const Header) bool {
        // TODO Snapshots
        const existing = &self.headers[header.op];
        if (existing.command == .reserved) {
            assert(existing.checksum == 0);
            assert(existing.checksum_body == 0);
            assert(existing.offset == 0);
            assert(existing.size == 0);
            return false;
        } else {
            if (existing.checksum == header.checksum) {
                assert(existing.checksum_body == header.checksum_body);
                assert(existing.op == header.op);
                return true;
            } else {
                return false;
            }
        }
    }

    pub fn has_clean(self: *Journal, header: *const Header) bool {
        // TODO Snapshots
        return self.has(header) and !self.dirty.bit(header.op);
    }

    pub fn has_dirty(self: *Journal, header: *const Header) bool {
        // TODO Snapshots
        return self.has(header) and self.dirty.bit(header.op);
    }

    /// Copies latest headers between `op_min` and `op_max` (both inclusive) as will fit in `dest`.
    /// Reverses the order when copying so that latest headers are copied first, which also protects
    /// against the callsite slicing the buffer the wrong way and incorrectly.
    /// Skips .reserved headers (gaps between headers).
    /// Zeroes the `dest` buffer in case the copy would underflow and leave a buffer bleed.
    /// Returns the number of headers actually copied.
    pub fn copy_latest_headers_between(
        self: *Journal,
        op_min: u64,
        op_max: u64,
        dest: []Header,
    ) usize {
        assert(op_min <= op_max);
        assert(dest.len > 0);

        var copied: usize = 0;
        std.mem.set(Header, dest, Header.reserved());

        // We start at op_max + 1 but front-load the decrement to avoid overflow when op_min == 0:
        var op = op_max + 1;
        while (op > op_min) {
            op -= 1;

            if (self.entry_for_op_exact(op)) |header| {
                dest[copied] = header.*;
                assert(dest[copied].invalid() == null);
                copied += 1;
            }
        }
        return copied;
    }

    const HeaderRange = struct { op_min: u64, op_max: u64 };

    /// Finds the latest break in headers, searching between `op_min` and `op_max` (both inclusive).
    /// A break is a missing header or a header not connected to the next header (by hash chain).
    /// Upon finding the highest break, extends the range downwards to cover as much as possible.
    ///
    /// For example: If ops 3, 9 and 10 are missing, returns: `{ .op_min = 9, .op_max = 10 }`.
    ///
    /// Another example: If op 17 is disconnected from op 18, 16 is connected to 17, and 12-15 are
    /// missing, returns: `{ .op_min = 12, .op_max = 17 }`.
    pub fn find_latest_headers_break_between(
        self: *Journal,
        op_min: u64,
        op_max: u64,
    ) ?HeaderRange {
        assert(op_min <= op_max);
        var range: ?HeaderRange = null;

        // We set B, the op after op_max, to null because we only examine breaks <= op_max:
        // In other words, we may report a missing header for op_max itself but not a broken chain.
        var B: ?*const Header = null;

        var op = op_max + 1;
        while (op > op_min) {
            op -= 1;

            // Get the entry at @mod(op) location, but only if entry.op == op, else null:
            var A = self.entry_for_op_exact(op);
            if (A) |a| {
                if (B) |b| {
                    // If A was reordered then A may have a newer op than B (but an older view).
                    // However, here we use entry_for_op_exact() so we can assert a.op + 1 == b.op:
                    assert(a.op + 1 == b.op);
                    // Further, while repair_header() should never put an older view to the right
                    // of a newer view, it may put a newer view to the left of an older view.
                    // We therefore do not assert a.view <= b.view unless the hash chain is intact.

                    // A exists and B exists:
                    if (range) |*r| {
                        assert(b.op == r.op_min);
                        if (a.checksum == b.parent) {
                            // A is connected to B, but B is disconnected, add A to range:
                            assert(a.view <= b.view);
                            r.op_min = a.op;
                        } else if (a.view < b.view) {
                            // A is not connected to B, and A is older than B, add A to range:
                            r.op_min = a.op;
                        } else if (a.view > b.view) {
                            // A is not connected to B, but A is newer than B, close range:
                            break;
                        } else {
                            // Op numbers in the same view must be connected.
                            unreachable;
                        }
                    } else if (a.checksum == b.parent) {
                        // A is connected to B, and B is connected or B is op_max.
                        assert(a.view <= b.view);
                    } else if (a.view < b.view) {
                        // A is not connected to B, and A is older than B, open range:
                        range = .{ .op_min = a.op, .op_max = a.op };
                    } else if (a.view > b.view) {
                        // A is not connected to B, but A is newer than B, open and close range:
                        // TODO Add unit test especially for this.
                        // This is important if we see `self.op < self.commit_max` then request
                        // prepares and then later receive a newer view to the left of `self.op`.
                        // We must then repair `self.op` which was reordered through a view change.
                        range = .{ .op_min = b.op, .op_max = b.op };
                        break;
                    } else {
                        // Op numbers in the same view must be connected.
                        unreachable;
                    }
                } else {
                    // A exists and B does not exist (or B has a lower op number):
                    if (range) |r| {
                        // We therefore cannot compare A to B, A may be older/newer, close range:
                        assert(r.op_min == op + 1);
                        break;
                    } else {
                        // We expect a range if B does not exist, unless:
                        assert(a.op == op_max);
                    }
                }
            } else {
                // A does not exist (or A has a lower op number):
                if (self.entry_for_op(op)) |wrapped_a| assert(wrapped_a.op < op);

                if (range) |*r| {
                    // Add A to range:
                    assert(r.op_min == op + 1);
                    r.op_min = op;
                } else {
                    // Open range:
                    assert(B != null or op == op_max);
                    range = .{ .op_min = op, .op_max = op };
                }
            }

            B = A;
        }

        return range;
    }

    pub fn read_sectors(self: *Journal, buffer: []u8, offset: u64) void {
        // Memory must not be owned by self.headers as self.headers may be modified concurrently:
        assert(@ptrToInt(buffer.ptr) < @ptrToInt(self.headers.ptr) or
            @ptrToInt(buffer.ptr) > @ptrToInt(self.headers.ptr) + self.size_headers);

        log.debug("{}: journal: read_sectors: offset={} len={}", .{
            self.replica,
            offset,
            buffer.len,
        });

        self.storage.read(buffer, offset);
    }

    /// A safe way of removing an entry, where the header must match the current entry to succeed.
    fn remove_entry(self: *Journal, header: *const Header) void {
        // Copy the header.op by value to avoid a reset() followed by undefined header.op usage:
        const op = header.op;
        log.debug("{}: journal: remove_entry: op={}", .{ self.replica, op });

        assert(self.entry(header).?.checksum == header.checksum);
        assert(self.headers[op].checksum == header.checksum); // TODO Snapshots

        defer self.headers[op] = Header.reserved();
        self.dirty.clear(op);
        self.faulty.clear(op);
    }

    /// Removes entries from `op_min` (inclusive) onwards.
    /// This is used after a view change to remove uncommitted entries discarded by the new leader.
    pub fn remove_entries_from(self: *Journal, op_min: u64) void {
        // TODO Snapshots
        // TODO Optimize to jump directly to op:
        assert(op_min > 0);
        log.debug("{}: journal: remove_entries_from: op_min={}", .{ self.replica, op_min });
        for (self.headers) |*header| {
            if (header.op >= op_min and header.command == .prepare) {
                self.remove_entry(header);
            }
        }
        self.assert_headers_reserved_from(op_min);
        // TODO At startup we need to handle entries that may have been removed but now reappear.
        // This is because we do not call `write_headers_between()` here.
    }

    pub fn set_entry_as_dirty(self: *Journal, header: *const Header) void {
        log.debug("{}: journal: set_entry_as_dirty: op={} checksum={}", .{
            self.replica,
            header.op,
            header.checksum,
        });
        if (self.entry(header)) |existing| {
            if (existing.checksum != header.checksum) {
                self.faulty.clear(header.op);
            }
        }
        self.headers[header.op] = header.*;
        self.dirty.set(header.op);
        // Do not clear any faulty bit for the same entry.
    }

    pub fn write(self: *Journal, message: *const Message) void {
        assert(message.header.command == .prepare);
        assert(message.header.size >= @sizeOf(Header));
        assert(message.header.size <= message.buffer.len);

        // The underlying header memory must be owned by the buffer and not by self.headers:
        // Otherwise, concurrent writes may modify the memory of the pointer while we write.
        assert(@ptrToInt(message.header) == @ptrToInt(message.buffer.ptr));

        // There should be no concurrency between setting an entry as dirty and deciding to write:
        assert(self.has_dirty(message.header));

        const sectors = message.buffer[0..Journal.sector_ceil(message.header.size)];
        assert(message.header.offset + sectors.len <= self.size_circular_buffer);

        if (std.builtin.mode == .Debug) {
            // Assert that any sector padding has already been zeroed:
            var sum_of_sector_padding_bytes: u32 = 0;
            for (sectors[message.header.size..]) |byte| sum_of_sector_padding_bytes += byte;
            assert(sum_of_sector_padding_bytes == 0);
        }

        self.write_debug(message.header, "starting");

        self.write_sectors(message.buffer, self.offset_in_circular_buffer(message.header.offset));
        if (!self.has(message.header)) {
            self.write_debug(message.header, "entry changed while writing sectors");
            return;
        }

        // TODO Snapshots
        self.write_headers_between(message.header.op, message.header.op);
        if (!self.has(message.header)) {
            self.write_debug(message.header, "entry changed while writing headers");
            return;
        }

        self.write_debug(message.header, "complete, marking clean");
        // TODO Snapshots
        self.dirty.clear(message.header.op);
        self.faulty.clear(message.header.op);
    }

    fn write_debug(self: *Journal, header: *const Header, status: []const u8) void {
        log.debug("{}: journal: write: view={} op={} offset={} len={}: {} {s}", .{
            self.replica,
            header.view,
            header.op,
            header.offset,
            header.size,
            header.checksum,
            status,
        });
    }

    pub fn offset_in_circular_buffer(self: *Journal, offset: u64) u64 {
        assert(offset < self.size_circular_buffer);
        return self.size_headers + offset;
    }

    fn offset_in_headers_version(self: *Journal, offset: u64, version: u1) u64 {
        assert(offset < self.size_headers);
        return switch (version) {
            0 => offset,
            1 => self.size_headers + self.size_circular_buffer + offset,
        };
    }

    /// Writes headers between `op_min` and `op_max` (both inclusive).
    fn write_headers_between(self: *Journal, op_min: u64, op_max: u64) void {
        // TODO Snapshots
        assert(op_min <= op_max);

        const offset = Journal.sector_floor(op_min * @sizeOf(Header));
        const len = Journal.sector_ceil((op_max - op_min + 1) * @sizeOf(Header));
        assert(len > 0);

        // We must acquire the concurrent range using the sector offset and len:
        // Different headers may share the same sector without any op_min or op_max overlap.
        // TODO Use a callback to acquire the range instead of suspend/resume:
        var range = Range{ .offset = offset, .len = len };
        self.writing_headers.acquire(&range);
        defer self.writing_headers.release(&range);

        const source = std.mem.sliceAsBytes(self.headers)[offset .. offset + len];
        var slice = self.write_headers_buffer[offset .. offset + len];
        assert(slice.len == source.len);
        assert(slice.len == len);
        std.mem.copy(u8, slice, source);

        log.debug("{}: journal: write_headers: op_min={} op_max={} sectors[{}..{}]", .{
            self.replica,
            op_min,
            op_max,
            offset,
            offset + len,
        });

        // Versions must be incremented upfront:
        // write_headers_to_version() will block while other calls may proceed concurrently.
        // If we don't increment upfront we could end up writing to the same copy twice.
        // We would then lose the redundancy required to locate headers or overwrite all copies.
        // TODO Snapshots
        if (self.write_headers_once(self.headers[op_min .. op_max + 1])) {
            const version_a = self.write_headers_increment_version();
            self.write_headers_to_version(version_a, slice, offset);
        } else {
            const version_a = self.write_headers_increment_version();
            const version_b = self.write_headers_increment_version();
            self.write_headers_to_version(version_a, slice, offset);
            self.write_headers_to_version(version_b, slice, offset);
        }
    }

    fn write_headers_increment_version(self: *Journal) u1 {
        self.write_headers_version +%= 1;
        return self.write_headers_version;
    }

    /// Since we allow gaps in the journal, we may have to write our headers twice.
    /// If a dirty header is being written as reserved (empty) then write twice to make this clear.
    /// If a dirty header has no previous clean chained entry to give its offset then write twice.
    /// Otherwise, we only need to write the headers once because their other copy can be located in
    /// the body of the journal (using the previous entry's offset and size).
    fn write_headers_once(self: *Journal, headers: []const Header) bool {
        for (headers) |*header| {
            // TODO Snapshots
            // We must use header.op and not the loop index as we are working from a slice:
            if (!self.dirty.bit(header.op)) continue;
            if (header.command == .reserved) {
                log.debug("{}: journal: write_headers_once: dirty reserved header", .{
                    self.replica,
                });
                return false;
            }
            if (self.previous_entry(header)) |previous| {
                assert(previous.command == .prepare);
                if (previous.checksum != header.parent) {
                    log.debug("{}: journal: write_headers_once: no hash chain", .{
                        self.replica,
                    });
                    return false;
                }
                // TODO Add is_dirty(header)
                // TODO Snapshots
                if (self.dirty.bit(previous.op)) {
                    log.debug("{}: journal: write_headers_once: previous entry is dirty", .{
                        self.replica,
                    });
                    return false;
                }
            } else {
                log.debug("{}: journal: write_headers_once: no previous entry", .{
                    self.replica,
                });
                return false;
            }
        }
        return true;
    }

    fn write_headers_to_version(self: *Journal, version: u1, buffer: []const u8, offset: u64) void {
        log.debug("{}: journal: write_headers_to_version: version={} offset={} len={}", .{
            self.replica,
            version,
            offset,
            buffer.len,
        });
        assert(offset + buffer.len <= self.size_headers);
        self.write_sectors(buffer, self.offset_in_headers_version(offset, version));
    }

    fn write_sectors(self: *Journal, buffer: []const u8, offset: u64) void {
        // Memory must not be owned by self.headers as self.headers may be modified concurrently:
        assert(@ptrToInt(buffer.ptr) < @ptrToInt(self.headers.ptr) or
            @ptrToInt(buffer.ptr) > @ptrToInt(self.headers.ptr) + self.size_headers);

        // TODO We can move this range queuing right into Storage and remove write_sectors entirely.
        // Our ConcurrentRange structure would also need to be weaned off of async/await but at
        // least then we can manage this all in one place (i.e. in Storage).
        var range = Range{ .offset = offset, .len = buffer.len };
        self.writing_sectors.acquire(&range);
        defer self.writing_sectors.release(&range);

        log.debug("{}: journal: write_sectors: offset={} len={}", .{
            self.replica,
            offset,
            buffer.len,
        });

        self.storage.write(buffer, offset);
    }

    pub fn sector_floor(offset: u64) u64 {
        const sectors = std.math.divFloor(u64, offset, config.sector_size) catch unreachable;
        return sectors * config.sector_size;
    }

    pub fn sector_ceil(offset: u64) u64 {
        const sectors = std.math.divCeil(u64, offset, config.sector_size) catch unreachable;
        return sectors * config.sector_size;
    }
};

// TODO Snapshots
pub const BitSet = struct {
    allocator: *Allocator,
    bits: []bool,

    /// The number of bits set (updated incrementally as bits are set or cleared):
    len: u64 = 0,

    fn init(allocator: *Allocator, count: u64) !BitSet {
        var bits = try allocator.alloc(bool, count);
        errdefer allocator.free(bits);
        std.mem.set(bool, bits, false);

        return BitSet{
            .allocator = allocator,
            .bits = bits,
        };
    }

    fn deinit(self: *BitSet) void {
        self.allocator.free(self.bits);
    }

    /// Clear the bit for an op (idempotent):
    pub fn clear(self: *BitSet, op: u64) void {
        if (self.bits[op]) {
            self.bits[op] = false;
            self.len -= 1;
        }
    }

    /// Whether the bit for an op is set:
    pub fn bit(self: *BitSet, op: u64) bool {
        return self.bits[op];
    }

    /// Set the bit for an op (idempotent):
    pub fn set(self: *BitSet, op: u64) void {
        if (!self.bits[op]) {
            self.bits[op] = true;
            self.len += 1;
            assert(self.len <= self.bits.len);
        }
    }
};
