/// Whether development or production:
pub const deployment_environment = .development;

/// The maximum log level in increasing order of verbosity (emergency=0, debug=7):
pub const log_level = 6;

/// The maximum number of replicas allowed in a cluster.
pub const replicas_max = 16;

/// The maximum number of clients allowed per cluster, where each client has a unique 128-bit ID.
/// This impacts the amount of memory allocated at initialization by the server.
/// This determines the size of the VR client table used to cache replies to clients by client ID.
/// Each client has one entry in the VR client table to store the latest `message_size_max` reply.
pub const clients_max = 32;

/// The minimum number of nodes required to form quorums for leader election or replication:
/// Majority quorums are only required across leader election and replication phases (not within).
/// As per Flexible Paxos, provided quorum_leader_election + quorum_replication > cluster_nodes:
/// 1. you may increase quorum_leader_election above a majority, so that
/// 2. you can decrease quorum_replication below a majority, to optimize the common case.
/// This improves latency by reducing the number of nodes required for synchronous replication.
/// This reduces redundancy only in the short term, asynchronous replication will still continue.
pub const quorum_leader_election = -1;
pub const quorum_replication = 2;

/// The default server port to listen on if not specified in `--addresses`:
pub const port = 3001;

/// The default network interface address to listen on if not specified in `--addresses`:
/// WARNING: Binding to all interfaces with "0.0.0.0" is dangerous and opens the server to anyone.
/// Bind to the "127.0.0.1" loopback address to accept local connections as a safe default only.
pub const address = "127.0.0.1";

/// Where journal files should be persisted:
pub const data_directory = "/var/lib/tigerbeetle";

/// The maximum number of accounts to store in memory:
/// This impacts the amount of memory allocated at initialization by the server.
pub const accounts_max = switch (deployment_environment) {
    .production => 1_000_000,
    else => 100_000,
};

/// The maximum number of transfers to store in memory:
/// This impacts the amount of memory allocated at initialization by the server.
/// We allocate more capacity than the number of transfers for a safe hash table load factor.
pub const transfers_max = switch (deployment_environment) {
    .production => 100_000_000,
    else => 1_000_000,
};

/// The maximum number of two-phase commits to store in memory:
/// This impacts the amount of memory allocated at initialization by the server.
pub const commits_max = transfers_max;

/// The maximum size of the journal file:
/// This is pre-allocated and zeroed for performance when initialized.
/// Writes within this file never extend the filesystem inode size reducing the cost of fdatasync().
/// This enables static allocation of disk space so that appends cannot fail with ENOSPC.
/// This also enables us to detect filesystem inode corruption that would change the journal size.
pub const journal_size_max = switch (deployment_environment) {
    .production => 128 * 1024 * 1024 * 1024,
    else => 256 * 1024 * 1024,
};

/// The maximum number of batch entries in the journal file:
/// A batch entry may contain many transfers, so this is not a limit on the number of transfers.
/// We need this limit to allocate space for copies of batch headers at the start of the journal.
/// These header copies enable us to disentangle corruption from crashes and recover accordingly.
pub const journal_headers_max = switch (deployment_environment) {
    .production => 1024 * 1024,
    else => 16384,
};

/// The maximum number of connections that can be held open by the server at any time:
pub const connections_max = replicas_max + clients_max;

/// The maximum size of a message in bytes:
/// This is also the limit of all inflight data across multiple pipelined requests per connection.
/// We may have one request of up to 2 MiB inflight or 2 pipelined requests of up to 1 MiB inflight.
/// This impacts sequential disk write throughput, the larger the buffer the better.
/// 2 MiB is 16,384 transfers, and a reasonable choice for sequential disk write throughput.
/// However, this impacts bufferbloat and head-of-line blocking latency for pipelined requests.
/// For a 1 Gbps NIC = 125 MiB/s throughput: 2 MiB / 125 * 1000ms = 16ms for the next request.
/// This impacts the amount of memory allocated at initialization by the server.
pub const message_size_max = 2 * 1024 * 1024;

/// The number of full-sized messages allocated at initialization by the message bus.
pub const message_bus_messages_max = connections_max * 4;
/// The number of header-sized messages allocated at initialization by the message bus.
/// These are much smaller/cheaper and we can therefore have many of them.
pub const message_bus_headers_max = connections_max * connection_send_queue_max;

/// The minimum and maximum amount of time in milliseconds to wait before initiating a connection.
/// Exponential backoff and full jitter are applied within this range.
/// For more, see: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
pub const connection_delay_min = 50;
pub const connection_delay_max = 1000;

/// The maximum number of outgoing messages that may be queued on a connection.
pub const connection_send_queue_max = 3;

/// The maximum number of connections in the kernel's complete connection queue pending an accept():
/// If the backlog argument is greater than the value in `/proc/sys/net/core/somaxconn`, then it is
/// silently truncated to that value. Since Linux 5.4, the default in this file is 4096.
pub const tcp_backlog = 64;

/// The maximum size of a kernel socket receive buffer in bytes (or 0 to use the system default):
/// This sets SO_RCVBUF as an alternative to the auto-tuning range in /proc/sys/net/ipv4/tcp_rmem.
/// The value is limited by /proc/sys/net/core/rmem_max, unless the CAP_NET_ADMIN privilege exists.
/// The kernel doubles this value to allow space for packet bookkeeping overhead.
/// The receive buffer should ideally exceed the Bandwidth-Delay Product for maximum throughput.
/// At the same time, be careful going beyond 4 MiB as the kernel may merge many small TCP packets,
/// causing considerable latency spikes for large buffer sizes:
/// https://blog.cloudflare.com/the-story-of-one-latency-spike/
pub const tcp_rcvbuf = 4 * 1024 * 1024;

/// The maximum size of a kernel socket send buffer in bytes (or 0 to use the system default):
/// This sets SO_SNDBUF as an alternative to the auto-tuning range in /proc/sys/net/ipv4/tcp_wmem.
/// The value is limited by /proc/sys/net/core/wmem_max, unless the CAP_NET_ADMIN privilege exists.
/// The kernel doubles this value to allow space for packet bookkeeping overhead.
pub const tcp_sndbuf = 4 * 1024 * 1024;

/// Whether to enable TCP keepalive:
pub const tcp_keepalive = true;

/// The time (in seconds) the connection needs to be idle before sending TCP keepalive probes:
/// Probes are not sent when the send buffer has data or the congestion window size is zero,
/// for these cases we also need tcp_user_timeout below.
pub const tcp_keepidle = 5;

/// The time (in seconds) between individual keepalive probes:
pub const tcp_keepintvl = 4;

/// The maximum number of keepalive probes to send before dropping the connection:
pub const tcp_keepcnt = 3;

/// The time (in milliseconds) to timeout an idle connection or unacknowledged send:
/// This timer rides on the granularity of the keepalive or retransmission timers.
/// For example, if keepalive will only send a probe after 10s then this becomes the lower bound
/// for tcp_user_timeout to fire, even if tcp_user_timeout is 2s. Nevertheless, this would timeout
/// the connection at 10s rather than wait for tcp_keepcnt probes to be sent. At the same time, if
/// tcp_user_timeout is larger than the max keepalive time then tcp_keepcnt will be ignored and
/// more keepalive probes will be sent until tcp_user_timeout fires.
/// For a thorough overview of how these settings interact:
/// https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/
pub const tcp_user_timeout = (tcp_keepidle + tcp_keepintvl * tcp_keepcnt) * 1000;

/// Whether to disable Nagle's algorithm to eliminate send buffering delays:
pub const tcp_nodelay = true;

/// The minimum size of an aligned kernel page and an Advanced Format disk sector:
/// This is necessary for direct I/O without the kernel having to fix unaligned pages with a copy.
/// The new Advanced Format sector size is backwards compatible with the old 512 byte sector size.
/// This should therefore never be less than 4 KiB to be future-proof when server disks are swapped.
pub const sector_size = 4096;

/// Whether to perform direct I/O to the underlying disk device:
/// This enables several performance optimizations:
/// * A memory copy to the kernel's page cache can be eliminated for reduced CPU utilization.
/// * I/O can be issued immediately to the disk device without buffering delay for improved latency.
/// This also enables several safety features:
/// * Disk data can be scrubbed to repair latent sector errors and checksum errors proactively.
/// * Fsync failures can be recovered from correctly.
/// WARNING: Disabling direct I/O is unsafe; the page cache cannot be trusted after an fsync error,
/// even after an application panic, since the kernel will mark dirty pages as clean, even
/// when they were never written to disk.
pub const direct_io = true;

/// The number of milliseconds between each replica tick, the basic unit of time in TigerBeetle.
/// Used to regulate heartbeats, retries and timeouts, all specified as multiples of a tick.
pub const tick_ms = 10;

/// The maximum skew between two clocks to allow when considering them to be in agreement.
/// The principle is that no two clocks tick exactly alike but some clocks more or less agree.
/// The maximum skew across the cluster as a whole is this value times the total number of clocks.
/// The cluster will be unavailable if the majority of clocks are all further than this value apart.
/// Decreasing this reduces the probability of reaching agreement on synchronized time.
/// Increasing this reduces the accuracy of synchronized time.
pub const clock_offset_tolerance_max_ms = 10000;

/// The amount of time before the clock's synchronized epoch is expired.
/// If the epoch is expired before it can be replaced with a new synchronized epoch, then this most
/// likely indicates either a network partition or else too many clock faults across the cluster.
/// A new synchronized epoch will be installed as soon as these conditions resolve.
pub const clock_epoch_max_ms = 60000;

/// The amount of time to wait for enough accurate samples before synchronizing the clock.
/// The more samples we can take per remote clock source, the more accurate our estimation becomes.
/// This impacts cluster startup time as the leader must first wait for synchronization to complete.
pub const clock_synchronization_window_min_ms = 2000;

/// The amount of time without agreement before the clock window is expired and a new window opened.
/// This happens where some samples have been collected but not enough to reach agreement.
/// The quality of samples degrades as they age so at some point we throw them away and start over.
/// This eliminates the impact of gradual clock drift on our clock offset (clock skew) measurements.
/// If a window expires because of this then it is likely that the clock epoch will also be expired.
pub const clock_synchronization_window_max_ms = 20000;
