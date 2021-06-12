# tigerbeetle

*TigerBeetle is a purpose-built financial accounting database, designed for high-throughput low-latency two-phase prepare/commit transfers between accounts.*

Watch a video introduction of TigerBeetle given to the [Interledger](https://interledger.org/) community on 25 November 2020:

[![Interledger Community Call video on 25 November covering TigerBeetle](https://img.youtube.com/vi/J1OaBRTV2vs/0.jpg)](https://www.youtube.com/watch?v=J1OaBRTV2vs)

## The Problem - Realtime Processing of Balance Updates

Processing events that impact the balance of an account must be done serially, in the correct order and reliably. For this reason, despite the trend to scale modern applications by distributing work across parallel threads/processes/machines, it remains difficult to scale a ledger or accounting system without sacrificing performance or safety.

For example, processing a balance update event involves:

1. validating the event,
2. processing any business rules that must consider the current balance before processing the event and the new balance after the event,
3. updating the balance
4. persisting the updated balance, and
5. notifying subscribers of the updated balance and that the event has been processed.

While investigating a few existing systems it became clear that the majority of architectures cobble together generic databases (relational or NoSQL, on-disk or in-memory) with business logic enforced in the application code. This separation between data and code, persistence and logic, invites the worst of distributed system problems: network delays, multiple roundtrips for a single balance update, clock skew, or cache incoherency etc.

Furthermore, these systems may achieve performance but at the expense of reliability, especially in the face of hardware failure, corruption and misdirected writes, a compromise that is unacceptable for a system-of-record for financial accounts.

## The Solution - A Purpose-Built Financial Accounting Database

Our survey led us to conclude that, while there are mechanisms available to shard account balances to parallelize updates and improve performance, there are still significant performance gains to be had in designing a database that is purpose built for storing balances and processing updates in a reliable manner.

## ProtoBeetle - 200,000 transfers per second

In the month of July 2020, we developed a prototype of TigerBeetle in Node as a performance sketch to measure the basic components of the design (batching, TCP protocol, cryptographic checksums everywhere, fsync journalling, in-memory business logic and hash table operations). **ProtoBeetle ran at 200,000 transfers per second on our office laptops**, supporting our back-of-the-envelope numbers.

We then integrated ProtoBeetle into [Mojaloop](https://mojaloop.io/) and our reference minimum deployment cluster of **Mojaloop went from 76 TPS on MySQL to 1757 TPS on ProtoBeetle**. A single stateless Mojaloop pod was unable to saturate ProtoBeetle. Most of the throughput was spent converting Mojaloop's individual HTTP requests into TCP batches.

**[Watch a 10-minute talk introducing ProtoBeetle.](https://youtu.be/QOC6PHFPtAM?t=324)**

## AlphaBeetle - 500,000 transfers per second

After ProtoBeetle, from September through October 2020, we knuckled down and rewrote TigerBeetle in C/Zig to create the alpha version of TigerBeetle, using [io_uring](https://kernel.dk/io_uring.pdf) as a foundation for fast I/O.

TigerBeetle's Zig implementation of io_uring was [submitted](https://github.com/ziglang/zig/pull/6356) for addition to the Zig standard library.

**[Watch a presentation of TigerBeetle given to the Interledger community on 25 November 2020.](https://www.youtube.com/watch?v=J1OaBRTV2vs)**

## BetaBeetle (under active development)

The [beta version](https://github.com/coilhq/tigerbeetle/tree/beta) of **TigerBeetle is now under active development** and [our design document](./docs/DESIGN.md) details our design decisions regarding performance and safety, and where we want to go regarding accounting features.

## QuickStart

**Prerequisites:** The current beta version of TigerBeetle targets Linux and takes advantage of the latest asynchronous IO capabilities of the Linux kernel v5.6 and newer, via [io_uring](https://kernel.dk/io_uring.pdf). As such it can only be used on recent versions of Linux with an updated kernel.

Later portable versions of TigerBeetle may supplement `io_uring` with `kqueue` for macOS and FreeBSD support, or `IOCP` for Windows support.

```bash
git clone https://github.com/coilhq/tigerbeetle.git
cd tigerbeetle
scripts/install.sh
```

If you want to run Parallels on macOS on an M1 chip, we recommend the [server install image of Ubuntu 20.10 Groovy Gorilla for ARM64](https://releases.ubuntu.com/20.10/), which ships with Linux 5.8 and which will support installing Parallels Tools easily.

## Benchmark

With TigerBeetle installed, you are ready to benchmark!

```bash
scripts/benchmark.sh
```

*If you encounter any benchmark errors, please send us the resulting `benchmark.log`.*

## Launch a Local Cluster

Launch a TigerBeetle cluster on your local machine by running each of these commands in a new terminal tab:

```
./tigerbeetle --cluster=1 --addresses=3001,3002,3003 --replica=0
./tigerbeetle --cluster=1 --addresses=3001,3002,3003 --replica=1
./tigerbeetle --cluster=1 --addresses=3001,3002,3003 --replica=2
```

Run the TigerBeetle binary to see all command line arguments:

```bash
./tigerbeetle --help
```

## Clients

* [tigerbeetle-node](https://github.com/coilhq/tigerbeetle-node) is a TigerBeetle Node.js client written in TypeScript (and Zig with [Node's N-API](https://nodejs.org/api/n-api.html) for ABI stability).

* [client.zig](./src/client.zig) is a TigerBeetle Zig client.

* [demo.zig](./src/demo.zig) is a lightweight TigerBeetle client for demonstration purposes only, which we used to create [six demos you can work your way through and modify](./docs/DEEP_DIVE.md) to explore TigerBeetle's commands.

## Performance Demos

Along the way, we also put together a series of performance demos and sketches to get you comfortable building TigerBeetle, show how low-level code can sometimes be easier than high-level code, help you understand some of the key components within TigerBeetle, and enable back-of-the-envelope calculations to motivate design decisions.

You may be interested in:

* [demos/protobeetle](./demos/protobeetle), how batching changes everything.
* [demos/bitcast](./demos/bitcast), how Zig makes zero-overhead network deserialization easy, fast and safe.
* [demos/io_uring](./demos/io_uring), how ring buffers can eliminate kernel syscalls, reduce server hardware requirements by a factor of two, and change the way we think about event loops.
* [demos/hash_table](./demos/hash_table), how linear probing compares with cuckoo probing, and what we look for in a hash table that needs to scale to millions (and billions) of account transfers.

## License

Copyright 2020-2021 Coil Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
