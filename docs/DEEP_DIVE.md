# Install TigerBeetle, benchmark! and work your way through six demos...

- [Clone TigerBeetle](#clone-tigerbeetle)
- [Upgrade Ubuntu to the 5.7.15 kernel](#upgrade-ubuntu-to-the-5715-kernel)
- [Install Zig](#install-zig)
- [Simulate non-pristine lab conditions](#simulate-non-pristine-lab-conditions)
- [Benchmark!](#benchmark)
- [Explore the API through six demos](#explore-the-api-through-six-demos)

## Video walkthrough

[![Video walkthrough on Youtube](https://img.youtube.com/vi/lQSIVgvea48/0.jpg)](https://www.youtube.com/watch?v=lQSIVgvea48)

## Clone TigerBeetle

```bash
git clone https://github.com/coilhq/tigerbeetle.git
cd tigerbeetle
```

## Upgrade Ubuntu to the 5.7.15 or 5.8 kernel

Get Ubuntu 20.04 to a recent kernel with io_uring. This should take less than a minute:

```
scripts/upgrade_ubuntu_kernel.sh
```

Alternatively, if you want to run Parallels on macOS on an M1 chip, we recommend the [server install image of Ubuntu 20.10 Groovy Gorilla for ARM64](https://releases.ubuntu.com/20.10/), which ships with Linux 5.8 and which will support installing Parallels Tools easily.

For newer than 5.7.15, you can also find the full list of [mainline releases here](https://kernel.ubuntu.com/~kernel-ppa/mainline/?C=N;O=D). Note that kernel 5.7.16 and up introduced a [network performance regression](https://github.com/axboe/liburing/issues/215) that was recently patched.

## Install Zig and build TigerBeetle

```bash
scripts/install.sh
```

You can also re-run `scripts/install_zig.sh latest` if you want to update your Zig to the latest build. Zig has a rapid release cadence at present and we are tracking the latest build to keep pace.

## Simulate non-pristine lab conditions

You are ready to rock! To simulate non-pristine lab conditions (tiger beetles thrive in harsh environments with noisy neighbors) turn up your favorite album, or if you're looking for something new we suggest Noel Gallagher's High Flying Birds.

Be aware that super non-pristine lab conditions such as an active video call might cause cache pollution and drop throughput. But rock 'n roll is generally fine.

## Benchmark!

With TigerBeetle installed, you are ready to benchmark!

```bash
scripts/benchmark.sh
```

## Explore the API through six demos

Take a look at the source code of these demos before you run them. Check out our screencast of these demos for much more detail (note that there have been a few changes to our types since the screencast).

Let's turn up the log level some more (and your favorite album) so you can see everything the server does as you run these demos:

* Open `src/config.zig` in your editor and change `log_level` to `7` (debug).

* Start a single replica cluster: `./tigerbeetle --cluster=1 --addresses=3001 --replica=0`

### Demo 1, 2: Create and lookup accounts

Let's create some accounts and check their balances and limits:

```bash
zig run src/demo_01_create_accounts.zig
zig run src/demo_02_lookup_accounts.zig
```

What happens if we create those accounts again?

```bash
zig run src/demo_01_create_accounts.zig
```

### Demo 3: Simple journal entries

Let's create some simple double-entry accounting journal entries:

```
zig run src/demo_03_create_transfers.zig
zig run src/demo_02_lookup_accounts.zig
```

### Demo 4, 5, 6: Two-phase commit journal entries

Let's try full two-phase commit transfers (create, and then commit):

*These two-phase commit transfers are designed for two-phase commit systems such as Interledger or Mojaloop, where the fulfil packet only includes the transfer id and you want to avoid a lookup query roundtrip to the database before writing the compensating journal entry.*

You will see the second transfer is rejected with an error for tripping the debit reserved limit.

```
zig run src/demo_04_create_transfers_two_phase_commit.zig
zig run src/demo_02_lookup_accounts.zig
```

You will see these two-phase transfers only update the inflight reserved limits.

Let's commit (and accept):

Again, the second transfer is rejected because it was never created.

```
zig run src/demo_05_accept_transfers.zig
zig run src/demo_02_lookup_accounts.zig
```

Let's also pretend someone else tried to commit (but reject) concurrently:

```
zig run src/demo_06_reject_transfers.zig
```

**From here, feel free to tweak these demos and see what happens. You can explore all our accounting invariants (and the DSL we created for these) in `src/state_machine.zig` by grepping the source for `fn create_account`, `fn create_transfer`, and `fn commit_transfer`.**

**Enjoy...**
