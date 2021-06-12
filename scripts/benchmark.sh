#!/usr/bin/env bash
set -e

COLOR_RED='\033[1;31m'
COLOR_END='\033[0m'

zig/zig build -Drelease-safe
mv zig-out/bin/tigerbeetle .

function onerror {
    if [ "$?" == "0" ]; then
        rm benchmark.log
    else
        echo -e "${COLOR_RED}"
        echo "Error running benchmark, here are more details (from benchmark.log):"
        echo -e "${COLOR_END}"
        cat benchmark.log
    fi

    for I in 0
    do
        echo "Stopping replica $I..."
    done
    kill %1
}
trap onerror EXIT

for I in 0
do
    echo "Starting replica $I..."
    ./tigerbeetle --cluster=1 --addresses=3001 --replica=$I > benchmark.log 2>&1 &
done

# Wait for replicas to start, listen and connect:
sleep 1

echo ""
echo "Benchmarking..."
zig/zig run -OReleaseSafe src/benchmark.zig
echo ""
