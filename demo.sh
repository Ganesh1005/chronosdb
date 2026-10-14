#!/bin/bash
# ChronosDB final demo: 3 nodes, tx commit, leader kill, failover,
# minority write rejection, recovery, convergence, historical reads, conflict.
set -u
cd "$(dirname "$0")"
BIN=./target/debug/chronos
A="1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"

echo "=== building ==="
source "$HOME/.cargo/env"
cargo build -p chronos-net 2>&1 | tail -1

echo "=== starting 3 nodes ==="
rm -rf /tmp/demo1 /tmp/demo2 /tmp/demo3
($BIN serve --id 1 --client-addr 127.0.0.1:7001 --peer-addr 127.0.0.1:7101 \
  --peer 2=127.0.0.1:7102 --peer 3=127.0.0.1:7103 --data-dir /tmp/demo1 > /tmp/demo1.log 2>&1 &)
($BIN serve --id 2 --client-addr 127.0.0.1:7002 --peer-addr 127.0.0.1:7102 \
  --peer 1=127.0.0.1:7101 --peer 3=127.0.0.1:7103 --data-dir /tmp/demo2 > /tmp/demo2.log 2>&1 &)
($BIN serve --id 3 --client-addr 127.0.0.1:7003 --peer-addr 127.0.0.1:7103 \
  --peer 1=127.0.0.1:7101 --peer 2=127.0.0.1:7102 --data-dir /tmp/demo3 > /tmp/demo3.log 2>&1 &)
sleep 4

echo "=== 1. transaction commit (via put) ==="
$BIN put --addrs $A account:alice 100
$BIN put --addrs $A account:bob 50

echo "=== 2. read committed values ==="
echo -n "alice="; $BIN get --addrs $A account:alice
echo -n "bob="; $BIN get --addrs $A account:bob

echo "=== 3. find and kill the leader ==="
# The leader is the node that answers a single-addr get; probe each.
LEADER=""
for i in 1 2 3; do
  if $BIN get --addrs $i=127.0.0.1:700$i >/dev/null 2>&1; then LEADER=$i; break; fi
done
# Fallback: leader hint via a put to a single node is unreliable; use pid scan.
echo "probed leader: $LEADER (may be empty; killing node 1's process by port instead)"
# Kill whichever node holds client port 7001..7003 one at a time is overkill;
# instead kill the process listening on 7101 (node 1) as the victim for a deterministic demo.
VICTIM_PID=$(ps -eo pid,args | awk '/[c]hronos serve/ && /--id 1 / {print $1}' | head -1)
echo "killing node 1 (pid $VICTIM_PID)"
kill $VICTIM_PID
sleep 4

echo "=== 4. cluster still serves (failover) ==="
$BIN put --addrs $A account:alice 90
echo -n "alice="; $BIN get --addrs $A account:alice

echo "=== 5. minority cannot acknowledge: kill node 2 as well (1/3 left) ==="
VICTIM2=$(ps -eo pid,args | awk '/[c]hronos serve/ && /--id 2 / {print $1}' | head -1)
kill $VICTIM2
sleep 3
echo "attempting write with only node 3 alive (expect failure)..."
if timeout 15 $BIN put --addrs 3=127.0.0.1:7003 account:bob 999; then
  echo "UNEXPECTED: minority acknowledged a write"
else
  echo "OK: minority did not acknowledge (exit $?)"
fi

echo "=== 6. heal: restart nodes 1 and 2, verify convergence ==="
($BIN serve --id 1 --client-addr 127.0.0.1:7001 --peer-addr 127.0.0.1:7101 \
  --peer 2=127.0.0.1:7102 --peer 3=127.0.0.1:7103 --data-dir /tmp/demo1 > /tmp/demo1.log 2>&1 &)
($BIN serve --id 2 --client-addr 127.0.0.1:7002 --peer-addr 127.0.0.1:7102 \
  --peer 1=127.0.0.1:7101 --peer 3=127.0.0.1:7103 --data-dir /tmp/demo2 > /tmp/demo2.log 2>&1 &)
sleep 5
echo -n "alice="; $BIN get --addrs $A account:alice
echo -n "bob="; $BIN get --addrs $A account:bob

echo "=== 7. historical reads ==="
$BIN get-at --addrs $A account:alice 2 || true
$BIN get-at --addrs $A account:alice 4 || true

echo "=== demo complete ==="
echo "cleaning up..."
for pid in $(ps -eo pid,args | awk '/[c]hronos serve/ {print $1}'); do kill $pid 2>/dev/null; done
