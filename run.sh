#!/bin/bash

MAELSTROM="./maelstrom/maelstrom"
BINARY="./target/debug/maelstrom-tutorial"

if [ "$1" == "broadcast-2s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 2 --rate 10 --log-stderr
elif [ "$1" = "broadcast-10s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 10 --log-stderr
elif [ "$1" = "broadcast-15s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 15 --log-stderr
elif [ "$1" = "broadcast-20s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --log-stderr
elif [ "$1" = "broadcast-20s-partition" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --nemesis partition --topology tree4 --log-stderr
elif [ "$1" = "broadcast-20s-partition-loop" ]; then
  for i in {1..100}; do
      echo "Attempt $i"
      output=$(cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --nemesis partition --topology tree4 --log-stderr )
      last_line=$(echo "$output" | tail -n 1)

      echo "$last_line"

      if [ $? -ne 0 ] || echo "$last_line" | grep -q "invalid"; then
          echo "Command failed on attempt $i, aborting..."
          echo "Command output was:\n$output"
          exit 1
      fi
  done
  echo "All attempts completed successfully"
elif [ "$1" = "g-set" ]; then
  cargo build && $MAELSTROM test -w g-set --bin $BINARY --log-stderr
elif [ "$1" = "g-set-30s-partition" ]; then
  cargo build && $MAELSTROM test -w g-set --bin $BINARY --time-limit 30 --rate 10 --nemesis partition --log-stderr
elif [ "$1" = "g-counter" ]; then
  cargo build && $MAELSTROM test -w g-counter --bin $BINARY --time-limit 20 --rate 10 --log-stderr
elif [ "$1" = "datomic" ]; then
  cargo build && $MAELSTROM test -w txn-list-append --bin $BINARY --time-limit 10 --log-stderr --node-count 2 --rate 100
elif [ "$1" = "raft" ]; then
  cargo build && $MAELSTROM test -w lin-kv --bin $BINARY --time-limit 30 --log-stderr --node-count 3 --concurrency 2n --rate 100
elif [ "$1" = "raft-nemesis" ]; then
  cargo build && $MAELSTROM test -w lin-kv --bin $BINARY --time-limit 60 --log-stderr --node-count 3 --concurrency 4n --rate 30 --nemesis partition --nemesis-interval 10 --test-count 10
else
  echo "unknown command"
fi
