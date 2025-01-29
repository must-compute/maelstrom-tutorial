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
else
  echo "unknown command"
fi
