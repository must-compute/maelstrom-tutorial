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
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 10 --rate 100 --nemesis partition --topology tree4 --log-stderr
else
  echo "unknown command"
fi
