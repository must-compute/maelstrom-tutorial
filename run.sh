#!/bin/bash

MAELSTROM="./maelstrom/maelstrom"
BINARY="./target/debug/maelstrom-tutorial"

if [ "$1" == "broadcast-2s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 2 --rate 10
elif [ "$1" = "broadcast-20s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --rate 100
elif [ "$1" = "broadcast-20s-line" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --rate 100 --node-count 25 --topology line
elif [ "$1" = "broadcast-20s-nemesis" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --topology tree4 --nemesis partition
else
  echo "unknown command"
fi
